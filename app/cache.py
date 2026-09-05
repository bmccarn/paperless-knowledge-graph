"""Bounded local caches and Redis namespaces with async offloading adapters."""

import asyncio
from collections import OrderedDict
from copy import deepcopy
import hashlib
import json
import logging
import threading
import time
from typing import Any, Optional
import uuid

logger = logging.getLogger(__name__)


class TTLCache:
    """Thread-safe bounded LRU storage with monotonic expiry and value isolation."""

    def __init__(self, default_ttl: int = 3600, max_entries: int = 1024, clock=time.monotonic):
        if max_entries < 1:
            raise ValueError("Cache capacity must be positive")
        self._store = OrderedDict()
        self._default_ttl = default_ttl
        self._max_entries = max_entries
        self._clock = clock
        self._lock = threading.RLock()
        self._hits = self._misses = self._evictions = 0

    def _evict_expired(self):
        now = self._clock()
        for key in [key for key, (_, expiry) in self._store.items() if now >= expiry]:
            del self._store[key]

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None or self._clock() >= entry[1]:
                self._store.pop(key, None)
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return deepcopy(entry[0])

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        with self._lock:
            self._evict_expired()
            lifetime = self._default_ttl if ttl is None else ttl
            if lifetime <= 0:
                self._store.pop(key, None)
                return
            self._store[key] = (deepcopy(value), self._clock() + lifetime)
            self._store.move_to_end(key)
            while len(self._store) > self._max_entries:
                self._store.popitem(last=False)
                self._evictions += 1

    def clear(self):
        with self._lock:
            self._store.clear()

    def invalidate_prefix(self, prefix: str):
        with self._lock:
            for key in [key for key in self._store if key.startswith(prefix)]:
                del self._store[key]

    def evict_expired(self):
        with self._lock:
            self._evict_expired()

    @property
    def size(self) -> int:
        with self._lock:
            self._evict_expired()
            return len(self._store)

    @property
    def stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": self.size, "max_entries": self._max_entries,
                "hits": self._hits, "misses": self._misses,
                "evictions": self._evictions,
                "hit_rate": round(self._hits / total, 3) if total else 0.0,
                "backend": "in-memory",
            }


class RedisCache:
    """Redis TTL entries. Namespace switching makes clear constant-work."""

    def __init__(self, redis_client, key_prefix: str, default_ttl: int = 3600):
        self._redis = redis_client
        self._prefix = key_prefix
        self._default_ttl = default_ttl
        self._hits = self._misses = 0
        self._lock = threading.RLock()
        self._pending_clear = False

    @property
    def _generation_key(self):
        return f"{self._prefix}:namespace"

    def _namespace(self):
        # If clear failed, publish it before exposing any old namespace again.
        if self._pending_clear:
            self._redis.incr(self._generation_key)
            self._pending_clear = False
        return int(self._redis.get(self._generation_key) or 0)

    def _make_key(self, key: str) -> str:
        return f"{self._prefix}:v{self._namespace()}:{key}"

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            try:
                raw = self._redis.get(self._make_key(key))
                if raw is None:
                    self._misses += 1
                    return None
                value = json.loads(raw)
                self._hits += 1
                return value
            except Exception as exc:
                logger.warning("Redis get error (%s): %s", self._prefix, exc)
                self._misses += 1
                return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        with self._lock:
            try:
                lifetime = self._default_ttl if ttl is None else ttl
                if lifetime <= 0:
                    self._redis.delete(self._make_key(key))
                    return
                self._redis.setex(self._make_key(key), lifetime, json.dumps(value, default=str))
            except Exception as exc:
                logger.warning("Redis set error (%s): %s", self._prefix, exc)

    def clear(self):
        with self._lock:
            self._pending_clear = True
            try:
                self._redis.incr(self._generation_key)
                self._pending_clear = False
            except Exception as exc:
                logger.warning("Redis clear error (%s): %s", self._prefix, exc)

    def invalidate_prefix(self, prefix: str):
        with self._lock:
            try:
                # Redis glob syntax must not change the requested literal prefix.
                escaped = ''.join('\\' + char if char in '*?[]\\' else char for char in prefix)
                pattern = self._make_key(escaped + '*')
                batch = []
                for key in self._redis.scan_iter(match=pattern, count=200):
                    batch.append(key)
                    if len(batch) == 200:
                        self._redis.delete(*batch)
                        batch = []
                if batch:
                    self._redis.delete(*batch)
            except Exception as exc:
                logger.warning("Redis prefix invalidation error (%s): %s", self._prefix, exc)
                self._pending_clear = True

    def evict_expired(self):
        pass

    @property
    def size(self):
        # Health requests must not enumerate the keyspace to obtain a count.
        return None

    @property
    def stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": None, "size_status": "not_scanned",
                "hits": self._hits, "misses": self._misses,
                "hit_rate": round(self._hits / total, 3) if total else 0.0,
                "backend": "redis",
            }


class CorpusGeneration:
    """Shared invalidation generation with a conservative local outage token."""

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._local_generation = 0
        self._local_id = uuid.uuid4().hex
        self._pending = False
        self._lock = threading.RLock()
        self._key = 'kg:corpus:generation'

    def get(self):
        with self._lock:
            if self._redis is not None:
                try:
                    if self._pending:
                        self._redis.incr(self._key)
                        self._pending = False
                    return f"redis:{int(self._redis.get(self._key) or 0)}"
                except Exception:
                    pass
            return f"local:{self._local_id}:{self._local_generation}"

    def advance(self):
        with self._lock:
            self._local_generation += 1
            if self._redis is not None:
                self._pending = True
                try:
                    self._redis.incr(self._key)
                    self._pending = False
                except Exception:
                    logger.warning("Corpus generation pending Redis recovery")
            return self.get()


def normalize_query_key(question: str) -> str:
    return hashlib.md5(question.strip().lower().encode('utf-8')).hexdigest()


def _init_caches():
    from app.config import settings
    if settings.redis_url.startswith('memory:'):
        return tuple(TTLCache(default_ttl=ttl) for ttl in (3600, 1800, 1800, 7200)), CorpusGeneration()
    try:
        import redis
        # Construction performs no I/O. Async callers offload the first socket
        # operation too; URL authentication, database selection and TLS survive.
        client = redis.Redis.from_url(settings.redis_url, decode_responses=False,
                                      socket_connect_timeout=2, socket_timeout=2)
        caches = tuple(RedisCache(client, prefix, ttl) for prefix, ttl in (
            ('kg:query', 86400), ('kg:vector', 7200), ('kg:graph', 7200), ('kg:entity', 14400)))
        return caches, CorpusGeneration(client)
    except Exception as exc:
        logger.warning('Redis configuration unavailable (%s); using bounded local caches', exc)
        return tuple(TTLCache(default_ttl=ttl) for ttl in (3600, 1800, 1800, 7200)), CorpusGeneration()


(query_cache, vector_cache, graph_cache, entity_cache), _corpus_generation = _init_caches()


def get_corpus_generation() -> str:
    return _corpus_generation.get()


def get_all_cache_stats() -> dict:
    return {name: cache.stats for name, cache in (
        ('query', query_cache), ('vector', vector_cache), ('graph', graph_cache), ('entity', entity_cache))}


def invalidate_on_sync():
    """Advance corpus identity and invalidate all derived cache namespaces."""
    generation = _corpus_generation.advance()
    for cache in (query_cache, vector_cache, graph_cache, entity_cache):
        cache.clear()
    return generation


async def cache_get(cache, key):
    return await asyncio.to_thread(cache.get, key)


async def cache_set(cache, key, value, ttl=None):
    return await asyncio.to_thread(cache.set, key, value, ttl)


async def get_corpus_generation_async():
    return await asyncio.to_thread(get_corpus_generation)


async def invalidate_on_sync_async():
    return await asyncio.to_thread(invalidate_on_sync)


async def get_all_cache_stats_async():
    return await asyncio.to_thread(get_all_cache_stats)
