"""Cache layer: Redis-backed with in-memory TTLCache fallback."""

import json
import logging
import time
import hashlib
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── In-memory fallback ──────────────────────────────────────────────

class TTLCache:
    """Simple dict-based cache with per-entry TTL."""

    def __init__(self, default_ttl: int = 3600):
        self._store: dict[str, tuple[Any, float]] = {}
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return None
        value, expires_at = entry
        if time.time() > expires_at:
            del self._store[key]
            self._misses += 1
            return None
        self._hits += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        t = ttl if ttl is not None else self._default_ttl
        self._store[key] = (value, time.time() + t)

    def clear(self):
        self._store.clear()

    def invalidate_prefix(self, prefix: str):
        keys_to_remove = [k for k in self._store if k.startswith(prefix)]
        for k in keys_to_remove:
            del self._store[k]

    def evict_expired(self):
        now = time.time()
        expired = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in expired:
            del self._store[k]

    @property
    def size(self) -> int:
        return len(self._store)

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": self.size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 3) if total > 0 else 0.0,
            "backend": "in-memory",
        }


# ── Redis-backed cache ──────────────────────────────────────────────

class RedisCache:
    """Redis-backed cache with TTL support."""

    def __init__(self, redis_client, key_prefix: str, default_ttl: int = 3600):
        self._redis = redis_client
        self._prefix = key_prefix
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def _make_key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def get(self, key: str) -> Optional[Any]:
        try:
            raw = self._redis.get(self._make_key(key))
            if raw is None:
                self._misses += 1
                return None
            self._hits += 1
            return json.loads(raw.decode("utf-8"))
        except Exception as e:
            logger.warning(f"Redis get error ({self._prefix}): {e}")
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        try:
            self._redis.setex(
                self._make_key(key),
                ttl if ttl is not None else self._default_ttl,
                json.dumps(value, default=str),
            )
        except Exception as e:
            logger.warning(f"Redis set error ({self._prefix}): {e}")

    def clear(self):
        try:
            keys = self._redis.keys(self._make_key("*"))
            if keys:
                self._redis.delete(*keys)
            logger.info(f"Cleared {len(keys)} keys from {self._prefix}")
        except Exception as e:
            logger.warning(f"Redis clear error: {e}")

    def invalidate_prefix(self, prefix: str):
        try:
            keys = self._redis.keys(self._make_key(f"{prefix}*"))
            if keys:
                self._redis.delete(*keys)
        except Exception as e:
            logger.warning(f"Redis invalidate error: {e}")

    def evict_expired(self):
        pass  # Redis handles TTL natively

    @property
    def size(self) -> int:
        try:
            return len(self._redis.keys(self._make_key("*")))
        except Exception:
            return 0

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": self.size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 3) if total > 0 else 0.0,
            "backend": "redis",
        }


# ── Initialization ──────────────────────────────────────────────────

def normalize_query_key(question: str) -> str:
    """Normalized cache key from question text."""
    return hashlib.md5(question.strip().lower().encode("utf-8")).hexdigest()


def _init_caches():
    """Try Redis, fall back to in-memory."""
    try:
        import redis
        client = redis.Redis(
            host="your-server-host",
            port=6379,
            db=0,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        client.ping()
        logger.info("Redis cache connected (your-server-host:6379)")
        return (
            RedisCache(client, "kg:query", default_ttl=86400),   # 24h
            RedisCache(client, "kg:vector", default_ttl=7200),   # 2h
            RedisCache(client, "kg:graph", default_ttl=7200),    # 2h
            RedisCache(client, "kg:entity", default_ttl=14400),  # 4h
        )
    except Exception as e:
        logger.warning(f"Redis unavailable ({e}), using in-memory cache")
        return (
            TTLCache(default_ttl=3600),
            TTLCache(default_ttl=1800),
            TTLCache(default_ttl=1800),
            TTLCache(default_ttl=7200),
        )


query_cache, vector_cache, graph_cache, entity_cache = _init_caches()


def get_all_cache_stats() -> dict:
    return {
        "query": query_cache.stats,
        "vector": vector_cache.stats,
        "graph": graph_cache.stats,
        "entity": entity_cache.stats,
    }


def invalidate_on_sync():
    """Clear all caches on sync/reindex."""
    query_cache.clear()
    vector_cache.clear()
    graph_cache.clear()
    entity_cache.clear()
    logger.info("All caches invalidated for sync/reindex")
