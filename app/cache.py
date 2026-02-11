"""In-memory TTL cache for query results, embeddings, and graph traversals."""

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TTLCache:
    """Simple dict-based cache with per-entry TTL."""

    def __init__(self, default_ttl: int = 3600):
        self._store: dict[str, tuple[Any, float]] = {}  # key -> (value, expires_at)
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
        }


# Global cache instances
query_cache = TTLCache(default_ttl=3600)       # 1 hour
vector_cache = TTLCache(default_ttl=1800)       # 30 min
graph_cache = TTLCache(default_ttl=1800)        # 30 min
entity_cache = TTLCache(default_ttl=7200)       # 2 hours, cleared on sync


def get_all_cache_stats() -> dict:
    return {
        "query": query_cache.stats,
        "vector": vector_cache.stats,
        "graph": graph_cache.stats,
        "entity": entity_cache.stats,
    }


def invalidate_on_sync():
    """Clear caches that should be invalidated when sync/reindex runs."""
    query_cache.clear()
    vector_cache.clear()
    graph_cache.clear()
    entity_cache.clear()
    logger.info("All caches invalidated for sync/reindex")
