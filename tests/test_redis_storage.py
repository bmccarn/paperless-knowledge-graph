import asyncio
import os
import unittest
from urllib.parse import urlparse

import redis
from tests.runtime import configure_test_environment
configure_test_environment()
from app.cache import RedisCache, CorpusGeneration, cache_get, cache_set


@unittest.skipUnless(os.environ.get("REDIS_TEST_URL"), "Set REDIS_TEST_URL for disposable Redis")
class RedisStorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        url = os.environ["REDIS_TEST_URL"]
        if urlparse(url).hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("Redis storage tests require localhost")
        self.client = redis.Redis.from_url(url)
        self.cache = RedisCache(self.client, "accuracy-test", default_ttl=60)
        self.peer = RedisCache(self.client, "accuracy-test", default_ttl=60)
        await asyncio.to_thread(self.cache.clear)

    async def asyncTearDown(self):
        keys = await asyncio.to_thread(lambda: list(self.client.scan_iter(match="accuracy-test:*")))
        if keys:
            await asyncio.to_thread(self.client.delete, *keys)
        await asyncio.to_thread(self.client.close)

    async def test_namespace_clear_propagates_to_other_clients_and_values_are_copies(self):
        await cache_set(self.cache, "answer", {"items": [1]})
        value = await cache_get(self.peer, "answer")
        value["items"].append(2)
        self.assertEqual(await cache_get(self.cache, "answer"), {"items": [1]})
        await asyncio.to_thread(self.peer.clear)
        self.assertIsNone(await cache_get(self.cache, "answer"))

    async def test_literal_prefix_and_shared_corpus_generation(self):
        await cache_set(self.cache, "doc[1]:a", "target")
        await cache_set(self.cache, "doc1:a", "retain")
        await asyncio.to_thread(self.cache.invalidate_prefix, "doc[1]:")
        self.assertIsNone(await cache_get(self.cache, "doc[1]:a"))
        self.assertEqual(await cache_get(self.cache, "doc1:a"), "retain")
        first, second = CorpusGeneration(self.client), CorpusGeneration(self.client)
        before = await asyncio.to_thread(second.get)
        await asyncio.to_thread(first.advance)
        self.assertNotEqual(await asyncio.to_thread(second.get), before)
        self.assertEqual(await asyncio.to_thread(second.get), await asyncio.to_thread(first.get))


if __name__ == "__main__":
    unittest.main()
