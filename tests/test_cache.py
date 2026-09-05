"""Cache contracts with real adapters and deterministic storage/clock fixtures."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import threading
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()
from app.cache import TTLCache, RedisCache, CorpusGeneration, cache_get, cache_set


class RedisFixture:
    def __init__(self):
        self.data = {}
        self.fail = False
        self.calls = []
        self.entered = self.release = None

    def _call(self, name):
        self.calls.append(name)
        if self.fail:
            raise ConnectionError('synthetic Redis outage')

    def get(self, key):
        self._call('get')
        if self.entered:
            self.entered.set()
            self.release.wait(2)
        return self.data.get(key)

    def incr(self, key):
        self._call('incr')
        self.data[key] = int(self.data.get(key, 0)) + 1
        return self.data[key]

    def setex(self, key, ttl, value):
        self._call('setex')
        self.data[key] = value

    def delete(self, *keys):
        self._call('delete')
        for key in keys:
            self.data.pop(key, None)

    def scan_iter(self, match, count):
        self._call('scan_iter')
        if count > 200:
            raise AssertionError('unbounded scan batch')
        yield from [key for key in self.data if fnmatch.fnmatch(key, match)]

    def keys(self, *args):
        raise AssertionError('KEYS is forbidden')


class CacheTests(unittest.TestCase):
    def test_expiry_at_deadline_and_zero_ttl(self):
        now = [100.0]
        cache = TTLCache(default_ttl=5, clock=lambda: now[0])
        cache.set('a', 1)
        now[0] = 105
        self.assertIsNone(cache.get('a'))
        cache.set('b', 2, ttl=0)
        self.assertEqual(cache.size, 0)

    def test_capacity_uses_recent_access_and_expired_entries_are_not_counted(self):
        now = [0]
        cache = TTLCache(default_ttl=20, max_entries=2, clock=lambda: now[0])
        cache.set('a', 1)
        cache.set('b', 2)
        cache.get('a')
        cache.set('c', 3)
        self.assertIsNone(cache.get('b'))
        self.assertEqual(cache.stats['size'], 2)
        self.assertEqual(cache.stats['evictions'], 1)
        now[0] = 20
        self.assertEqual(cache.stats['size'], 0)

    def test_mutating_input_or_output_cannot_change_cached_value(self):
        cache = TTLCache()
        value = {'nested': ['original']}
        cache.set('a', value)
        value['nested'].append('mutated input')
        returned = cache.get('a')
        returned['nested'].append('mutated output')
        self.assertEqual(cache.get('a'), {'nested': ['original']})

    def test_concurrent_set_get_clear_remains_bounded(self):
        cache = TTLCache(max_entries=20)
        def use_cache(worker):
            for index in range(100):
                cache.set(f'{worker}:{index}', {'items': [index]})
                cache.get(f'{worker}:{index}')
                if index % 17 == 0:
                    cache.invalidate_prefix(f'{worker}:')
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(use_cache, range(8)))
        self.assertLessEqual(cache.stats['size'], 20)

    def test_redis_namespace_clear_prevents_old_hits_without_enumeration(self):
        redis = RedisFixture()
        left, right = RedisCache(redis, 'test'), RedisCache(redis, 'test')
        left.set('answer', {'amount': 5})
        self.assertEqual(right.get('answer'), {'amount': 5})
        right.clear()
        self.assertIsNone(left.get('answer'))
        self.assertNotIn('scan_iter', redis.calls)
        self.assertTrue(any(':v0:' in key for key in redis.data))  # old data expires naturally

    def test_redis_prefix_scan_does_not_clear_other_entries(self):
        redis = RedisFixture()
        cache = RedisCache(redis, 'test')
        cache.set('document:1', 1)
        cache.set('other:1', 2)
        cache.invalidate_prefix('document:')
        self.assertIsNone(cache.get('document:1'))
        self.assertEqual(cache.get('other:1'), 2)
        self.assertIn('scan_iter', redis.calls)

    def test_stats_do_not_contact_or_scan_redis(self):
        redis = RedisFixture()
        stats = RedisCache(redis, 'test').stats
        self.assertIsNone(stats['size'])
        self.assertEqual(stats['size_status'], 'not_scanned')
        self.assertEqual(redis.calls, [])

    def test_failed_clear_cannot_restore_old_entries_after_recovery(self):
        redis = RedisFixture()
        cache = RedisCache(redis, 'test')
        cache.set('answer', 'stale')
        redis.fail = True
        cache.clear()
        self.assertIsNone(cache.get('answer'))
        redis.fail = False
        self.assertIsNone(cache.get('answer'))
        cache.set('answer', 'fresh')
        self.assertEqual(cache.get('answer'), 'fresh')

    def test_generation_shared_and_pending_invalidation_published_after_outage(self):
        redis = RedisFixture()
        first, second = CorpusGeneration(redis), CorpusGeneration(redis)
        before = first.get()
        first.advance()
        self.assertNotEqual(second.get(), before)
        before_outage = second.get()
        redis.fail = True
        outage = first.advance()
        self.assertTrue(outage.startswith('local:'))
        redis.fail = False
        recovered = first.get()
        self.assertNotEqual(recovered, before_outage)
        self.assertEqual(recovered, second.get())


class AsyncCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_socket_does_not_block_event_loop(self):
        redis = RedisFixture()
        cache = RedisCache(redis, 'test')
        redis.entered, redis.release = threading.Event(), threading.Event()
        pending = asyncio.create_task(cache_get(cache, 'missing'))
        entered = await asyncio.to_thread(redis.entered.wait, 1)
        self.assertTrue(entered)
        # The cache worker is still blocked while this independent async task runs.
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=0.2)
        self.assertFalse(pending.done())
        redis.release.set()
        self.assertIsNone(await pending)

    async def test_async_set_get_matches_sync_value_isolation(self):
        cache = TTLCache()
        await cache_set(cache, 'a', {'items': [1]})
        value = await cache_get(cache, 'a')
        value['items'].append(2)
        self.assertEqual(await cache_get(cache, 'a'), {'items': [1]})


if __name__ == '__main__':
    unittest.main()
