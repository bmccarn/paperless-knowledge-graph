"""Remote owner acknowledgment cannot outrun resource teardown."""
import asyncio
from contextlib import asynccontextmanager
import json
import unittest

from scripts.live_query_control import own_runtime


class RemoteControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.identity = {'manifest_sha256': 'a' * 64, 'pod_uid': 'isolated-pod', 'nonce': 'b' * 32}
        self.events = []
        self.reader = asyncio.StreamReader()

    @asynccontextmanager
    async def runtime(self):
        self.events.append('readers-open')
        task = asyncio.create_task(asyncio.Event().wait())
        try:
            yield {'port': 12345}
        finally:
            self.events.append('server-stop')
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.events.append('workers-joined')
            self.events.append('readers-close')

    async def emit(self, message):
        self.events.append(message['event'])

    async def run_owner(self, **options):
        return await own_runtime(self.runtime, self.reader, self.emit,
                                 identity=self.identity, seconds=options.get('seconds', 1))

    async def test_stop_acknowledges_only_after_join_and_reader_close(self):
        self.reader.feed_data(json.dumps({'command': 'stop', **self.identity}).encode() + b'\n')
        result = await self.run_owner()
        self.assertEqual(result['reason'], 'stop')
        self.assertEqual(self.events, ['readers-open', 'ready', 'server-stop',
                                      'workers-joined', 'readers-close', 'closed'])

    async def test_eof_cleans_up_without_claiming_successful_stop(self):
        self.reader.feed_eof()
        result = await self.run_owner()
        self.assertEqual(result['reason'], 'control_eof')
        self.assertEqual(self.events[-3:], ['workers-joined', 'readers-close', 'closed'])

    async def test_mismatched_control_and_timeout_fail_after_teardown(self):
        self.reader.feed_data(json.dumps({'command': 'stop', **self.identity, 'nonce': 'wrong'}).encode() + b'\n')
        with self.assertRaisesRegex(ValueError, 'Unscheduled'):
            await self.run_owner()
        self.assertEqual(self.events[-3:], ['workers-joined', 'readers-close', 'failed'])
        self.events.clear(); self.reader = asyncio.StreamReader()
        with self.assertRaises(TimeoutError):
            await self.run_owner(seconds=.01)
        self.assertEqual(self.events[-3:], ['workers-joined', 'readers-close', 'failed'])

    async def test_cancellation_and_broken_ready_channel_still_drain_workers(self):
        task = asyncio.create_task(self.run_owner())
        while 'ready' not in self.events:
            await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.events[-3:], ['workers-joined', 'readers-close', 'failed'])
        self.events.clear()
        async def broken(message):
            raise BrokenPipeError()
        with self.assertRaises(BrokenPipeError):
            await own_runtime(self.runtime, self.reader, broken, identity=self.identity, seconds=1)
        self.assertEqual(self.events[-3:], ['server-stop', 'workers-joined', 'readers-close'])

    async def test_startup_failure_cannot_publish_readiness(self):
        @asynccontextmanager
        async def unavailable():
            raise OSError('synthetic listener failure')
            yield
        with self.assertRaises(OSError):
            await own_runtime(unavailable, self.reader, self.emit, identity=self.identity, seconds=1)
        self.assertEqual(self.events, ['failed'])

    async def test_stop_near_deadline_and_owner_recancellation_do_not_interrupt_worker_cleanup(self):
        cleanup_started = asyncio.Event()
        @asynccontextmanager
        async def draining():
            async def worker():
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup_started.set()
                    self.events.append('cleanup-start')
                    await asyncio.sleep(.03)
                    self.events.append('cleanup-finished')
            worker_task = asyncio.create_task(worker())
            await asyncio.sleep(0)
            try:
                yield {'port': 12345}
            finally:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
                self.events.append('dependencies-restored')
        for recancel in (False, True):
            with self.subTest(recancel=recancel):
                self.events.clear(); cleanup_started.clear()
                self.reader = asyncio.StreamReader()
                self.reader.feed_data(json.dumps({'command': 'stop', **self.identity}).encode() + b'\n')
                owner = asyncio.create_task(own_runtime(draining, self.reader, self.emit,
                                                       identity=self.identity, seconds=.01))
                await cleanup_started.wait()
                if recancel:
                    owner.cancel()
                    await asyncio.sleep(.005)
                    owner.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await owner
                else:
                    await owner
                self.assertEqual(self.events[-3:], ['cleanup-finished', 'dependencies-restored',
                                                   'failed' if recancel else 'closed'])

    async def test_partial_startup_timeout_and_recancellation_finish_worker_cleanup(self):
        cleanup_started = asyncio.Event()
        @asynccontextmanager
        async def starting():
            async def worker():
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup_started.set()
                    await asyncio.sleep(.03)
                    self.events.append('worker-cleaned')
            worker_task = asyncio.create_task(worker())
            await asyncio.sleep(0)
            try:
                await asyncio.Event().wait()
                yield {'port': 12345}
            finally:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
                self.events.append('dependencies-restored')
        owner = asyncio.create_task(own_runtime(starting, self.reader, self.emit,
                                               identity=self.identity, seconds=.01))
        await cleanup_started.wait()
        owner.cancel()
        await asyncio.sleep(.005)
        owner.cancel()
        with self.assertRaises(TimeoutError):
            await owner
        self.assertEqual(self.events, ['worker-cleaned', 'dependencies-restored', 'failed'])

    async def test_entry_completion_cancellation_cannot_skip_context_exit(self):
        owner = None
        @asynccontextmanager
        async def racing():
            self.events.append('resource-open')
            asyncio.get_running_loop().call_soon(owner.cancel)
            try:
                yield {'port': 12345}
            finally:
                self.events.append('resource-closed')
        owner = asyncio.create_task(own_runtime(racing, self.reader, self.emit,
                                               identity=self.identity, seconds=1))
        with self.assertRaises(asyncio.CancelledError):
            await owner
        self.assertEqual(self.events, ['resource-open', 'resource-closed', 'failed'])

    async def test_failed_cleanup_never_emits_closed(self):
        @asynccontextmanager
        async def broken():
            try:
                yield {'port': 12345}
            finally:
                raise RuntimeError('synthetic cleanup failure')
        self.reader.feed_eof()
        with self.assertRaises(RuntimeError):
            await own_runtime(broken, self.reader, self.emit, identity=self.identity, seconds=1)
        self.assertEqual(self.events, ['ready', 'failed'])

    async def test_invalid_admission_identity_cannot_open_runtime(self):
        for seconds in (True, float('nan'), 0, 3601):
            with self.assertRaises(ValueError):
                await self.run_owner(seconds=seconds)
        with self.assertRaises(ValueError):
            await own_runtime(self.runtime, self.reader, self.emit, identity={}, seconds=1)
        self.assertEqual(self.events, [])
