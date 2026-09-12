"""Server task/listener lifetime without opening a network connection."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.live_query_server import loopback_server


class LiveServerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.socket = MagicMock(); self.socket.getsockname.return_value = ('127.0.0.1', 1234)
        self.server = None

    def factory(self, config):
        self.assertEqual(config.lifespan, 'off')
        state = SimpleNamespace(tasks=set(), connections=set())
        server = SimpleNamespace(started=False, should_exit=False, force_exit=False, server_state=state)
        async def serve(*, sockets):
            server.started = True
            while not server.should_exit:
                await asyncio.sleep(0.001)
        server.serve = serve
        self.server = server
        return server

    async def test_loopback_lifespan_off_and_listener_closed_on_normal_exit(self):
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', self.factory):
            async with loopback_server(object()) as ready:
                self.assertEqual(ready['url'], 'http://127.0.0.1:1234')
                self.assertIsNotNone(self.server.capture_signals())
        self.socket.bind.assert_called_once_with(('127.0.0.1', 0))
        self.socket.close.assert_called_once()
        self.assertTrue(self.server.should_exit)

    async def test_request_tasks_finish_cleanup_before_listener_scope_returns(self):
        cleaned = asyncio.Event()
        async def active_request():
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                cleaned.set()
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', self.factory):
            with self.assertRaisesRegex(RuntimeError, 'Owner failed'):
                async with loopback_server(object()):
                    task = asyncio.create_task(active_request())
                    self.server.server_state.tasks.add(task)
                    await asyncio.sleep(0)
                    raise RuntimeError('Owner failed')
        self.assertTrue(cleaned.is_set())
        self.assertTrue(task.done())
        self.socket.close.assert_called_once()

    async def test_startup_failure_closes_listener(self):
        def failed(config):
            server = self.factory(config)
            async def serve(**kwargs):
                raise RuntimeError('Startup failed')
            server.serve = serve
            return server
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', failed):
            with self.assertRaisesRegex(RuntimeError, 'Startup failed'):
                async with loopback_server(object()):
                    self.fail('Started failed server')
        self.socket.close.assert_called_once()

    async def test_owner_cancellation_propagates_after_listener_and_server_cleanup(self):
        entered = asyncio.Event()
        async def owner():
            async with loopback_server(object()):
                entered.set()
                await asyncio.Future()
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', self.factory):
            task = asyncio.create_task(owner())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.socket.close.assert_called_once()
        self.assertTrue(self.server.should_exit)

    async def test_actual_uvicorn_shutdown_cancellation_does_not_interrupt_async_cleanup(self):
        import uvicorn
        native_server = uvicorn.Server
        cleaned, entered = asyncio.Event(), asyncio.Event()
        async def active_request():
            entered.set()
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0.03)
                cleaned.set()
        def factory(config):
            config.timeout_graceful_shutdown = 0.001
            server = native_server(config)
            server.servers = []
            server.lifespan = SimpleNamespace(shutdown=AsyncMock())
            async def serve(**kwargs):
                server.started = True
                while not server.should_exit:
                    await asyncio.sleep(0.001)
                await server.shutdown()
            server.serve = serve
            self.server = server
            return server
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', factory):
            async with loopback_server(object()):
                task = asyncio.create_task(active_request())
                self.server.server_state.tasks.add(task)
                task.add_done_callback(self.server.server_state.tasks.discard)
                await entered.wait()
        self.assertTrue(cleaned.is_set())
        self.assertEqual(task.cancelling(), 1)
        self.assertTrue(task.done())
        self.socket.close.assert_called_once()

    async def test_failed_server_closes_active_connections_before_waiting_for_listener(self):
        loop = asyncio.get_running_loop()
        listener = asyncio.Server(loop, [], None, None, 128, None, None)
        listener._attach()
        closed = asyncio.Event()
        class Connection:
            def shutdown(self):
                closed.set()
                listener._detach()
        def factory(config):
            server = self.factory(config)
            server.servers = [listener]
            server.server_state.connections.add(Connection())
            async def serve(**kwargs):
                raise RuntimeError('Serving failure')
            server.serve = serve
            return server
        with patch('socket.socket', return_value=self.socket), patch('uvicorn.Server', factory):
            async with asyncio.timeout(1):
                with self.assertRaisesRegex(RuntimeError, 'Serving failure'):
                    async with loopback_server(object()):
                        self.fail('Unexpected readiness')
        self.assertTrue(closed.is_set())
        self.socket.close.assert_called_once()
