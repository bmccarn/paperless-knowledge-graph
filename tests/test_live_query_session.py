"""Exercise lifetime boundaries with the actual route and query stream."""
import asyncio
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from app.query import QueryEngine
from scripts.live_query_session import delivery_session
from tests.test_live_query_delivery import Engine


class LiveSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_delivery_is_conserved_and_owned_workers_are_joined(self):
        import httpx
        previous = (main.query_engine, main.conversations, main._start_background_worker)
        engine = Engine()
        async with delivery_session(main, engine, {'question': 'Exact question', 'mode': 'strict'}, lambda history: {}) as run:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=run['app']),
                                         base_url='http://127.0.0.1') as client:
                response = await client.post('/query/stream', json={
                    'question': 'Exact question', 'mode': 'strict', 'conversation_id': 'evaluation'})
                self.assertEqual(response.status_code, 200)
                run['delivery'].conserved_final(run['observed']['final'], main._get_paperless_url())
        self.assertEqual((main.query_engine, main.conversations, main._start_background_worker), previous)
        self.assertTrue(run['workers'])
        self.assertTrue(all(task.done() for task in run['workers']))
        self.assertEqual(len(run['conversation'].messages), 2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=run['app']),
                                     base_url='http://127.0.0.1') as client:
            self.assertEqual((await client.post('/query/stream', json={})).status_code, 403)
        self.assertEqual(len(engine.calls), 1)

    async def test_disconnect_and_context_failure_join_inner_query_before_restoration(self):
        import httpx
        entered, cleaned = asyncio.Event(), asyncio.Event()
        class BlockingEngine:
            query_stream = QueryEngine.query_stream
            async def query(self, *args):
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    # Cleanup still sees the isolated engine and private history.
                    self.attached_during_cleanup = main.query_engine is self
                    await asyncio.sleep(0)
                    cleaned.set()
        engine = BlockingEngine()
        with self.assertRaisesRegex(RuntimeError, 'Abort experiment'):
            async with delivery_session(main, engine, {'question': 'Exact question', 'mode': 'strict'}, lambda history: {}) as run:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=run['app']),
                                             base_url='http://127.0.0.1') as client:
                    request = asyncio.create_task(client.post('/query/stream', json={
                        'question': 'Exact question', 'mode': 'strict'}))
                    await asyncio.wait_for(entered.wait(), 2)
                    request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
                    self.assertFalse(cleaned.is_set())
                    raise RuntimeError('Abort experiment')
        self.assertTrue(cleaned.is_set())
        self.assertTrue(engine.attached_during_cleanup)
        self.assertTrue(all(task.done() for task in run['workers']))
        self.assertFalse(main._background_workers)
        with self.assertRaises(ValueError):
            run['delivery'].conserved_final({}, main._get_paperless_url())

    async def test_serving_or_busy_application_is_rejected_before_patching(self):
        engine = Engine(); previous = main.query_engine
        for attribute, value in (('_startup_ready', True), ('_background_workers', {object()})):
            with patch.object(main, attribute, value), self.assertRaises(ValueError):
                async with delivery_session(main, engine, {}, lambda history: {}):
                    self.fail('Serving process admitted')
            self.assertIs(main.query_engine, previous)
