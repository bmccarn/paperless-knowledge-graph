"""Assembly ownership using synthetic stores, one captured fake call and no network."""
import base64
from contextlib import asynccontextmanager, contextmanager, ExitStack
from datetime import date
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main, query, cache
from scripts.live_query_runtime import query_runtime


class QueryRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.events = []
        self.request = {'question': 'Frozen question', 'mode': 'strict', 'model': 'synthetic', 'history': []}
        self.corpus = {'generation': 'redis:1'}
        self.guard = SimpleNamespace(started=False)
        self.observed = {'guard': self.guard, 'final': None}
        self.final = {'answer': 'Synthetic answer', 'query_plan': {'requirements_status': 'complete'}}
        self.documents = SimpleNamespace(documents={1: 'digest'}, hashes={}, changed=False,
                                         get_document=AsyncMock(return_value={}))
        self.readers = {'documents': self.documents,
                        'graph_reads': SimpleNamespace(denied=[]), 'vector_reads': SimpleNamespace(denied=[])}
        self.boundary = SimpleNamespace(bodies=[b'\xffpartial raw SSE'], statuses=[200], completed=[True],
                                         conserved_final=Mock(side_effect=lambda final, url: final))
        self.delivery = {'observed': self.observed, 'delivery': self.boundary,
                         'conversation': SimpleNamespace(messages=[]), 'app': object()}
        self.client = SimpleNamespace(max_retries=2,
            chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value={
                'choices': [{'index': 0, 'finish_reason': 'stop', 'message': {'content': 'Synthetic'}}]}))),
            embeddings=SimpleNamespace(create=AsyncMock()), close=AsyncMock())
        self.engine = SimpleNamespace(client=self.client)
        async def close_engine():
            self.events.append('engine-close'); await self.engine.client.close()
        self.engine.close = close_engine
        self.imported = SimpleNamespace(close=AsyncMock())
        self.orchestrator = SimpleNamespace(enabled=True, close=AsyncMock())
        self.patches = ExitStack(); self.addCleanup(self.patches.close)
        self.patches.enter_context(patch.object(main, 'query_engine', self.imported))
        self.patches.enter_context(patch.object(main, '_startup_ready', False))
        self.patches.enter_context(patch.object(main, '_background_workers', set()))
        self.patches.enter_context(patch.object(query.graph_store, 'driver', None))
        self.patches.enter_context(patch.object(query.embeddings_store, 'pool', None))
        self.patches.enter_context(patch.object(cache, '_corpus_generation', SimpleNamespace(_pending=False, _redis=None)))
        self.construct = self.patches.enter_context(patch.object(query, 'QueryEngine', return_value=self.engine))
        self.patches.enter_context(patch('app.strands_orchestrator.StrandsQueryOrchestrator', return_value=self.orchestrator))
        self.patches.enter_context(patch('app.answer_coverage.restore_question_coverage', return_value={'status': 'complete'}))
        self.snapshot = self.patches.enter_context(patch('scripts.live_query_runtime.current_corpus',
                                                        AsyncMock(side_effect=[self.corpus, self.corpus])))
        for name, replacement in (('capture_stages', self.stages), ('attach_readers', self.attach),
                                  ('delivery_session', self.session), ('loopback_server', self.server)):
            self.patches.enter_context(patch('scripts.live_query_runtime.' + name, replacement))

    @contextmanager
    def stages(self, orchestrator, capture, directory, *, reader_inventory):
        self.assertTrue(reader_inventory)
        self.capture_deadline = capture.deadline
        self.events.append('capture-open')
        try: yield {'hashes': {}, 'attempts': []}
        finally: self.events.append('capture-close')

    @asynccontextmanager
    async def attach(self, capture, directory):
        self.events.append('readers-open')
        try: yield self.readers
        finally: self.events.append('readers-close')

    @asynccontextmanager
    async def session(self, *args):
        self.seen_request = args[2]
        self.events.append('delivery-open')
        try: yield self.delivery
        finally: self.events.append('workers-joined')

    @asynccontextmanager
    async def server(self, app):
        self.events.append('server-open')
        try: yield {'port': 12345, 'url': 'http://127.0.0.1:12345'}
        finally: self.events.append('server-close')

    def runtime(self):
        return query_runtime(self.request, self.root, max_calls=10, seconds=10,
                             expected_corpus=self.corpus, evaluated_at=date.today().isoformat())

    async def finish_request(self):
        await self.engine.client.chat.completions.create(model='synthetic', messages=[])
        self.guard.started = True; self.observed['final'] = self.final

    async def test_complete_capture_and_result_follow_worker_drain(self):
        previous = query.strands_orchestrator
        async with self.runtime() as state:
            self.assertEqual(state['port'], 12345)
            await self.finish_request()
        self.assertIs(query.strands_orchestrator, previous)
        self.assertEqual(self.events[-5:], ['server-close', 'workers-joined', 'readers-close', 'capture-close', 'engine-close'])
        self.construct.assert_called_once()
        controls = self.construct.call_args.kwargs
        self.assertTrue(controls['question_pipeline'])
        self.assertEqual(controls['acquisition_deadline'], self.capture_deadline)
        self.assertTrue(callable(controls['acquisition_observer']))
        self.client.close.assert_awaited_once(); self.imported.close.assert_awaited_once()
        self.orchestrator.close.assert_awaited_once()
        self.documents.get_document.assert_awaited_once_with(1)
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertIsNone(artifact['error']); self.assertEqual(artifact['native_call_count'], 1)
        self.assertEqual(artifact['final'], self.final)
        self.assertEqual(base64.b64decode(artifact['delivery']['bodies_base64'][0]), self.boundary.bodies[0])
        self.assertEqual(len(artifact['model_sha256']), 2)
        self.assertNotIn('capture', artifact)

    async def test_preflight_drift_prevents_browser_and_model_start(self):
        self.snapshot.side_effect = [{'generation': 'redis:2'}]
        with self.assertRaisesRegex(ValueError, 'admitted snapshot'):
            async with self.runtime(): self.fail('Unexpected browser readiness')
        self.assertNotIn('server-open', self.events)
        self.client.chat.completions.create.assert_not_awaited()
        self.assertEqual(json.loads((self.root / 'runtime.json').read_bytes())['error'], 'ValueError')

    async def test_date_rollover_during_preflight_prevents_browser_submission(self):
        from datetime import timedelta
        today = date.today()
        with patch('scripts.live_query_runtime.date') as clock:
            clock.today.side_effect = [today, today + timedelta(days=1)]
            with self.assertRaisesRegex(ValueError, 'admitted snapshot'):
                async with self.runtime(): self.fail('Unexpected browser readiness')
        self.assertNotIn('server-open', self.events)
        self.client.chat.completions.create.assert_not_awaited()

    async def test_post_delivery_drift_fails_but_preserves_native_artifacts(self):
        self.snapshot.side_effect = [self.corpus, {'generation': 'redis:2'}]
        with self.assertRaisesRegex(ValueError, 'boundary changed'):
            async with self.runtime(): await self.finish_request()
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertEqual(artifact['error'], 'ValueError'); self.assertEqual(artifact['native_call_count'], 1)
        self.assertEqual(artifact['final'], self.final)

    async def test_incomplete_browser_request_cannot_qualify(self):
        with self.assertRaisesRegex(ValueError, 'did not complete'):
            async with self.runtime(): pass
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertEqual(artifact['error'], 'ValueError'); self.assertEqual(artifact['native_call_count'], 0)
        self.assertIn('workers-joined', self.events)

    async def test_mutating_caller_inputs_cannot_admit_corpus_drift_or_change_request(self):
        self.snapshot.side_effect = [{'generation': 'redis:1'}, {'generation': 'redis:2'}]
        with self.assertRaisesRegex(ValueError, 'boundary changed'):
            async with self.runtime() as ready:
                self.assertEqual(set(ready), {'port', 'url'})
                self.corpus['generation'] = 'redis:2'
                self.request['question'] = 'Changed caller question'
                await self.finish_request()
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertEqual(artifact['error'], 'ValueError')
        self.assertEqual(artifact['corpus_before'], {'generation': 'redis:1'})
        self.assertEqual(artifact['corpus_after'], {'generation': 'redis:2'})
        self.assertEqual(self.seen_request['question'], 'Frozen question')

    async def test_cleanup_failure_does_not_erase_original_failure_type(self):
        self.orchestrator.close.side_effect = RuntimeError('Synthetic cleanup failure')
        with self.assertRaises(RuntimeError):
            async with self.runtime():
                raise LookupError('Synthetic request failure')
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertEqual(artifact['error'], 'RuntimeError')
        self.assertEqual(artifact['exception_types'], ['RuntimeError', 'LookupError'])
        self.assertIn('readers-close', self.events)
        self.client.close.assert_awaited_once()

    async def test_denied_store_operation_invalidates_otherwise_complete_result(self):
        with self.assertRaisesRegex(ValueError, 'boundary changed'):
            async with self.runtime():
                await self.finish_request()
                self.readers['graph_reads'].denied.append('write_mutation')
        artifact = json.loads((self.root / 'runtime.json').read_bytes())
        self.assertEqual(artifact['denied_graph_operations'], ['write_mutation'])
        self.assertEqual(artifact['error'], 'ValueError')
