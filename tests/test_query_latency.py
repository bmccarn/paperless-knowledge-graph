"""Request-owned concurrency keeps serial evidence order without live services."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import query
from app.embeddings import EmbeddingsStore


class Gate:
    def __init__(self, values):
        self.values = values
        self.started = asyncio.Queue()
        self.release = {key: asyncio.Event() for key in values}
        self.active = 0
        self.peak = 0

    async def call(self, key, *args, **kwargs):
        self.active += 1
        self.peak = max(self.peak, self.active)
        await self.started.put(key)
        try:
            await self.release[key].wait()
            value = self.values[key]
            if isinstance(value, Exception):
                raise value
            return value
        finally:
            self.active -= 1


class QueryLatencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = object.__new__(query.QueryEngine)
        self.engine.question_pipeline = False

    async def run_gated(self, operation, gate, count):
        task = asyncio.create_task(operation)
        try:
            pending = count
            while pending:
                wave = [await asyncio.wait_for(gate.started.get(), .5) for _ in range(min(3, pending))]
                self.assertEqual(gate.active, min(3, pending))
                self.assertTrue(gate.started.empty())
                for key in reversed(wave):
                    gate.release[key].set()
                pending -= len(wave)
            result = await asyncio.wait_for(task, 1)
            self.assertEqual(gate.peak, min(3, count))
            self.assertEqual(gate.active, 0)
            return result
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_gap_searches_overlap_and_merge_in_proposal_order(self):
        from app.source_discovery import SourceDiscovery
        for broad, pipeline in ((False, False), (True, False), (False, True)):
            with self.subTest(broad=broad, question_pipeline=pipeline):
                self.engine.question_pipeline = pipeline
                proposals = [f'search {i}' for i in range(5)]
                used = proposals[:5 if broad or pipeline else 3]
                gate = Gate({q: {'vector_results': [{'document_id': i, 'content': q}]} for i, q in enumerate(used)})
                target, method = ((SourceDiscovery, 'search') if pipeline else
                                  (self.engine, '_retrieve_light' if broad else '_retrieve'))
                with patch.object(self.engine, '_synthesize_with_gaps', AsyncMock(return_value={'confidence': 1.0, 'follow_up_queries': proposals})), \
                        patch.object(target, method, side_effect=gate.call):
                    _, context, searches, _ = await self.run_gated(
                        self.engine._gap_review('Recorded items?', {}, [], 'deep', broad=broad), gate, len(used))
                self.assertEqual(searches, used)
                self.assertEqual([r['content'] for r in context['vector_results']], used)

    async def test_graph_lookups_overlap_keep_first_duplicate_and_skip_failures(self):
        names = [f'Entity {i}' for i in range(14)]
        values = {name: [{'properties': {'uuid': str(i % 4), 'name': name}}] for i, name in enumerate(names[:12])}
        values[names[2]] = ValueError('synthetic failure')
        gate = Gate(values)
        with patch.object(query.graph_store, 'search_nodes', side_effect=gate.call), \
                patch.object(query.graph_store, 'get_subgraph', AsyncMock(return_value={})):
            context = await self.run_gated(self.engine._expand_graph(names), gate, 12)
        self.assertEqual([n['properties']['name'] for n in context['graph_nodes']], [names[i] for i in (0, 1, 3, 6)])

    async def test_document_fetches_overlap_keep_slice_deduplication_and_failures(self):
        values = {i: {'content': f'Recorded value {i}', 'title': f'Record {i}'} for i in range(1, 5)}
        values[2] = ValueError('synthetic failure')
        gate = Gate(values)
        with patch.object(query.paperless_client, 'get_document', side_effect=gate.call):
            chunks = await self.run_gated(self.engine._expand_source_documents(
                'Recorded value?', [1, 2, 1, 3, 4, 5], high_accuracy=True), gate, 4)
        self.assertEqual([r['document_id'] for r in chunks], [1, 3, 4])

    async def test_cancelled_operations_join_children_even_after_repeated_cancellation(self):
        from app.source_discovery import SourceDiscovery
        for kind in ('gap', 'broad_gap', 'pipeline_gap', 'graph', 'documents'):
            with self.subTest(kind=kind):
                self.engine.question_pipeline = kind == 'pipeline_gap'
                active = set()
                started = asyncio.Event()
                cleaning = asyncio.Event()
                finish_cleanup = asyncio.Event()
                async def blocked(key, *args, **kwargs):
                    active.add(key)
                    if len(active) == 3:
                        started.set()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cleaning.set()
                        await finish_cleanup.wait()
                        active.remove(key)
                if kind in ('gap', 'broad_gap', 'pipeline_gap'):
                    target, method = ((SourceDiscovery, 'search') if kind == 'pipeline_gap' else
                                      (self.engine, '_retrieve_light' if kind == 'broad_gap' else '_retrieve'))
                    operation = self.engine._gap_review('Recorded items?', {}, [], 'deep', broad=kind == 'broad_gap')
                elif kind == 'graph':
                    target, method = query.graph_store, 'search_nodes'
                    operation = self.engine._expand_graph(list('abcdef'))
                else:
                    target, method = query.paperless_client, 'get_document'
                    operation = self.engine._expand_source_documents('Recorded?', list(range(5)), True)
                with patch.object(self.engine, '_synthesize_with_gaps', AsyncMock(return_value={'follow_up_queries': list('abc')})), \
                        patch.object(target, method, side_effect=blocked):
                    task = asyncio.create_task(operation)
                    try:
                        await asyncio.wait_for(started.wait(), .5)
                        task.cancel()
                        await asyncio.wait_for(cleaning.wait(), .5)
                        task.cancel()
                        self.assertTrue(active)
                        finish_cleanup.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task, 1)
                        self.assertFalse(active)
                    finally:
                        finish_cleanup.set()
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    async def test_embedding_reuse_is_per_query_and_per_request(self):
        store = object.__new__(EmbeddingsStore)
        store.generate_embedding = AsyncMock(return_value=[.5])
        connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
        manager = type('Connection', (), {'__aenter__': AsyncMock(return_value=connection), '__aexit__': AsyncMock()})()
        store.pool = SimpleNamespace(acquire=lambda: manager)
        from app.embeddings import query_embedding_scope
        for _ in range(2):
            async with query_embedding_scope():
                await asyncio.gather(store.vector_search('same'), store.entity_vector_search('same'),
                                     store.vector_search_by_doc_ids('same', [1]), store.vector_search('different'))
        self.assertEqual(store.generate_embedding.await_count, 4)

    async def test_embedding_scope_joins_cancelled_provider_and_isolates_simultaneous_requests(self):
        from app.embeddings import query_embedding_scope
        store = object.__new__(EmbeddingsStore)
        gate = Gate({'same': [.5]})
        store.generate_embedding = AsyncMock(side_effect=gate.call)
        async def request():
            async with query_embedding_scope():
                return await store._query_embedding('same')
        tasks = [asyncio.create_task(request()) for _ in range(2)]
        try:
            await asyncio.wait_for(gate.started.get(), .5)
            await asyncio.wait_for(gate.started.get(), .5)
            tasks[0].cancel()
            with self.assertRaises(asyncio.CancelledError):
                await tasks[0]
            self.assertEqual(gate.active, 1)
            gate.release['same'].set()
            self.assertEqual(await tasks[1], [.5])
            self.assertEqual(store.generate_embedding.await_count, 2)
            self.assertEqual(gate.active, 0)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_audit_and_repair_timing_preserves_inputs_and_failures(self):
        calls = SimpleNamespace(audit_answer_units=AsyncMock(return_value={'ok': True}),
                                repair_answer=AsyncMock(side_effect=ValueError('synthetic failure')))
        timing = {'audit': [], 'repair': []}
        token = query._REQUEST_TIMINGS.set(timing)
        try:
            observed = query._TimedFinalization(calls)
            args = ('question', ['units'], ['spans'], {'plan': True})
            self.assertEqual(await observed.audit_answer_units(*args), {'ok': True})
            with self.assertRaisesRegex(ValueError, 'synthetic failure'):
                await observed.repair_answer(*args)
            calls.audit_answer_units.assert_awaited_once_with(*args)
            calls.repair_answer.assert_awaited_once_with(*args)
            self.assertEqual(len(timing['audit']), 1)
            self.assertEqual(len(timing['repair']), 1)
        finally:
            query._REQUEST_TIMINGS.reset(token)

    async def test_one_content_free_stage_line_per_request_includes_each_audit_and_repair(self):
        from tests.test_query_delivery import RetrievedEngine
        from tests.test_answer_finalization import SupportedAuditor
        engine = RetrievedEngine()
        with patch.object(query, 'strands_orchestrator', SupportedAuditor()), \
                patch.object(query, 'cache_get', AsyncMock(return_value=None)), \
                patch.object(query, 'cache_set', AsyncMock()), \
                patch.object(query.embeddings_store, 'get_incomplete_document_ids', AsyncMock(return_value=set())), \
                patch.object(query.embeddings_store, 'get_open_feedback_document_ids', AsyncMock(return_value=set())), \
                patch.object(query, 'source_dates', AsyncMock(return_value={})), \
                self.assertLogs('app.query', level='INFO') as logs:
            await engine.query('Recorded premium?', mode='deep')
        lines = [line.split('query_stage_timings ', 1)[1] for line in logs.output if 'query_stage_timings ' in line]
        self.assertEqual(len(lines), 1)
        timing = json.loads(lines[0])
        self.assertEqual(set(('planning', 'primary_retrieval', 'gap_review', 'expansion', 'draft', 'audit', 'repair')) - timing['seconds'].keys(), set())
        self.assertTrue(timing['seconds']['audit'])
        self.assertNotIn('premium', lines[0])
