"""Ordinary query discovery -> originals -> reader -> audit -> saved receipt."""
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.query import QueryEngine
from app.cache import invalidate_on_sync
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_coverage import restore_question_coverage
from app.source_acquisition import acquisition_digest
from tests.test_source_acquisition import Index, Originals, docs
from tests import test_question_pipeline as pipeline_fixtures


class QueryIndex(Index):
    async def vector_search(self, query, limit=20, strict=False): return []
    async def keyword_search(self, query, limit=15, strict=False): return []
    async def entity_vector_search(self, query, limit=8, strict=False): return [{'entity_uuid': 'station-owner'}]
    async def entity_keyword_search(self, query, limit=8, strict=False): return []


class AcquisitionQueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        invalidate_on_sync()
        self.engine = QueryEngine(question_pipeline=True)
        self.orchestrator = StrandsQueryOrchestrator(); self.orchestrator.enabled = True
        self.calls = []; self.fail_stage = None
        self.original = docs(2)
        self.original[1]['content'] = 'Monthly premium: $321.00 USD.'
        self.original[2]['content'] = 'Later station statement.'
        self.index = QueryIndex(self.original); self.index.ids = [1]
        self.graph = type('Graph', (), {'get_node': AsyncMock(return_value={
            'properties': {'uuid': 'station-owner', 'name': 'Station', 'source_doc_ids': [1]}}),
            'search_nodes': AsyncMock(return_value=[]), 'get_subgraph': AsyncMock(return_value={})})()
        self.patches = [patch('app.query.embeddings_store', self.index),
            patch('app.query.paperless_client', Originals(self.original)),
            patch('app.query.graph_store', self.graph),
            patch('app.query.strands_orchestrator', self.orchestrator),
            patch.object(self.orchestrator, '_text_agent', side_effect=self.model)]
        for p in self.patches: p.start()

    async def asyncTearDown(self):
        for p in reversed(self.patches): p.stop()
        await self.engine.close()

    async def model(self, **kwargs):
        return await pipeline_fixtures.QuestionPipelineTests.model(self, **kwargs)

    async def test_complete_query_and_incomplete_graph_lead_survive_restoration(self):
        for incomplete in (False, True):
            invalidate_on_sync(); self.calls.clear()
            if incomplete:
                self.graph.get_node.return_value['properties']['source_doc_ids'] = [1, 2]
                self.index.missing = {2}
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
            final = result['finalization']
            self.assertTrue(final['answer_verified'], final)
            self.assertEqual(result['evidence_pack']['coverage']['evidence_item_count'], 1)
            self.assertEqual(result['evidence_pack']['coverage']['source_document_count'], 1)
            self.assertEqual(final['question_coverage']['complete'], not incomplete)
            restored = restore_question_coverage(result)
            self.assertIsNotNone(restored)
            self.assertEqual(restored['complete'], not incomplete)
            self.assertEqual(final['source_acquisition']['complete'], not incomplete)
            self.assertEqual(self.calls.count('source_reader'), 1)
            if incomplete:
                self.assertEqual(final['source_acquisition']['documents'][-1]['state'], 'unindexed')
                self.assertFalse(self.engine._cacheable_answer(result, 'quick',
                    request_identity=result['query_plan']['request_identity_digest']))
                altered = copy.deepcopy(result)
                receipt = altered['finalization']['source_acquisition']
                receipt['documents'] = receipt['documents'][:1]; receipt['complete'] = True
                receipt['digest'] = acquisition_digest({k: v for k, v in receipt.items() if k != 'digest'})
                altered['finalization']['question_coverage'].update(complete=True, status='complete',
                    acquisition_complete=True, acquisition_digest=receipt['digest'])
                self.assertIsNone(restore_question_coverage(altered))
                stripped = copy.deepcopy(result)
                for key in ('acquisition_required', 'acquisition_request_digest'):
                    stripped['query_plan'].pop(key)
                for key in ('source_acquisition', 'acquisition_inventory_digest'):
                    stripped['finalization'].pop(key)
                for key in ('acquisition_digest', 'acquisition_complete'):
                    stripped['finalization']['question_coverage'].pop(key)
                stripped['finalization']['question_coverage'].update(complete=True, status='complete')
                self.assertIsNone(restore_question_coverage(stripped))

    async def test_every_planned_and_gap_query_reaches_discovery(self):
        original_model = self.model
        async def many(**kwargs):
            value = json.loads(await original_model(**kwargs))
            if kwargs['name'] == 'query_planner':
                value['subqueries'] = [{'role': 'planned', 'query': f'station phase {i}'} for i in range(11)]
            return json.dumps(value)
        with patch.object(self.orchestrator, '_text_agent', side_effect=many), patch.object(
                self.engine, '_synthesize_with_gaps', AsyncMock(return_value={
                    'follow_up_queries': [f'late station section {i}' for i in range(5)], 'entities_found': []})):
            result = await self.engine.query('What monthly premium is recorded?', mode='strict')
        queries = [o['query'] for o in result['finalization']['source_acquisition']['operations']
                   if o.get('query') and o['id'].startswith(('planned:', 'gap:'))]
        self.assertEqual(len(queries), 17)
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(self.calls.count('source_reader'), 1)

    async def test_malformed_actual_gap_response_is_an_acquisition_failure(self):
        with patch.object(self.engine, '_llm_json', AsyncMock(return_value={})):
            result = await self.engine.query('What monthly premium is recorded?', mode='strict')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertFalse(result['finalization']['question_coverage']['complete'])
        operations = result['finalization']['source_acquisition']['operations']
        self.assertEqual(next(o for o in operations if o['id'] == 'gap_planning')['status'], 'failed')

    async def test_outer_query_cancellation_retains_completed_and_pending_progress(self):
        import asyncio
        self.index.ids = [1, 2]
        captured = []
        self.engine.acquisition_observer = captured.append
        waiting = asyncio.Event()
        sources = Originals(self.original)
        async def partial(i):
            if i == 2:
                waiting.set(); await asyncio.Event().wait()
            return await sources.get_document(i)
        with patch('app.query.paperless_client.get_document', side_effect=partial), patch(
                'app.query.settings.strands_max_concurrent_calls', 1):
            task = asyncio.create_task(self.engine.query('What monthly premium is recorded?', mode='quick'))
            await waiting.wait(); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(len(captured), 1)
        progress = captured[0]['progress']
        self.assertEqual(progress['snapshot_status'], 'cancelled')
        self.assertEqual(progress['documents']['1']['state'], 'supplied')
        self.assertEqual(progress['documents']['2']['state'], 'pending')
        self.assertTrue(progress['documents']['1']['spans'])
        self.assertNotIn('source_reader', self.calls)
