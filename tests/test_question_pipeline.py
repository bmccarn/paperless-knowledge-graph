"""Public all-mode inactive pipeline contract with original-source model controls."""
import json
import unittest
from unittest.mock import patch, AsyncMock

from tests.runtime import configure_test_environment
configure_test_environment()
from app.query import QueryEngine
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.query_metrics import record_native_stage
from app.cache import invalidate_on_sync
from app.answer_coverage import restore_question_coverage, restore_pipeline_metadata
import copy
from tests.test_query_delivery import RetrievedEngine
from tests.source_audit_fixtures import decision


class PipelineEngine(RetrievedEngine):
    question_pipeline = True
    _build_query_plan = QueryEngine._build_query_plan

    async def _final_synthesis(self, *args, **kwargs):
        raise AssertionError('Old draft synthesis must not run')


class QuestionPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        invalidate_on_sync()
        self.engine = PipelineEngine()
        self.orchestrator = StrandsQueryOrchestrator(); self.orchestrator.enabled = True
        self.calls = []
        self.fail_stage = None
        self.patches = [patch('app.query.strands_orchestrator', self.orchestrator),
            patch('app.query.embeddings_store.get_incomplete_document_ids', AsyncMock(return_value=set())),
            patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())),
            patch.object(self.orchestrator, '_text_agent', side_effect=self.model)]
        for p in self.patches: p.start()

    async def asyncTearDown(self):
        for p in self.patches: p.stop()

    async def model(self, **kwargs):
        name = kwargs['name']; self.calls.append(name); record_native_stage(name)
        if self.fail_stage == name: return None
        if name == 'query_planner':
            return json.dumps({'intent': 'lookup', 'domain': 'general', 'requires_current': False,
                'needs_timeline': False, 'must_answer_current_vs_historical': False,
                'required_doc_types': [], 'subqueries': [], 'reasoning': 'Requested source value',
                'resolved_question': 'What monthly premium is recorded?', 'requirements': [{
                    'id': 'r1', 'aspect': 'Recorded monthly premium', 'temporal_scope': 'none', 'comparison_scope': 'none'}]})
        payload = json.loads(kwargs['prompt'])
        if name == 'source_reader':
            d = payload['source_documents'][0]
            return json.dumps({'documents': [{'document_id': d['document_id'], 'observations': [
                {'text': 'The statement records a monthly premium of $321.00 USD.',
                 'references': [{'span_id': d['windows'][0]['span']['span_id']}]}], 'limitations': []}]})
        if name == 'answer_composer':
            handle = payload['source_documents'][0]['windows'][0]['span']['span_id']
            return json.dumps({'observations': ['The statement records a monthly premium of $321.00 USD.'],
                'requirement_mapping': [{'requirement_id': 'r1', 'observation_ids': ['u1'], 'status': 'proposed'}],
                'source_references': [{'observation_id': 'u1', 'span_ids': [handle]}]})
        if name == 'source_auditor':
            handle = payload['source_documents'][0]['windows'][0]['span']['span_id']
            return json.dumps({'assessments': [decision(unit_id=u['id'], references=[{'span_id': handle}])
                                               for u in payload['units']]})
        if name == 'answer_coverage':
            return json.dumps({'requirements': [{'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']}],
                               'omitted_requested_aspects': False})
        self.fail('Unexpected model stage: ' + name)

    async def test_every_mode_uses_original_reading_composition_audit_and_coverage(self):
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            self.calls.clear()
            result = await self.engine.query('What monthly premium is recorded?', mode=mode)
            self.assertTrue(result['finalization']['answer_verified'], mode)
            self.assertTrue(result['finalization']['question_coverage']['complete'])
            self.assertEqual(self.calls, ['query_planner', 'source_reader', 'answer_composer', 'source_auditor', 'answer_coverage'])
            execution = result['finalization']['pipeline_execution']
            self.assertEqual(execution['native_call_count'], 5)
            self.assertEqual(execution['native_call_ceiling'], 7)
            self.assertFalse(result['cached'])

    async def test_http_style_and_sse_deliver_identical_final_state_without_drafts(self):
        ordinary = await self.engine.query('What monthly premium is recorded?', mode='quick')
        events = [e async for e in self.engine.query_stream('What monthly premium is recorded?', mode='quick')]
        self.assertEqual([e['type'] for e in events], ['status', 'complete'])
        streamed = {k: v for k, v in events[-1].items() if k != 'type'}
        ordinary['cached'] = True
        for result in (streamed, ordinary):
            for step in result['trace']:
                step.pop('timestamp', None)  # Separate requests have distinct progress times.
        self.assertEqual(streamed, ordinary)

    async def test_resolved_followup_guides_the_actual_quick_retrieval(self):
        plan, _ = await self.engine._build_query_plan('What about that value?', 'quick')
        retrieve = AsyncMock(return_value={'vector_results': [], 'graph_nodes': []})
        with patch.object(self.engine, '_retrieve', retrieve):
            await QueryEngine._execute_retrieval_plan(self.engine, 'What about that value?', plan, 'quick')
        retrieve.assert_awaited_once_with('What monthly premium is recorded?')
        self.assertEqual(plan['original_question'], 'What about that value?')

    async def test_cache_and_saved_receipts_bind_question_sources_and_final_ledger(self):
        result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        self.assertIsNotNone(restore_question_coverage(result))
        self.assertTrue(self.engine._cacheable_answer(result, 'quick', request_identity=result['query_plan']['request_identity_digest']))
        for change in (lambda x: x['finalization'].pop('question_coverage'),
                       lambda x: x['finalization'].update(disposition='unaudited'),
                       lambda x: x['query_plan'].update(original_question='Different question'),
                       lambda x: x['query_plan'].update(resolved_question='Different resolution'),
                       lambda x: x['query_plan'].update(source_date_order='dmy'),
                       lambda x: x['query_plan']['requirements'][0].update(aspect='Different requested aspect'),
                       lambda x: x['claim_ledger']['spans'][0].update(content_digest='changed'),
                       lambda x: x['claim_ledger']['claims'][0]['references'][0].update(quote='Monthly premium: $999.00 USD.'),
                       lambda x: x['finalization'].update(evidence_snapshot_digest='a' * 64)):
            changed = copy.deepcopy(result); change(changed)
            self.assertFalse(self.engine._cacheable_answer(changed, 'quick'))
            restored = restore_pipeline_metadata(changed, changed['answer'])
            self.assertFalse(restored['finalization']['question_coverage']['complete'])
            self.assertFalse(restored['finalization']['answer_verified'])
            self.assertEqual(restored['answer'], result['answer'])
        legacy = {'finalization': {'disposition': 'unaudited'}, 'answer': 'Old unaudited response'}
        self.assertEqual(restore_pipeline_metadata(legacy, legacy['answer']), legacy)
        self.assertFalse(self.engine._cacheable_answer(legacy, 'quick'))

    async def test_legacy_quick_cache_hit_cannot_bypass_new_pipeline(self):
        legacy = {'finalization': {'disposition': 'unaudited'}, 'answer': 'Old unaudited response'}
        with patch('app.query.cache_get', AsyncMock(return_value=legacy)):
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertFalse(result['cached'])
        self.assertIn('source_auditor', self.calls)

    async def test_coarse_plan_never_claims_complete_question_coverage(self):
        self.fail_stage = 'query_planner'
        result = await self.engine.query('Describe the recorded information.', mode='quick')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(result['query_plan']['requirements_status'], 'coarse')
        self.assertFalse(result['finalization']['question_coverage']['complete'])

    async def test_reader_and_composer_failure_never_fall_back_to_old_draft(self):
        for stage in ('source_reader', 'answer_composer'):
            self.fail_stage = stage
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(result['finalization']['pipeline_failure'], stage)
            self.assertNotIn('$321', result['answer'])

    async def test_coverage_failure_keeps_verified_facts_with_unavailable_coverage(self):
        self.fail_stage = 'answer_coverage'
        result = await self.engine.query('What monthly premium is recorded?', mode='strict')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertIn('$321', result['answer'])
        self.assertEqual(result['finalization']['question_coverage']['status'], 'unavailable')

    async def test_actual_http_and_sse_endpoints_preserve_the_coverage_receipt(self):
        import httpx
        from app import main
        with patch.object(main, 'query_engine', self.engine):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://synthetic.local') as client:
                ordinary = await client.post('/query', json={'question': 'What monthly premium is recorded?', 'mode': 'quick'})
                streamed = await client.post('/query/stream', json={'question': 'What monthly premium is recorded?', 'mode': 'quick'})
        self.assertEqual(ordinary.status_code, 200)
        self.assertEqual(streamed.status_code, 200)
        terminal = next(json.loads(line[6:]) for line in streamed.text.splitlines()
                        if line.startswith('data: ') and json.loads(line[6:]).get('type') == 'complete')
        self.assertEqual(ordinary.json()['finalization']['question_coverage'], terminal['finalization']['question_coverage'])
        self.assertIsNotNone(restore_question_coverage(terminal))

    async def test_conversation_restoration_downgrades_invalid_receipts_without_writing_rows(self):
        import uuid
        from datetime import datetime, timezone
        from types import SimpleNamespace
        from contextlib import asynccontextmanager
        from app import conversations
        result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        changed = copy.deepcopy(result); changed['finalization'].pop('question_coverage')
        now = datetime.now(timezone.utc)
        base = {'id': uuid.uuid4(), 'role': 'assistant', 'content': result['answer'], 'sources': json.dumps([{'document_id': 999, 'title': 'Wrong source', 'excerpt': '$999', 'paperless_url': 'https://wrong.invalid'}]),
                'entities': None, 'confidence': .8, 'query_time_ms': 10, 'cached': False,
                'follow_ups': None, 'created_at': now}
        rows = [{**base, 'metadata': json.dumps(r)} for r in (result, changed)]
        original = copy.deepcopy(rows)
        connection = SimpleNamespace(fetchrow=AsyncMock(return_value={
            'id': uuid.uuid4(), 'title': 'Saved query', 'created_at': now, 'updated_at': now}),
            fetch=AsyncMock(return_value=rows))
        @asynccontextmanager
        async def acquire(): yield connection
        with patch.object(conversations, '_pool', SimpleNamespace(acquire=acquire)):
            restored = await conversations.get_conversation(str(uuid.uuid4()))
        self.assertTrue(restored['messages'][0]['finalization']['question_coverage']['complete'])
        sources = restored['messages'][0]['sources']
        self.assertEqual([s['document_id'] for s in sources], [101])
        self.assertIn('$321', sources[0]['excerpt'])
        self.assertNotIn('wrong.invalid', sources[0]['paperless_url'])
        self.assertEqual(restored['messages'][1]['sources'], [])
        self.assertFalse(restored['messages'][1]['finalization']['answer_verified'])
        self.assertFalse(restored['messages'][1]['finalization']['question_coverage']['complete'])
        self.assertEqual(restored['messages'][1]['confidence'], 0)
        self.assertEqual(restored['messages'][1]['content'], result['answer'])
        self.assertEqual(rows, original)

    async def test_other_request_cache_entry_is_rejected(self):
        prior = await self.engine.query('What monthly premium is recorded?', mode='quick')
        for question, mode, history in (
                ('What annual charge is recorded?', 'quick', None),
                ('What monthly premium is recorded?', 'strict', None),
                ('What monthly premium is recorded?', 'quick', [{'role': 'user', 'content': 'Different context'}])):
            self.calls.clear()
            with patch('app.query.cache_get', AsyncMock(return_value=copy.deepcopy(prior))):
                result = await self.engine.query(question, mode=mode, conversation_history=history)
            self.assertFalse(result['cached'])
            self.assertIn('answer_composer', self.calls)
            self.assertNotEqual(result['query_plan']['request_identity_digest'], prior['query_plan']['request_identity_digest'])

    async def test_corrupt_saved_success_states_never_retain_verified_presentation(self):
        result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        mutations = [lambda r: r['finalization'].pop('pipeline_version'),
            lambda r: r['finalization'].update(pipeline_version='question-evidence-v0'),
            lambda r: r.update(finalization=[]), lambda r: r.update(query_plan=[])]
        mutations += [lambda r, value=value: r['finalization'].update(answer_verified=value)
                      for value in (False, 1, 'true')]
        mutations += [lambda r, value=value: (r['finalization'].pop('question_coverage'),
                      r['evidence'].update(coverage=value)) for value in ([], ['bad'], 'bad', True)]
        for mutate in mutations:
            changed = copy.deepcopy(result); mutate(changed)
            restored = restore_pipeline_metadata(changed, result['answer'])
            self.assertFalse(restored['finalization']['answer_verified'])
            self.assertFalse(restored['finalization']['complete'])
            self.assertFalse(restored['finalization']['question_coverage']['complete'])
            self.assertEqual(restored['verification']['status'], 'unavailable')
            self.assertEqual(restored['evidence']['score'], 0)
            self.assertEqual(restored['claim_ledger']['claims'], [])
            self.assertFalse(restored['source_summary']['claim_summary'])
            self.assertFalse(restored['source_summary']['trust_dimensions'])
            self.assertFalse(restored['source_summary']['trust_reasons'])
            self.assertEqual(restored['answer'], result['answer'])

    async def test_cache_sources_and_freshness_use_bound_references(self):
        prior = await self.engine.query('What monthly premium is recorded?', mode='quick')
        prior['sources'] = [{'document_id': 999, 'excerpt': '$999'}]
        prior['evidence_pack']['items'] = []
        with patch('app.query.cache_get', AsyncMock(return_value=copy.deepcopy(prior))):
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        self.assertEqual([s['document_id'] for s in result['sources']], [101])
        self.assertIn('$321', result['sources'][0]['excerpt'])
        async def incomplete(ids): return {101} if 101 in ids else set()
        with patch('app.query.cache_get', AsyncMock(return_value=copy.deepcopy(prior))), patch(
                'app.query.embeddings_store.get_incomplete_document_ids', side_effect=incomplete):
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['finalization']['disposition'], 'corpus_changed')

    async def test_failed_execution_remains_identifiable_when_restored(self):
        for stage in ('source_reader', 'answer_composer'):
            self.fail_stage = stage
            result = await self.engine.query('What monthly premium is recorded?', mode='quick')
            restored = restore_pipeline_metadata(result, result['answer'])
            self.assertEqual(restored, result)
            self.assertEqual(restored['finalization']['pipeline_failure'], stage)
