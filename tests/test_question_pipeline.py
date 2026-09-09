"""Public all-mode inactive pipeline contract with original-source model controls."""
import json
import unittest
from unittest.mock import patch, AsyncMock

from tests.runtime import configure_test_environment
configure_test_environment()
from app.query import QueryEngine
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.query_metrics import record_native_stage
from tests.test_query_delivery import RetrievedEngine
from tests.source_audit_fixtures import decision


class PipelineEngine(RetrievedEngine):
    question_pipeline = True
    _build_query_plan = QueryEngine._build_query_plan

    async def _final_synthesis(self, *args, **kwargs):
        raise AssertionError('Old draft synthesis must not run')


class QuestionPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
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
        for result in (streamed, ordinary):
            for step in result['trace']:
                step.pop('timestamp', None)  # Separate requests have distinct progress times.
        self.assertEqual(streamed, ordinary)

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
