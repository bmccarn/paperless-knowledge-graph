"""Completion preserves verified facts and never bypasses the original audit."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_coverage import restore_question_coverage
from app.question_pipeline import finalize_question
from app.question_evidence import PIPELINE_VERSION
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.query_metrics import QueryMetrics, CURRENT_QUERY_METRICS, record_native_stage
from tests.source_audit_fixtures import decision


class AnswerCompletionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.first = 'The record lists the original charge as $310 USD.'
        self.second = 'The record lists the revised balance as $270 USD.'
        self.question = 'What original charge and revised balance are recorded?'
        self.source = self.first + '\n' + self.second + '\nThe record labels the account blue.'
        self.pack = {'items': [{'id': 'record', 'document_id': 17, 'chunk_index': 0,
            'title': 'Account record', 'source_kind': 'ocr', 'content': self.source, 'source_content': self.source}]}
        self.plan = {'resolved_question': self.question, 'original_question': self.question,
            'requirements': [{'id': f'r{i}', 'aspect': aspect, 'temporal_scope': 'none', 'comparison_scope': 'none'}
                             for i, aspect in enumerate(('Original charge', 'Revised balance'), 1)],
            'evaluated_at': '2026-09-09', 'source_date_order': 'mdy', 'requirements_status': 'complete',
            'request_identity_digest': 'a' * 64, 'pipeline_version': PIPELINE_VERSION}
        self.calls, self.payloads = [], []
        self.behavior = 'recover'
        self.orchestrator = StrandsQueryOrchestrator(); self.orchestrator.enabled = True

    async def model(self, **kwargs):
        name, payload = kwargs['name'], json.loads(kwargs['prompt'])
        self.calls.append(name); self.payloads.append((name, copy.deepcopy(payload))); record_native_stage(name)
        if name == 'source_reader':
            doc = payload['source_documents'][0]
            return json.dumps({'documents': [{'document_id': doc['document_id'], 'observations': [
                    {'text': text, 'references': [{'span_id': doc['windows'][0]['span']['span_id']}]}
                    for text in ((self.second, self.first) if self.behavior == 'complete' else (self.second,))], 'limitations': []}]})
        if name == 'fact_selector':
            return json.dumps({'dispositions': [{'observation_id': row['id'], 'status': 'delivered'}
                for row in payload['observations']]})
        if name == 'answer_completion':
            completion = name == 'answer_completion'
            if completion and self.behavior == 'cancel': raise asyncio.CancelledError
            if completion and self.behavior == 'malformed': return 'not json'
            observations = ([self.first] if completion else [self.second])
            if self.behavior == 'complete' and not completion: observations = [self.second, self.first]
            if completion and self.behavior == 'none': observations = []
            if completion and self.behavior == 'duplicate': observations = [self.second]
            if completion and self.behavior == 'invented': observations = ['The original charge was $999 USD.']
            if completion and self.behavior == 'unrelated': observations = ['The record labels the account blue.']
            mapping = []
            for requirement in payload['requirements']:
                ids = (['u1'] if completion and observations else
                       ['u1'] if not completion and requirement['id'] == 'r2' else
                       ['u2'] if self.behavior == 'complete' else [])
                mapping.append({'requirement_id': requirement['id'], 'observation_ids': ids,
                                'status': 'proposed' if ids else 'unresolved'})
            span = payload['source_documents'][0]['windows'][0]['span']['span_id']
            if completion and self.behavior == 'foreign': span = 'unavailable-original'
            return json.dumps({'observations': observations, 'requirement_mapping': mapping,
                'source_references': [{'observation_id': f'u{i}', 'span_ids': [span]}
                                      for i in range(1, len(observations) + 1)]})
        if name == 'source_auditor':
            span = payload['source_documents'][0]['windows'][0]['span']['span_id']
            combined = 'answer_completion' in self.calls
            return json.dumps({'assessments': [decision(unit_id=u['id'], references=[{'span_id': span}],
                status='unsupported' if combined and self.behavior == 'revoked' and u['id'] == 'u1' else 'supported')
                for u in payload['units']]})
        if name == 'answer_coverage':
            combined = 'answer_completion' in self.calls
            if combined and self.behavior == 'coverage_unavailable': return None
            answered = self.behavior == 'complete' or (combined and self.behavior != 'unrelated')
            return json.dumps({'requirements': [
                {'requirement_id': 'r1', 'status': 'answered' if answered else 'unresolved',
                 'observation_ids': ['u2'] if answered else []},
                {'requirement_id': 'r2', 'status': 'answered', 'observation_ids': ['u1']}],
                'omitted_requested_aspects': False})
        self.fail('Unexpected stage: ' + name)

    async def run_pipeline(self, mode='strict'):
        self.calls.clear(); self.payloads.clear()
        plan = {**self.plan, 'mode': mode}
        metrics = QueryMetrics(); token = CURRENT_QUERY_METRICS.set(metrics)
        try:
            with patch.object(self.orchestrator, '_text_agent', side_effect=self.model):
                final = await finalize_question(self.orchestrator, self.question, self.pack, plan, mode)
        finally:
            CURRENT_QUERY_METRICS.reset(token)
        final.update(question=self.question, query_plan=plan, mode=mode)
        self.assertLessEqual(metrics.report()['native_call_count'], metrics.report()['native_call_ceiling'])
        return final

    async def test_reader_gap_is_recovered_in_all_modes_only_after_combined_original_audit(self):
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            final = await self.run_pipeline(mode)
            self.assertTrue(final['finalization']['answer_verified'], final)
            self.assertIn(self.first, final['answer'])
            self.assertIn(self.second, final['answer'])
            self.assertEqual(final['finalization']['coverage_recovery']['status'], 'accepted')
            self.assertTrue(restore_question_coverage(final)['complete'])
            if mode == 'timeline':
                from app.timeline import restore_timeline
                self.assertIn(restore_timeline(final)[1]['status'], {'ready', 'no_dates'})
            self.assertEqual(self.calls, ['source_reader', 'fact_selector', 'source_auditor',
                'answer_coverage', 'answer_completion', 'source_auditor', 'answer_coverage'])
            audits = [p for n, p in self.payloads if n == 'source_auditor']
            self.assertEqual(audits[0]['units'][0]['text'], audits[1]['units'][0]['text'])
            self.assertEqual(audits[0]['source_documents'], audits[1]['source_documents'])
            self.assertEqual(len(audits[1]['units']), 2)

    async def test_complete_answer_does_not_start_completion(self):
        self.behavior = 'complete'
        final = await self.run_pipeline()
        self.assertTrue(restore_question_coverage(final)['complete'])
        self.assertNotIn('answer_completion', self.calls)

    async def test_unusable_extensions_preserve_original_and_original_receipt(self):
        for behavior in ('none', 'malformed', 'duplicate', 'foreign', 'invented', 'unrelated', 'coverage_unavailable'):
            with self.subTest(behavior=behavior):
                self.behavior = behavior
                final = await self.run_pipeline()
                self.assertTrue(final['finalization']['answer_verified'])
                self.assertEqual(final['answer'], '- ' + self.second + ' [Document 17](/documents/17)')
                self.assertFalse(restore_question_coverage(final)['complete'])
                self.assertEqual(self.calls.count('answer_completion'), 1)
                self.assertNotIn('answer_editor', self.calls)
                self.assertLessEqual(self.calls.count('source_auditor'), 2)

    async def test_newer_rejection_of_original_never_restores_prior_approval(self):
        self.behavior = 'revoked'
        final = await self.run_pipeline()
        self.assertFalse(final['finalization']['answer_verified'])
        self.assertEqual(final['finalization']['pipeline_failure'], 'answer_completion')
        self.assertEqual(final['finalization']['coverage_recovery']['status'], 'prior_support_revoked')
        self.assertNotIn('$270', final['answer'])
        self.assertEqual(self.calls.count('source_auditor'), 2)

    async def test_completed_prior_rejection_survives_a_sibling_audit_timeout(self):
        from app.config import settings
        self.behavior = 'revoked'
        base = self.model
        prior = [self.second, 'The record labels the account blue.',
                 'The record labels the project green.', 'The record labels the division red.']
        source = self.first + '\n' + '\n'.join(prior)
        self.pack['items'][0].update(content=source, source_content=source)
        negatives = []

        async def two_batches(**kwargs):
            response = await base(**kwargs)
            name, payload = kwargs['name'], json.loads(kwargs['prompt'])
            if name == 'source_reader':
                raw = json.loads(response)
                span = payload['source_documents'][0]['windows'][0]['span']['span_id']
                raw['documents'][0]['observations'] = [
                    {'text': text, 'references': [{'span_id': span}]} for text in prior]
                return json.dumps(raw)
            if name == 'source_auditor' and 'answer_completion' in self.calls:
                if payload['units'][0]['id'] == 'u5':
                    await asyncio.Event().wait()
                negatives.extend(a for a in json.loads(response)['assessments'] if a['status'] == 'unsupported')
            return response

        self.model = two_batches
        with patch.object(settings, 'answer_audit_timeout_seconds', 0.03):
            final = await self.run_pipeline()
        self.assertEqual(len(negatives), 1)
        self.assertFalse(final['finalization']['answer_verified'])
        self.assertEqual(final['finalization']['coverage_recovery']['status'], 'prior_support_revoked')
        self.assertNotIn(self.second, final['answer'])
        self.assertEqual(self.calls.count('source_auditor'), 3)

    async def test_cancellation_is_not_a_fallback_answer(self):
        self.behavior = 'cancel'
        with self.assertRaises(asyncio.CancelledError): await self.run_pipeline()

    async def test_repaired_or_subset_initial_answers_do_not_trigger_completion(self):
        from app.answer_finalization import AnswerFinalizer
        original = AnswerFinalizer.finalize
        for field, value in (('attempts', 2), ('disposition', 'partial')):
            async def ineligible(instance, *args, **kwargs):
                final = await original(instance, *args, **kwargs)
                final['finalization'][field] = value
                return final
            with patch.object(AnswerFinalizer, 'finalize', ineligible):
                await self.run_pipeline()
            self.assertNotIn('answer_completion', self.calls)

    async def test_cross_domain_capacity_reader_gap_uses_the_same_contract(self):
        self.first = 'The equipment record lists an initial capacity of 300 kW.'
        self.second = 'The equipment record lists an approved capacity of 480 kW.'
        self.question = 'What initial and approved equipment capacities are documented?'
        self.plan.update(resolved_question=self.question, original_question=self.question)
        self.plan['requirements'][0]['aspect'] = 'Initial capacity'
        self.plan['requirements'][1]['aspect'] = 'Approved capacity'
        self.pack['items'][0].update(content=self.first + '\n' + self.second,
                                     source_content=self.first + '\n' + self.second)
        final = await self.run_pipeline()
        self.assertTrue(restore_question_coverage(final)['complete'])
        self.assertIn('300 kW', final['answer'])
        self.assertIn('480 kW', final['answer'])

    async def test_real_http_and_sse_routes_conserve_recovered_answer_and_receipt(self):
        import httpx
        from types import SimpleNamespace
        from app import main
        async def query(question, **kwargs):
            return await self.run_pipeline(kwargs['mode'])
        async def stream(question, **kwargs):
            yield {'type': 'complete', **await query(question, **kwargs)}
        engine = SimpleNamespace(query=query, query_stream=stream)
        with patch.object(main, 'query_engine', engine):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                         base_url='http://synthetic.local') as client:
                ordinary = await client.post('/query', json={'question': self.question, 'mode': 'strict'})
                streamed = await client.post('/query/stream', json={'question': self.question, 'mode': 'strict'})
        self.assertEqual(ordinary.status_code, 200)
        self.assertEqual(streamed.status_code, 200)
        event = next(json.loads(line[6:]) for line in streamed.text.splitlines()
                     if line.startswith('data: ') and json.loads(line[6:]).get('type') == 'complete')
        self.assertEqual(ordinary.json()['answer'], event['answer'])
        self.assertEqual(ordinary.json()['finalization']['question_coverage'],
                         event['finalization']['question_coverage'])
        self.assertTrue(restore_question_coverage(event)['complete'])
