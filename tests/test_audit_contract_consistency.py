"""Cross-field audit protocol recovery through production boundaries, no live models."""
import asyncio
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import source_audit
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.answer_observations import ObservationCandidate
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision
from tests.test_fact_pipeline_integration import item, plan

ORIGINALS = {
    101: 'Site ALPHA. Collected March 1, 2024. Concentration: 10 mg/L.',
    102: 'Site ALPHA. Collected April 1, 2024. Concentration: 6 mg/L.',
}
PACK = {'items': [item(k, text) for k, text in ORIGINALS.items()]}
OBSERVATION = ('For site ALPHA, the recorded concentration changed from 10 mg/L on '
               'March 1, 2024 to 6 mg/L on April 1, 2024.')


class AuditContractConsistencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.auditor = StrandsQueryOrchestrator(); self.auditor.enabled = True
        self.addAsyncCleanup(self.auditor.close)
        self.refs = [{'span_id': s['span_id']} for s in evidence_spans(PACK, citation_safe=True)]
        self.valid = decision(references=self.refs, temporal_scope='documented',
                              temporal_assertion='retrieved_comparison',
                              comparison_scope='retrieved_documents', comparison_document_ids=[101, 102])
        self.valid['checks']['comparison'] = 'supported'
        self.wrong = {**self.valid, 'temporal_scope': 'historical'}

    async def run_rows(self, rows, *, mode='strict', text=OBSERVATION):
        replies = [json.dumps({'assessments': [row]}) for row in rows]
        with patch.object(self.auditor, '_text_agent', AsyncMock(side_effect=replies)) as calls:
            result = await AnswerFinalizer(self.auditor, allow_subset=False).finalize(
                'How did the recorded concentration change?',
                ObservationCandidate.from_response({'observations': [text]}), PACK,
                mode=mode, evaluated_at='2026-09-09')
        return result, [json.loads(c.kwargs['prompt']) for c in calls.await_args_list]

    async def test_historical_comparison_uses_one_same_input_correction_in_every_mode(self):
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode):
                result, payloads = await self.run_rows([self.wrong, self.valid], mode=mode)
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(len(payloads), 2)
                before, after = copy.deepcopy(payloads)
                before.pop('protocol_correction'); correction = after.pop('protocol_correction')
                self.assertEqual(before, after)
                self.assertIn('inconsistent_scope_checks', json.dumps(correction))
                self.assertEqual(result['claim_ledger']['audit_batches'][0]['attempts'], 2)
                if mode == 'timeline':
                    from app.timeline import restore_timeline
                    self.assertIsNotNone(restore_timeline(result))

    async def test_all_cross_field_contradictions_use_the_same_correction_boundary(self):
        variants = [
            {'temporal_scope': 'current'}, {'temporal_scope': 'none'},
            {'temporal_assertion': 'source_observation'}, {'temporal_assertion': 'present_world'},
            {'comparison_scope': None}, {'comparison_document_ids': []},
        ]
        for fields in variants:
            with self.subTest(fields=fields):
                result, payloads = await self.run_rows([{**self.valid, **fields}, self.valid])
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(len(payloads), 2)

    async def test_metadata_cannot_auto_accept_or_retry_a_second_failure(self):
        parsed = source_audit.parse_decisions(json.dumps({'assessments': [self.wrong]}), ['u1'])
        self.assertEqual(parsed['assessments'][0]['status'], 'unsupported')
        for second in (self.wrong, {**self.valid, 'status': 'unsupported'}):
            result, payloads = await self.run_rows([self.wrong, second])
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(len(payloads), 2)

    async def test_every_negative_semantic_signal_suppresses_metadata_correction(self):
        rows = []
        for facet in source_audit.FACETS:
            for status in ('not_established', 'contradicted'):
                row = copy.deepcopy(self.wrong); row['checks'][facet] = status; rows.append(row)
        for status in ('unsupported', 'missing', 'conflicting'):
            rows.append({**self.wrong, 'status': status})
        rows.append({**self.wrong, 'unresolved_assumptions': ['An action not established by the sources.']})
        for row in rows:
            with self.subTest(row=row):
                result, payloads = await self.run_rows([row])
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertEqual(len(payloads), 1)

    async def test_correction_preserves_deadline_and_cancellation_owner(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                entered = asyncio.Event()
                stopped = asyncio.Event()
                calls = 0
                async def native(**kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        return json.dumps({'assessments': [self.wrong]})
                    entered.set()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        stopped.set()
                with patch.object(self.auditor, '_text_agent', side_effect=native):
                    task = asyncio.create_task(AnswerFinalizer(
                        self.auditor, allow_subset=False, timeout_seconds=.1).finalize(
                            'What changed?', ObservationCandidate.from_response(
                                {'observations': [OBSERVATION]}), PACK, evaluated_at='2026-09-09'))
                    await asyncio.wait_for(entered.wait(), 1)
                    if cancel:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        result = await task
                        self.assertEqual(result['finalization']['disposition'], 'timeout')
                        self.assertFalse(result['finalization']['answer_verified'])
                    self.assertTrue(stopped.is_set())
                    self.assertEqual(calls, 2)

    async def test_negative_sibling_prevents_whole_batch_reroll(self):
        negative = {**self.valid, 'unit_id': 'u2', 'status': 'unsupported'}
        native = AsyncMock(return_value=json.dumps({'assessments': [self.wrong, negative]}))
        with patch.object(self.auditor, '_text_agent', native):
            result = await AnswerFinalizer(self.auditor, allow_subset=False).finalize(
                'What changed?', ObservationCandidate.from_response({'observations': [OBSERVATION, OBSERVATION]}),
                PACK, evaluated_at='2026-09-09')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(native.await_count, 1)

    async def test_mixed_protocol_errors_share_one_correction_budget(self):
        unknown = {**self.valid, 'references': [{'span_id': 'foreign-handle'}]}
        for rows in ((unknown, self.wrong), (self.wrong, unknown), (self.wrong, self.wrong)):
            result, payloads = await self.run_rows(rows)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(len(payloads), 2)
            self.assertEqual(result['claim_ledger']['audit_batches'][0]['attempts'], 2)

    async def test_consistent_metadata_cannot_override_values_or_source_membership(self):
        result, payloads = await self.run_rows([self.wrong, self.valid], text=OBSERVATION.replace('6 mg/L', '600 mg/L'))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)
        self.assertIn('value_mismatch', result['claim_ledger']['claims'][0]['rejection_reasons'])
        for ids in ([999], [101]):
            result, payloads = await self.run_rows([self.wrong, {**self.valid, 'comparison_document_ids': ids}])
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(len(payloads), 2)
            self.assertIn('invalid_comparison_scope', result['claim_ledger']['claims'][0]['rejection_reasons'])

    async def test_direct_auditor_cannot_bypass_the_finalizer_defense(self):
        legacy = type('Direct', (), {'audit_answer_units': AsyncMock(return_value={
            'assessments': [self.wrong]})})()
        result = await AnswerFinalizer(legacy).finalize('What changed?',
            ObservationCandidate.from_response({'observations': [OBSERVATION]}), PACK, evaluated_at='2026-09-09')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertIn('invalid_temporal_assertion', result['claim_ledger']['claims'][0]['rejection_reasons'])
        self.assertEqual(legacy.audit_answer_units.await_count, 1)

    async def test_legacy_optional_framing_remains_optional(self):
        from app.answer_finalization import evidence_spans
        text = 'Site ALPHA records a concentration of 10 mg/L.'
        ref = {**evidence_spans(PACK, citation_safe=True)[0], 'quote': ORIGINALS[101]}
        legacy = type('Direct', (), {'audit_answer_units': AsyncMock(return_value={
            'assessments': [{'unit_id': 'u1', 'status': 'supported', 'temporal_scope': 'historical', 'references': [ref]}]})})()
        result = await AnswerFinalizer(legacy).finalize('What does the record report?', text, PACK,
                                                      evaluated_at='2026-09-09')
        self.assertTrue(result['finalization']['answer_verified'])

    async def test_editor_then_metadata_correction_restores_complete_question_meaning(self):
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode):
                await self.pipeline_control(mode)

    async def test_editor_cannot_reset_mixed_error_budget(self):
        await self.pipeline_control('strict', mixed_failure=True)

    async def pipeline_control(self, mode, *, mixed_failure=False):
        from app.question_pipeline import finalize_question
        from app.answer_coverage import restore_question_coverage, restore_pipeline_metadata
        question = 'How did the recorded concentration change?'
        p = plan(question, ['Recorded concentration change'])
        p['requirements'][0].update(temporal_scope='historical', comparison_scope='retrieved_documents')
        texts = {101: 'Site ALPHA records 10 mg/L on March 1, 2024.',
                 102: 'Site ALPHA records 6 mg/L on April 1, 2024.'}
        ancillary = 'Treatment permanently cured site ALPHA.'
        calls, correction_payloads = [], []
        async def native(name, system_prompt, prompt, **kwargs):
            calls.append(name)
            if name == 'answer_editor':
                return json.dumps({'observations': [OBSERVATION]})
            x = json.loads(prompt)
            if name == 'source_reader':
                doc = x['source_documents'][0]; handle = doc['windows'][0]['span']['span_id']
                values = [texts[doc['document_id']]] + ([ancillary] if doc['document_id'] == 102 else [])
                return json.dumps({'documents': [{'document_id': doc['document_id'], 'observations': [
                    {'text': text, 'references': [{'span_id': handle}]} for text in values], 'limitations': []}]})
            if name == 'source_auditor':
                handles = {d['document_id']: d['windows'][0]['span']['span_id'] for d in x['source_documents']}
                if len(x['units']) == 1:
                    correction_payloads.append(x)
                    row = copy.deepcopy(self.valid if x['protocol_correction'] else self.wrong)
                    if mixed_failure and not x['protocol_correction']:
                        row['references'] = [{'span_id': 'foreign-handle'}]
                    elif mixed_failure:
                        row = copy.deepcopy(self.wrong)
                    return json.dumps({'assessments': [row]})
                rows = []
                for unit in x['units']:
                    bad = unit['text'] == '- ' + ancillary
                    owner = 101 if unit['text'] == '- ' + texts[101] else 102
                    rows.append(decision(unit_id=unit['id'], status='unsupported' if bad else 'supported',
                        references=[{'span_id': handles[owner]}], temporal_scope='historical',
                        temporal_assertion='source_observation'))
                return json.dumps({'assessments': rows})
            if name == 'answer_coverage':
                return json.dumps({'requirements': [{'requirement_id': 'r1', 'status': 'answered',
                    'observation_ids': ['u1']}], 'omitted_requested_aspects': False})
            self.fail('Unexpected stage ' + name)
        with patch.object(self.auditor, '_text_agent', side_effect=native):
            final = await finalize_question(self.auditor, question, PACK, p, mode)
        final.update(question=question, query_plan=p, mode=mode)
        self.assertEqual(len(correction_payloads), 2)
        before, after = copy.deepcopy(correction_payloads)
        before.pop('protocol_correction'); after.pop('protocol_correction')
        self.assertEqual(before, after)
        self.assertEqual(calls.count('answer_editor'), 1)
        self.assertEqual(calls.count('source_auditor'), 3)
        self.assertEqual(restore_pipeline_metadata(final, final['answer']), final)
        if mixed_failure:
            self.assertFalse(final['finalization']['answer_verified'])
            self.assertNotIn('answer_coverage', calls)
        else:
            self.assertTrue(final['finalization']['answer_verified'])
            self.assertIn(OBSERVATION, final['answer'])
            coverage = restore_question_coverage(final)
            self.assertIsNotNone(coverage)
            self.assertEqual(coverage['requirements'][0]['status'], 'answered')
            self.assertFalse(coverage['complete'])  # Editing cannot restore exact original inventory.
