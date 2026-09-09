"""Native protocol correction preserves original evidence and semantic rejection."""
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app import source_audit
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision


class ProtocolCorrectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.text = 'The equipment register dated March 20, 2024 records a capacity of 480 units.'
        self.pack = {'items': [{'id': 'register', 'document_id': 1, 'chunk_index': 0,
            'source_kind': 'ocr', 'content': self.text, 'source_content': self.text}]}
        self.handle = evidence_spans(self.pack, citation_safe=True)[0]['span_id']
        self.valid = decision(references=[{'span_id': self.handle}], temporal_scope='historical',
                              temporal_assertion='source_observation')
        self.auditor = StrandsQueryOrchestrator()
        self.auditor.enabled = True

    async def run_rows(self, rows):
        replies = [json.dumps({'assessments': [row]}) for row in rows]
        with patch.object(self.auditor, '_text_agent', AsyncMock(side_effect=replies)) as calls:
            result = await AnswerFinalizer(self.auditor).finalize(
                'What does the register record?', self.text, self.pack, evaluated_at='2026-09-09')
        return result, [json.loads(call.kwargs['prompt']) for call in calls.await_args_list]

    async def test_unknown_handle_recovers_once_with_same_units_and_sources(self):
        wrong = {**self.valid, 'references': [{'span_id': 'PRIVATE-FOREIGN-HANDLE'}]}
        result, payloads = await self.run_rows([wrong, self.valid])
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]['units'], payloads[1]['units'])
        self.assertEqual(payloads[0]['source_spans'], payloads[1]['source_spans'])
        self.assertNotIn('PRIVATE-FOREIGN-HANDLE', json.dumps(payloads[1]))
        self.assertIn('unknown_source_handle', json.dumps(payloads[1]['protocol_correction']))

    async def test_repeated_unknown_handle_cannot_certify_or_retry_forever(self):
        wrong = {**self.valid, 'references': [{'span_id': 'unknown'}]}
        result, payloads = await self.run_rows([wrong, wrong])
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)

    async def test_corrected_reference_does_not_override_semantic_contradiction(self):
        wrong = {**self.valid, 'references': [{'span_id': 'unknown'}]}
        contradicted = copy.deepcopy(self.valid)
        contradicted['checks']['predicate'] = 'contradicted'
        result, payloads = await self.run_rows([wrong, contradicted])
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)
        self.assertIn('semantic_predicate', result['claim_ledger']['claims'][0]['rejection_reasons'])

    async def test_historical_observation_drops_unused_comparison_list(self):
        unused = {**self.valid, 'comparison_document_ids': [1, 987]}
        result, payloads = await self.run_rows([unused])
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 1)
        parsed = source_audit.parse_decisions(json.dumps({'assessments': [unused]}), ['u1'])
        self.assertEqual(parsed['assessments'][0]['comparison_document_ids'], [])

    async def test_scope_check_inconsistency_uses_existing_correction(self):
        wrong = copy.deepcopy(self.valid)
        wrong['checks']['temporal'] = 'not_applicable'
        result, payloads = await self.run_rows([wrong, self.valid])
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]['units'], payloads[1]['units'])
        self.assertEqual(payloads[0]['source_spans'], payloads[1]['source_spans'])
        self.assertIn('inconsistent_scope_checks', json.dumps(payloads[1]['protocol_correction']))

    async def test_scope_correction_can_preserve_an_undated_source_observation(self):
        self.text = 'The form selects a change of custodian to Avery Sample.'
        self.pack['items'][0].update(content=self.text, source_content=self.text)
        handle = evidence_spans(self.pack, citation_safe=True)[0]['span_id']
        wrong = decision(references=[{'span_id': handle}], temporal_scope='historical',
                         temporal_assertion='source_observation')
        wrong['checks']['temporal'] = 'not_applicable'
        corrected = {**wrong, 'temporal_scope': 'none', 'temporal_assertion': 'none'}
        result, payloads = await self.run_rows([wrong, corrected])
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(len(payloads), 2)

    async def test_scope_check_cannot_retry_indefinitely_or_override_corrected_rejection(self):
        wrong = copy.deepcopy(self.valid)
        wrong['checks']['temporal'] = 'not_applicable'
        for corrected in (wrong, {**self.valid, 'status': 'unsupported'}):
            result, payloads = await self.run_rows([wrong, corrected])
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(len(payloads), 2)

    async def test_negative_facets_assumptions_and_verdict_suppress_scope_retry(self):
        for negative in ('facet', 'assumptions', 'verdict'):
            wrong = copy.deepcopy(self.valid)
            wrong['checks']['temporal'] = 'not_applicable'
            if negative == 'facet':
                wrong['checks']['predicate'] = 'contradicted'
            elif negative == 'assumptions':
                wrong['unresolved_assumptions'] = ['An unestablished action stage.']
            else:
                wrong['status'] = 'unsupported'
            result, payloads = await self.run_rows([wrong])
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(len(payloads), 1)

    async def test_negative_sibling_suppresses_batch_scope_retry(self):
        wrong = copy.deepcopy(self.valid)
        wrong['checks']['temporal'] = 'not_applicable'
        negative = {**self.valid, 'unit_id': 'u2', 'status': 'unsupported'}
        with patch.object(self.auditor, '_text_agent', AsyncMock(return_value=json.dumps(
                {'assessments': [wrong, negative]}))) as calls:
            result = await AnswerFinalizer(self.auditor).finalize(
                'What capacity is recorded?', self.text + '\n\nThe recorded capacity is 999 units.', self.pack)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(calls.await_count, 1)
        self.assertEqual([c['status'] for c in result['claim_ledger']['claims']], ['unsupported', 'unsupported'])

    def test_empty_allowed_set_is_not_absent_manifest(self):
        response = json.dumps({'assessments': [self.valid]})
        self.assertEqual(source_audit.parse_decisions(response, ['u1'])['assessments'][0]['status'], 'supported')
        with self.assertRaises(source_audit.SourceAuditProtocolError) as caught:
            source_audit.parse_decisions(response, ['u1'], allowed_span_ids=set())
        self.assertEqual(caught.exception.reason, 'unknown_source_handle')

    def test_real_comparison_and_bad_types_remain_rejected(self):
        for changes in ({'comparison_scope': 'retrieved_documents'}, {'temporal_scope': 'documented'},
                        {'temporal_assertion': 'retrieved_comparison'}):
            row = {**self.valid, 'comparison_document_ids': [1], **changes}
            parsed = source_audit.parse_decisions(json.dumps({'assessments': [row]}), ['u1'])['assessments'][0]
            self.assertEqual(parsed['status'], 'unsupported')
            self.assertIn('semantic_comparison', parsed['semantic_decision']['rejection_reasons'])
        for ids in ([True], 'unused', [1.5]):
            with self.assertRaises(source_audit.SourceAuditProtocolError):
                source_audit.parse_decisions(json.dumps({'assessments': [{**self.valid, 'comparison_document_ids': ids}]}), ['u1'])
