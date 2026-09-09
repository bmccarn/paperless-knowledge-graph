"""Decision consistency cannot replace original-source acceptance."""
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import source_audit
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_finalization import AnswerFinalizer, evidence_spans
from tests.source_audit_fixtures import decision


TEXT = 'Vendor Example completed a refund of $125 on July 2, 2024. The receipt was signed July 3, 2024.'
PACK = {'items': [{'id': 'receipt', 'document_id': 17, 'chunk_index': 0,
                  'source_kind': 'ocr', 'content': TEXT, 'source_content': TEXT}]}
CLAIM = 'Vendor Example completed a refund of $125 on July 2, 2024.'
PRIVATE_BASIS = 'PRIVATE MODEL ASSESSMENT: the original describes a request only.'


class SourceAuditContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.auditor = StrandsQueryOrchestrator()
        self.auditor.enabled = True
        self.span = evidence_spans(PACK, citation_safe=True)[0]

    def row(self, **fields):
        return decision(**{'references': [{'span_id': self.span['span_id']}],
                           'temporal_scope': 'historical', 'temporal_assertion': 'source_observation', **fields})

    def parse(self, row):
        return source_audit.parse_decisions(json.dumps({'assessments': [row]}), ['u1'])['assessments'][0]

    async def run_finalizer(self, row, *, claim=CLAIM, repairer=None):
        with patch.object(self.auditor, '_text_agent', AsyncMock(return_value=json.dumps({'assessments': [row]}))) as calls:
            result = await AnswerFinalizer(self.auditor, repairer).finalize('What does the record establish?', claim, PACK)
        return result, calls.await_count

    async def test_each_negative_facet_is_rejected_without_protocol_vote(self):
        for facet in source_audit.FACETS:
            for value in ('not_established', 'contradicted'):
                row = self.row(source_basis=PRIVATE_BASIS)
                row['checks'][facet] = value
                result, count = await self.run_finalizer(row)
                self.assertEqual(count, 1, (facet, value))
                self.assertFalse(result['finalization']['answer_verified'])
                claim = result['claim_ledger']['claims'][0]
                self.assertEqual(claim['model_status'], 'supported')
                self.assertEqual(claim['status'], 'unsupported')
                self.assertIn('semantic_' + facet, claim['rejection_reasons'])
                self.assertNotIn(PRIVATE_BASIS, json.dumps(result))
                self.assertEqual(result['claim_ledger']['audit_batches'][0]['status'], 'valid')

    async def test_assumptions_reach_editor_but_never_become_public_evidence(self):
        row = self.row(source_basis=PRIVATE_BASIS, unresolved_assumptions=['PRIVATE assumed completion'])
        repairer = type('Repairer', (), {'repair_answer': AsyncMock(return_value={'observations': [CLAIM]})})()
        result, count = await self.run_finalizer(row, repairer=repairer)
        self.assertEqual(count, 2)  # original and changed atomic candidate; no protocol retry
        self.assertFalse(result['finalization']['answer_verified'])
        findings = repairer.repair_answer.await_args.args[3]['claims'][0]
        self.assertEqual(findings['semantic_decision']['source_basis'], PRIVATE_BASIS)
        self.assertIn('semantic_assumptions', findings['rejection_reasons'])
        self.assertNotIn('PRIVATE', json.dumps(result))

    def test_negative_model_verdicts_remain_unchanged(self):
        for verdict in ('unsupported', 'missing', 'conflicting'):
            row = self.row(status=verdict, unresolved_assumptions=['A fact is not established.'])
            parsed = self.parse(row)
            self.assertEqual(parsed['status'], verdict)
            self.assertEqual(parsed['model_status'], verdict)

    async def test_typed_temporal_applicability_cannot_be_bypassed(self):
        for metadata, facet in (({}, 'temporal'),
                ({'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                  'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [17]}, 'comparison')):
            row = self.row()
            row.update(metadata)
            row['checks'][facet] = 'not_applicable'
            result, count = await self.run_finalizer(row)
            self.assertEqual(count, 1)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertIn('semantic_' + facet, result['claim_ledger']['claims'][0]['rejection_reasons'])

    async def test_positive_facets_cannot_override_original_source_value_or_current_gates(self):
        for change, claim in (({'references': [{'span_id': 'invented'}]}, CLAIM),
                              ({}, CLAIM.replace('$125', '$250')),
                              ({}, CLAIM.replace('2024', '2025')),
                              ({'temporal_scope': 'current', 'temporal_assertion': 'present_world'},
                               'The refund is currently complete.')):
            row = self.row()
            row.update(change)
            result, count = await self.run_finalizer(row, claim=claim)
            self.assertFalse(result['finalization']['answer_verified'], (change, claim))
            self.assertEqual(count, 1)

    async def test_completed_and_existing_state_observations_remain_eligible(self):
        for text in (CLAIM, 'The equipment record states current capacity of 400 units and selects a custodian update.'):
            pack = {'items': [{**PACK['items'][0], 'content': text, 'source_content': text}]}
            span = evidence_spans(pack, citation_safe=True)[0]
            row = self.row(references=[{'span_id': span['span_id']}])
            row['checks']['conditions'] = 'supported'
            with patch.object(self.auditor, '_text_agent', AsyncMock(return_value=json.dumps({'assessments': [row]}))):
                result = await AnswerFinalizer(self.auditor).finalize('What is recorded?', text, pack)
            self.assertTrue(result['finalization']['answer_verified'])

    def test_malformed_decisions_reject_unknown_missing_and_ambiguous_fields(self):
        cases = []
        for key in self.row():
            row = self.row(); del row[key]
            cases.append(row)
        for facet in source_audit.FACETS:
            row = self.row(); row['checks'][facet] = 'PRIVATE unknown'
            cases.append(row)
        for facet in source_audit.CORE_FACETS:
            row = self.row(); row['checks'][facet] = 'not_applicable'
            cases.append(row)
        for field, value in (('source_basis', ''), ('source_basis', 'x' * 1201),
                             ('checks', {'subject': 'supported'}), ('unresolved_assumptions', ['x'] * 7),
                             ('unresolved_assumptions', [False]), ('references', [{'span_id': 's', 'quote': 'PRIVATE'}]),
                             ('comparison_document_ids', [True]), ('status', [])):
            row = self.row(); row[field] = value
            cases.append(row)
        for row in cases:
            with self.assertRaises(source_audit.SourceAuditProtocolError) as error:
                self.parse(row)
            self.assertNotIn('PRIVATE', str(error.exception))
        raw = json.dumps({'assessments': [self.row()]})
        duplicate = raw.replace('"predicate": "supported"', '"predicate": "contradicted", "predicate": "supported"')
        for text in ('Prefix ' + raw, raw + ' trailing', '```json\n' + raw + '\n```', duplicate):
            with self.assertRaises(source_audit.SourceAuditProtocolError):
                source_audit.parse_decisions(text, ['u1'])

    async def test_persistent_protocol_failure_cannot_use_editor_to_gain_success(self):
        bad = self.row(); del bad['checks']['predicate']
        repairer = type('Repairer', (), {'repair_answer': AsyncMock(return_value={'observations': [CLAIM]})})()
        result, count = await self.run_finalizer(bad, repairer=repairer)
        self.assertEqual(count, 2)
        self.assertEqual(result['finalization']['disposition'], 'incomplete')
        self.assertFalse(result['finalization']['answer_verified'])
        repairer.repair_answer.assert_not_awaited()
        self.assertEqual(result['claim_ledger']['audit_batches'][0]['final_errors'], ['semantic_protocol_invalid_checks'])

    async def test_subset_receives_fresh_decisions_and_strips_failed_basis(self):
        first = self.row(source_basis=PRIVATE_BASIS)
        second = self.row(unit_id='u2', status='unsupported', source_basis=PRIVATE_BASIS)
        subset = self.row(status='conflicting', source_basis=PRIVATE_BASIS)
        replies = [json.dumps({'assessments': [first, second]}), json.dumps({'assessments': [subset]})]
        with patch.object(self.auditor, '_text_agent', AsyncMock(side_effect=replies)) as calls:
            result = await AnswerFinalizer(self.auditor).finalize('What happened?', CLAIM + '\n\nAn unsupported event occurred.', PACK)
        self.assertEqual(calls.await_count, 2)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['subset_audit']['claims'][0]['status'], 'conflicting')
        self.assertNotIn('PRIVATE', json.dumps(result))
        payload = json.loads(calls.await_args_list[1].kwargs['prompt'])
        self.assertNotIn('semantic_decision', json.dumps(payload))

    async def test_model_cannot_select_a_legacy_protocol(self):
        raw = {'assessments': [self.row()], 'protocol': 'legacy'}
        with patch.object(self.auditor, '_text_agent', AsyncMock(return_value=json.dumps(raw))):
            result = await AnswerFinalizer(self.auditor).finalize('What happened?', CLAIM, PACK)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['summary']['audited'], 0)
