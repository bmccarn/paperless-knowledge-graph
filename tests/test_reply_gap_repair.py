"""An audit left incomplete only by invalid replies can still be repaired; an unknown handle rejects only its unit."""
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import source_audit
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision
from tests.test_audited_missing_subset import CHARGE, DOSE, LAB, NAME
from tests.test_protocol_error_partial import EXTRA, HANG, PROTOCOL, ScriptedAuditor
from tests.test_source_dates import pack
from tests.test_partial_answers import SOURCE

CONFLICT = 'An extra charge is $999 USD.'
LINES = [CHARGE, CONFLICT, LAB, NAME, EXTRA]  # Batch one holds the conflicting unit, batch two EXTRA alone.


class Repairer:
    def __init__(self, *lines):
        self.lines, self.findings = list(lines), []

    async def repair_answer(self, question, answer, evidence, verification):
        self.findings.append(verification)
        return {'answer': '\n'.join(self.lines)}


async def finalize(auditor, repairer, answer='\n'.join(LINES), **kwargs):
    return await AnswerFinalizer(auditor, repairer, **kwargs).finalize('What is recorded?', answer, pack(SOURCE))


class ReplyGapRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_batch_and_conflict_elsewhere_still_repair(self):
        auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL]}, conflict=True)
        repairer = Repairer(CHARGE, LAB, NAME)
        result = await finalize(auditor, repairer)
        self.assertEqual(result['finalization']['attempts'], 2)
        [findings] = repairer.findings
        gap = [c for c in findings['claims'] if c['status'] == 'unchecked']
        self.assertEqual([c['claim'] for c in gap], [EXTRA])
        self.assertIn('audit_reply_invalid', gap[0]['rejection_reasons'])
        # Two initial batches, one correction, then one audit of the repaired candidate.
        self.assertEqual(len(auditor.contexts), 4)
        self.assertEqual(auditor.contexts[-1], '\n'.join([CHARGE, LAB, NAME]))
        self.assertEqual(result['finalization']['disposition'], 'supported')
        self.assertTrue(result['claim_ledger']['complete'])
        self.assertNotIn('$999', result['answer'])
        self.assertNotIn(EXTRA, result['answer'])

    async def test_repaired_candidate_follows_its_own_audit(self):
        # The repair keeps the conflicting unit: the final audit withholds.
        result = await finalize(ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL]}, conflict=True),
                                Repairer(CHARGE, CONFLICT, LAB))
        self.assertEqual(result['finalization']['attempts'], 2)
        self.assertEqual(result['finalization']['disposition'], 'unsupported')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('$321', result['answer'])
        # The repair keeps the gap unit and its batch fails again: the #183 partial path applies to the repair.
        auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL, PROTOCOL, PROTOCOL]}, conflict=True)
        result = await finalize(auditor, Repairer(CHARGE, LAB, NAME, DOSE, EXTRA))
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertEqual(result['claim_ledger']['candidate_text'], '\n'.join([CHARGE, LAB, NAME, DOSE]))
        self.assertEqual([u['omission_reason'] for u in result['verification']['partial']['omitted_units']],
                         ['audit_reply_invalid'])

    async def test_execution_failures_withhold_without_repair(self):
        cases = {
            'empty reply': ({EXTRA: [None]}, 'incomplete'),
            'protocol error, then empty correction': ({EXTRA: [PROTOCOL, None]}, 'incomplete'),
            'protocol batch and empty batch': ({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [None]}, 'incomplete'),
            'every batch invalid': ({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [PROTOCOL, PROTOCOL]}, 'incomplete'),
            'raise': ({EXTRA: [RuntimeError('unavailable')]}, 'audit_failed'),
            'timeout': ({EXTRA: [TimeoutError()]}, 'audit_failed'),
            'hang': ({EXTRA: [HANG]}, 'timeout'),
        }
        for name, (scripts, disposition) in cases.items():
            with self.subTest(name):
                repairer = Repairer(CHARGE)
                result = await finalize(ScriptedAuditor(scripts, conflict=True), repairer, timeout_seconds=0.2)
                self.assertEqual(repairer.findings, [])
                self.assertEqual(result['finalization']['attempts'], 1)
                self.assertEqual(result['finalization']['disposition'], disposition)
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertNotIn('$321', result['answer'])


class UnknownHandleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        text = 'The register names Vendor Example. The register lists a pump.'
        self.source = {'items': [{'id': 'register', 'document_id': 1, 'chunk_index': 0,
                                  'source_kind': 'ocr', 'content': text, 'source_content': text}]}
        self.handle = evidence_spans(self.source, citation_safe=True)[0]['span_id']
        self.auditor = StrandsQueryOrchestrator()
        self.auditor.enabled = True

    async def run_audit(self, bad_refs):
        lines = ['The register names Vendor Example.', 'The register lists a pump.']
        prompts = []

        async def reply(**kwargs):
            payload = json.loads(kwargs['prompt'])
            prompts.append(payload)
            # Every audit of u2 cites a handle that was not offered; u1 is valid.
            return json.dumps({'assessments': [
                decision(unit_id=uid, references=bad_refs if uid == 'u2' and len(payload['expected_unit_ids']) == 2
                         else [{'span_id': self.handle}]) for uid in payload['expected_unit_ids']]})
        with patch.object(self.auditor, '_text_agent', side_effect=reply), \
                self.assertLogs('app.strands_orchestrator', 'WARNING') as logs:
            result = await AnswerFinalizer(self.auditor).finalize('What does the register record?', '\n'.join(lines), self.source)
        return result, prompts, lines, logs.output

    async def test_unknown_handle_after_correction_rejects_only_its_unit(self):
        for refs in ([{'span_id': self.handle[:-2]}], [{'span_id': self.handle}, {'span_id': self.handle + ':safe:0:9'}]):
            with self.subTest(refs=refs):
                result, prompts, lines, logs = await self.run_audit(refs)
                # The batch, its correction, then the subset audit of u1.
                self.assertEqual(len(prompts), 3)
                self.assertIn('unknown_source_handle', json.dumps(prompts[1]['protocol_correction']))
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertEqual(result['claim_ledger']['candidate_text'], lines[0])
                [omitted] = result['verification']['partial']['omitted_units']
                self.assertEqual((omitted['claim'], omitted['status']), (lines[1], 'unsupported'))
                self.assertIn('unknown_source_handle', omitted['rejection_reasons'])
                self.assertNotIn(lines[1], result['answer'])
                self.assertTrue(any('unknown_source_handles' in line for line in logs))

    def test_unknown_handle_classes_are_content_free(self):
        allowed = {'aaaa:bbbb:0', 'aaaa:bbbb:1600:safe:4:1798'}
        cases = {'aaaa:bbbb:1600': 'truncated', 'aaaa:bbbb:0:safe:1:9': 'extended',
                 'aaaa:bbbb:3200': 'same_chunk', 'aaab:bbbb:0': 'near_miss', 'unrelated': 'other'}
        for handle, kind in cases.items():
            self.assertEqual(source_audit.unknown_handle_class(handle, allowed), kind)
        misses = []
        row = decision(references=[{'span_id': 'aaaa:bbbb:1600'}])
        response = json.dumps({'assessments': [row]})
        with self.assertRaises(source_audit.SourceAuditProtocolError):
            source_audit.parse_decisions(response, ['u1'], allowed_span_ids=allowed, unknown_handle_misses=misses)
        parsed = source_audit.parse_decisions(response, ['u1'], allowed_span_ids=allowed,
                                              unknown_handle_misses=misses, reject_unknown_handle_units=True)
        self.assertEqual(misses, ['truncated', 'truncated'])
        self.assertEqual(parsed['assessments'][0]['status'], 'unsupported')
        self.assertIn('unknown_source_handle', parsed['assessments'][0]['semantic_decision']['rejection_reasons'])


if __name__ == '__main__':
    unittest.main()
