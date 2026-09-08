"""A malformed worker envelope can recover once without relaxing source audit."""
import asyncio
import copy
import unittest
from app.answer_finalization import AnswerFinalizer
from tests.test_source_dates import pack, ExactAuditor

ANSWER = 'The invoice records a $321 USD charge.'


class ProtocolAuditor(ExactAuditor):
    def __init__(self, first, second=None):
        self.first, self.second, self.inputs = first, second, []
        super().__init__()

    async def audit_answer_units(self, question, units, spans, plan):
        self.inputs.append(copy.deepcopy((units, spans, plan)))
        valid = await super().audit_answer_units(question, units, spans, plan)
        if len(self.inputs) == 1:
            return self.first(valid)
        return self.second(valid) if self.second else valid


class AuditProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_correction_reuses_exact_units_and_sources(self):
        changes = [lambda v: {**v, 'assessments': [{**v['assessments'][0], 'unit_id': 'foreign private instruction'}]},
                   lambda v: {**v, 'assessments': v['assessments'] * 2},
                   lambda v: {'assessments': []},
                   lambda v: {'assessments': 'malformed private source'},
                   lambda v: {**v, 'assessments': [{**v['assessments'][0], 'status': 'unchecked'}]}]
        for change in changes:
            with self.subTest(change=change):
                auditor = ProtocolAuditor(change)
                result = await AnswerFinalizer(auditor).finalize('What charge?', ANSWER, pack(ANSWER))
                self.assertTrue(result['finalization']['complete'])
                self.assertEqual(len(auditor.inputs), 2)
                self.assertEqual(auditor.inputs[0][:2], auditor.inputs[1][:2])
                correction = auditor.inputs[1][2]['audit_protocol_recovery']
                self.assertEqual(correction['expected_unit_ids'], ['u1'])
                self.assertNotIn('private', str(correction))
                protocol = result['claim_ledger']['audit_batches'][0]
                self.assertEqual(protocol['attempts'], 2)
                self.assertEqual(protocol['status'], 'corrected')
                self.assertNotIn('private', str(protocol))

    async def test_semantic_verdicts_and_unavailable_results_are_not_retried(self):
        for status in ('unsupported', 'missing', 'conflicting'):
            auditor = ProtocolAuditor(lambda v: {'assessments': [{**v['assessments'][0], 'status': status, 'references': []}]})
            result = await AnswerFinalizer(auditor).finalize('What charge?', ANSWER, pack(ANSWER))
            self.assertEqual(len(auditor.inputs), 1)
            self.assertFalse(result['finalization']['complete'])
        for empty in ({}, None):
            auditor = ProtocolAuditor(lambda v: empty)
            result = await AnswerFinalizer(auditor).finalize('What charge?', ANSWER, pack(ANSWER))
            self.assertEqual(len(auditor.inputs), 1)
            self.assertFalse(result['finalization']['answer_verified'])

    async def test_failed_correction_does_not_admit_earlier_supported_units(self):
        def invalid(valid):
            return {'assessments': valid['assessments'] + [{'unit_id': 'foreign', 'status': 'supported', 'references': []}]}
        auditor = ProtocolAuditor(invalid, invalid)
        result = await AnswerFinalizer(auditor).finalize('What charge?', ANSWER, pack(ANSWER))
        self.assertEqual(len(auditor.inputs), 2)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['summary']['audited'], 0)
        self.assertNotIn('partial', result['verification'])

    async def test_unavailable_correction_never_publishes_initial_supported_assessment(self):
        for failure in (None, {}, RuntimeError("unavailable"), TimeoutError()):
            class Auditor(ExactAuditor):
                async def audit_answer_units(self, *args):
                    valid = await super().audit_answer_units(*args)
                    if self.calls == 1:
                        valid["assessments"].append({"unit_id": "foreign", "status": "supported", "references": []})
                        return valid
                    if isinstance(failure, Exception):
                        raise failure
                    return failure
            auditor = Auditor()
            result = await AnswerFinalizer(auditor).finalize('What charge?', ANSWER, pack(ANSWER))
            self.assertEqual(auditor.calls, 2)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertNotIn('321', result['answer'])
            self.assertNotIn('partial', result['verification'])

    async def test_protocol_correction_shares_the_original_audit_deadline(self):
        cancelled = asyncio.Event()
        class Auditor(ExactAuditor):
            async def audit_answer_units(self, *args):
                valid = await super().audit_answer_units(*args)
                if self.calls == 1:
                    return {"assessments": []}
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
        auditor = Auditor()
        result = await AnswerFinalizer(auditor, timeout_seconds=0.02).finalize('What charge?', ANSWER, pack(ANSWER))
        self.assertEqual(auditor.calls, 2)
        self.assertTrue(cancelled.is_set())
        self.assertEqual(result['finalization']['disposition'], 'timeout')
        self.assertFalse(result['finalization']['answer_verified'])
