"""A completed absence verdict can be omitted only after fresh subset validation."""
import unittest
from app.answer_finalization import AnswerFinalizer
from tests.test_partial_answers import MixedAuditor, SOURCE
from tests.test_source_dates import pack


class MissingAuditor(MixedAuditor):
    async def audit_answer_units(self, *args):
        result = await super().audit_answer_units(*args)
        for assessment in result['assessments']:
            if assessment['status'] == 'unsupported':
                assessment.update(status='missing', references=[])
        return result


class AuditedMissingSubsetTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_units_are_omitted_and_delivered_subset_is_reaudited(self):
        for supported, missing in [('The invoice records a $321 USD service charge.', 'An extra charge is $999 USD.'),
                                   ('The laboratory records a 5 mg dose.', 'The previous dose was 90 mg.')]:
            auditor = MissingAuditor()
            result = await AnswerFinalizer(auditor).finalize('What is recorded?', supported+'\n'+missing, pack(SOURCE))
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertTrue(result['finalization']['answer_verified'])
            self.assertNotIn(missing, result['answer'])
            self.assertEqual(auditor.contexts, [supported+'\n'+missing, supported])
            self.assertEqual(result['verification']['partial']['omitted_units'][0]['status'], 'missing')
            self.assertEqual(result['claim_ledger']['summary']['supported'], 1)

    async def test_missing_subset_failure_and_conflict_cannot_publish(self):
        answer = 'The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.'
        for auditor in [MissingAuditor(fail_subset=True), MissingAuditor(conflict=True)]:
            result = await AnswerFinalizer(auditor).finalize('What is recorded?', answer, pack(SOURCE))
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertNotEqual(result['finalization']['disposition'], 'partial')

    async def test_missing_verdict_does_not_hide_incomplete_protocol(self):
        class Incomplete(MissingAuditor):
            async def audit_answer_units(self, *args):
                result = await super().audit_answer_units(*args)
                result['assessments'] = result['assessments'][:1]
                return result
        auditor = Incomplete()
        result = await AnswerFinalizer(auditor).finalize('What is recorded?',
            'The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.', pack(SOURCE))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['finalization']['disposition'], 'incomplete')
