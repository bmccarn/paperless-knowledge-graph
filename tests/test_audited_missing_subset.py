"""A completed absence verdict can be omitted only after fresh subset validation."""
import unittest
from app.answer_finalization import AnswerFinalizer
from app.answer_observations import ObservationCandidate
from tests.test_partial_answers import MixedAuditor, SOURCE
from tests.test_source_dates import pack


class MissingAuditor(MixedAuditor):
    async def audit_answer_units(self, *args):
        result = await super().audit_answer_units(*args)
        for assessment in result['assessments']:
            if assessment['status'] == 'unsupported':
                assessment.update(status='missing', references=[])
        return result


class RoundAuditor(MixedAuditor):
    """Overrides the status of units holding a marker, per audit round."""
    def __init__(self, *rounds):
        super().__init__()
        self.rounds = rounds

    async def audit_answer_units(self, question, units, spans, plan):
        result = await super().audit_answer_units(question, units, spans, plan)
        overrides = self.rounds[len(self.contexts) - 1] if len(self.contexts) <= len(self.rounds) else {}
        for assessment, unit in zip(result['assessments'], units):
            for marker, status in overrides.items():
                if marker in unit['text']:
                    assessment.update(status=status, references=[] if status == 'missing' else assessment['references'])
        return result


CHARGE, DOSE = 'The invoice records a $321 USD service charge.', 'The laboratory records a 5 mg dose.'
LAB, NAME = 'The invoice comes from the laboratory.', 'The record names a service charge.'


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

    async def test_unavailable_subset_is_reported_as_incomplete_with_its_own_diagnostics(self):
        class Unavailable(MissingAuditor):
            async def audit_answer_units(self, *args):
                if self.contexts:
                    return None
                return await super().audit_answer_units(*args)
        result = await AnswerFinalizer(Unavailable()).finalize('What is recorded?',
            'The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.', pack(SOURCE))
        self.assertEqual(result['finalization']['disposition'], 'incomplete')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('$321', result['answer'])
        audit = result['claim_ledger']['subset_audit']
        self.assertFalse(audit['complete'])
        self.assertEqual(audit['summary']['unchecked'], 1)
        self.assertEqual(audit['audit_batches'][0]['final_errors'], ['unavailable'])
        self.assertIn('partial answer source audit did not complete', result['verification']['missing_evidence'][0])

    async def test_failed_subset_retains_the_complete_sanitized_source_manifest(self):
        from app.answer_finalization import evidence_spans
        evidence = pack(SOURCE)
        evidence['items'].append({**evidence['items'][0], 'id':'unrelated', 'document_id':102,
                                  'content':'An extra charge is $999 USD.'})
        spans = evidence_spans(evidence, citation_safe=True)
        class Unavailable(MissingAuditor):
            async def audit_answer_units(self, *args):
                if self.contexts:
                    return None
                return await super().audit_answer_units(*args)
        result = await AnswerFinalizer(Unavailable()).finalize('What is recorded?',
            'The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.', evidence)
        ledger = result['claim_ledger']
        audit = ledger['subset_audit']
        self.assertEqual(len(ledger['spans']), 2)
        self.assertEqual([s['span_id'] for s in audit['spans']], [s['span_id'] for s in spans])
        self.assertEqual(audit['available_span_count'], 2)
        self.assertEqual(audit['selection_coverage'][0]['selected_document_ids'], [101, 102])
        self.assertTrue(all('content' not in s and 'date_context_before' not in s for s in audit['spans']))

    async def test_second_subset_round_omits_a_newly_rejected_survivor(self):
        lines = [CHARGE, DOSE, LAB]
        for answer, text in [('\n'.join(lines), '\n'.join),
                             (ObservationCandidate(tuple(lines)), lambda kept: '\n\n'.join('- ' + x for x in kept))]:
            with self.subTest(observations=not isinstance(answer, str)):
                auditor = RoundAuditor({'laboratory.': 'unsupported'}, {'5 mg': 'missing'})
                result = await AnswerFinalizer(auditor).finalize('What is recorded?', answer, pack(SOURCE))
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(len(auditor.contexts), 3)
                if isinstance(answer, str):
                    self.assertEqual(auditor.contexts, [text(lines), text([CHARGE, DOSE]), text([CHARGE])])
                partial = result['verification']['partial']
                self.assertEqual([u['claim'].removeprefix('- ') for u in partial['omitted_units']], [LAB, DOSE])
                self.assertEqual([u['status'] for u in partial['omitted_units']], ['unsupported', 'missing'])
                self.assertEqual([u['id'] for u in partial['omitted_units']], ['u3', 'u2'])
                self.assertEqual((partial['omitted_count'], partial['original_total'], partial['original_supported']), (2, 3, 2))
                self.assertEqual(result['claim_ledger']['candidate_text'], text([CHARGE]))
                self.assertNotIn('5 mg', result['answer'])

    async def test_subset_rounds_are_bounded(self):
        auditor = RoundAuditor({'names': 'unsupported'}, {'laboratory.': 'unsupported'}, {'5 mg': 'unsupported'})
        result = await AnswerFinalizer(auditor).finalize('What is recorded?', '\n'.join([CHARGE, DOSE, LAB, NAME]), pack(SOURCE))
        self.assertEqual(result['finalization']['disposition'], 'unsupported')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('$321', result['answer'])
        self.assertNotIn('partial', result['verification'])
        self.assertEqual(len(auditor.contexts), 3)
        # The stored ledger stays the original audit, with each subset round nested under it.
        ledger = result['claim_ledger']
        self.assertEqual([c['status'] for c in ledger['claims']], ['supported'] * 3 + ['unsupported'])
        self.assertEqual(result['verification']['unsupported_claims'], [NAME])
        self.assertIn('rejection_reasons', ledger['claims'][3])
        self.assertEqual(ledger['subset_audit']['summary']['unsupported'], 1)
        second = ledger['subset_audit']['subset_audit']
        self.assertEqual(second['summary']['unsupported'], 1)
        self.assertTrue(all('content' not in s for s in second['spans']))

    async def test_conflict_in_a_subset_round_prevents_partial_delivery(self):
        auditor = RoundAuditor({'laboratory.': 'unsupported'}, {'5 mg': 'conflicting'})
        result = await AnswerFinalizer(auditor).finalize('What is recorded?', '\n'.join([CHARGE, DOSE, LAB]), pack(SOURCE))
        self.assertEqual(result['finalization']['disposition'], 'unsupported')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('$321', result['answer'])
        self.assertEqual(len(auditor.contexts), 2)

    async def test_omitted_ids_from_later_rounds_name_the_original_units(self):
        # The middle unit goes first, so the second round renumbers the survivors.
        auditor = RoundAuditor({'5 mg': 'unsupported'}, {'laboratory.': 'unsupported'})
        result = await AnswerFinalizer(auditor).finalize('What is recorded?', '\n'.join([CHARGE, DOSE, LAB]), pack(SOURCE))
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertEqual([(u['id'], u['claim']) for u in result['verification']['partial']['omitted_units']],
                         [('u2', DOSE), ('u3', LAB)])
