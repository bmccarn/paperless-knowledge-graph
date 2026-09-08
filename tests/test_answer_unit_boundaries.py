"""Names and abbreviations cannot create separately publishable fragments."""
import unittest
from app.answer_finalization import AnswerFinalizer, answer_units
from tests.test_source_dates import pack


class AnswerUnitBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_initials_stay_with_names_and_offsets_remain_exact(self):
        for name in ('Alice Q. Example', 'Dr. Maria T. Chen', 'Jordan R. Sample',
                     'Émile Q. de Vries', 'Zoë É. Durand', 'Zoe E\u0301. Durand', 'Alice Q.\nExample',
                     'Alice Q.\nR. Example', 'Alice Q.\rExample', 'J. Smith', 'É. Durand', 'J. van der Meer'):
            first = f'The receipt names {name}.'
            second = 'The recorded charge is $20.50.'
            answer = first + ' ' + second
            units = answer_units(answer)
            self.assertEqual([u['text'] for u in units], [first, second])
            for u in units:
                self.assertEqual(answer[u['start']:u['end']], u['text'])

    async def test_unsupported_named_sentence_cannot_publish_its_initial_fragments(self):
        for prefix in ('The receipt names Alice Q. Example and Jordan R. Sample',
                       'The receipt names Zoe E\u0301. Durand', 'The record says approx.',
                       'The form lists examples, e.g.', 'The receipt names J. Smith',
                       'The receipt names Alice Q.\nR. Example', 'The receipt names Alice Q.\rExample',
                       'The receipt names J. van der Meer', 'The receipt names J. de Vries',
                       'The record says approx.\n', 'The form lists examples, e.g.\na prior invoice'):
            await self.check_subset(prefix + ' and costs $999.', 'The statement records $321.', prefix)

    async def test_letter_values_and_terminal_abbreviations_keep_independent_sentences(self):
        for first in ('The record lists vitamin A.', 'The record assigns grade B.',
                      'The record lists apartment Q.', 'The record names Acme Inc.'):
            await self.check_subset(first, 'An extra charge is $999.', '$999', expected=first)

    async def test_letter_values_before_content_noun_sentences_remain_independent(self):
        for first, second in (
            ('The laboratory reports grade B.', 'Follow-up testing costs $999.'),
            ('The record lists vitamin A.', 'Supplements cost $999.'),
            ('The record lists apartment Q.', 'Rent is $999.'),
        ):
            await self.check_subset(first, second, '$999', expected=first)

    def test_headings_numbering_and_line_endings_do_not_create_fragments(self):
        for newline in ('\n', '\r\n', '\r'):
            for heading in ('', '# Recorded observations'+newline, 'Recorded observations:'+newline):
                for marker in ('', '1. ', '* '):
                    for body in ('The invoice charges $999.', 'The receipt names J. van der Meer and costs $999.',
                                 'The receipt names Alice Q.'+newline+'R. Example and costs $999.',
                                 'The record says approx.'+newline+'$999.',
                                 'The record gives examples, e.g.'+newline+'a prior charge of $999.'):
                        first = heading + marker + body
                        answer = first + newline + 'The statement records $321.'
                        units = answer_units(answer)
                        self.assertEqual([u['text'] for u in units], [first, 'The statement records $321.'])
                        self.assertTrue(all(answer[u['start']:u['end']] == u['text'] for u in units))

    async def check_subset(self, first, second, omitted, expected='The statement records $321.'):
        source = expected
        answer = first + ' ' + second
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                span = spans[0]
                return {'assessments': [{
                    'unit_id': u['id'], 'status': 'unsupported' if '$999' in u['text'] else 'supported',
                    'temporal_scope': 'historical',
                    'references': [{**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': source}],
                } for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What is recorded?', answer, pack(source))
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn(omitted, result['answer'])
        self.assertIn(expected, result['answer'])
        self.assertEqual(result['claim_ledger']['summary']['total'], 1)
