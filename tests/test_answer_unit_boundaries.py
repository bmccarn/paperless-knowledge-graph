"""Names and abbreviations cannot create separately publishable fragments."""
import unittest
from app.answer_finalization import AnswerFinalizer, answer_units
from tests.test_source_dates import pack


class AnswerUnitBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_initials_stay_with_names_and_offsets_remain_exact(self):
        for name in ('Alice Q. Example', 'Dr. Maria T. Chen', 'Jordan R. Sample',
                     'Émile Q. de Vries', 'Zoë É. Durand', 'Zoe E\u0301. Durand', 'Alice Q.\nExample',
                     'Alice Q.\nR. Example', 'Alice Q.\rExample', 'J. Smith', 'É. Durand'):
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
                       'The record says approx.\n', 'The form lists examples, e.g.\na prior invoice'):
            await self.check_subset(prefix + ' and costs $999.', 'The statement records $321.', prefix)

    async def test_letter_values_and_terminal_abbreviations_keep_independent_sentences(self):
        for first in ('The record lists vitamin A.', 'The record assigns grade B.',
                      'The record lists apartment Q.', 'The record names Acme Inc.'):
            await self.check_subset(first, 'An extra charge is $999.', '$999', expected=first)

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
