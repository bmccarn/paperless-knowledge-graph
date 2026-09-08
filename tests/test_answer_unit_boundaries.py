"""Names and abbreviations cannot create separately publishable fragments."""
import unittest
from app.answer_finalization import AnswerFinalizer, answer_units
from tests.test_source_dates import pack


class AnswerUnitBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_initials_stay_with_names_and_offsets_remain_exact(self):
        for name in ('Alice Q. Example', 'Dr. Maria T. Chen', 'Jordan R. Sample',
                     'Émile Q. de Vries', 'Zoë É. Durand'):
            first = f'The receipt names {name}.'
            second = 'The recorded charge is $20.50.'
            answer = first + ' ' + second
            units = answer_units(answer)
            self.assertEqual([u['text'] for u in units], [first, second])
            for u in units:
                self.assertEqual(answer[u['start']:u['end']], u['text'])

    async def test_unsupported_named_sentence_cannot_publish_its_initial_fragments(self):
        source = 'The receipt names Alice Q. Example and Jordan R. Sample. The statement records $321.'
        answer = 'The receipt names Alice Q. Example and Jordan R. Sample and costs $999.\nThe statement records $321.'
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
        self.assertNotIn('Alice', result['answer'])
        self.assertNotIn('Example', result['answer'])
        self.assertEqual(result['claim_ledger']['summary']['total'], 1)
