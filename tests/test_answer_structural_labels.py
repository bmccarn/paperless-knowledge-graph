"""Presentation labels keep their factual context through subset verification."""
import unittest

from app.answer_finalization import AnswerFinalizer, answer_units
from tests.test_source_dates import pack


class StructuralLabelTests(unittest.IsolatedAsyncioTestCase):
    def test_strong_labels_join_following_prose_with_exact_ranges(self):
        for newline in ('\n', '\r\n', '\r'):
            for marker in ('', '* ', '1. '):
                for delimiter in ('**', '__'):
                    for topic in ('Invoice charges', 'Recorded measurements'):
                        heading = marker + delimiter + topic + delimiter
                        answer = heading + newline * 2 + 'The record shows 20 units.'
                        units = answer_units(answer)
                        self.assertEqual([u['text'] for u in units], [answer])
                        self.assertEqual(answer[units[0]['start']:units[0]['end']], answer)

    async def test_labeled_facts_survive_or_are_omitted_as_complete_units(self):
        for label in ('**Invoice charges**', '__Recorded measurements__'):
            supported = label + '\n\nThe statement records $20.'
            unsupported = '**Additional charge**\n\nThe extra fee is $999.'
            calls = []
            class Auditor:
                async def audit_answer_units(self, question, units, spans, plan):
                    calls.append([u['text'] for u in units])
                    span = spans[0]
                    return {'assessments': [{
                        'unit_id': u['id'], 'status': 'supported' if u['text'] == supported else 'unsupported',
                        'temporal_scope': 'historical',
                        'references': [{**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')},
                                        'quote': 'The statement records $20.'}],
                    } for u in units]}
            result = await AnswerFinalizer(Auditor()).finalize(
                'What is recorded?', supported + '\n\n' + unsupported, pack('The statement records $20.'))
            self.assertEqual(calls, [[supported, unsupported], [supported]])
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertIn(supported, result['answer'])
            self.assertNotIn('Additional charge', result['answer'])
            self.assertEqual(result['claim_ledger']['summary']['total'], 1)

    async def test_quoted_emphasized_facts_remain_independently_deliverable(self):
        for first in ('**The invoice states “Paid.”**', '**The invoice states "Paid."**',
                      '**The invoice records $20 (paid.)**'):
            class Auditor:
                async def audit_answer_units(self, question, units, spans, plan):
                    span = spans[0]
                    return {'assessments': [{
                        'unit_id': u['id'], 'status': 'unsupported' if '$999' in u['text'] else 'supported',
                        'temporal_scope': 'historical',
                        'references': [{**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': first}],
                    } for u in units]}
            result = await AnswerFinalizer(Auditor()).finalize(
                'What is recorded?', first + '\n\nThe extra fee is $999.', pack(first))
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertIn(first, result['answer'])
            self.assertNotIn('$999', result['answer'])

    async def test_factual_label_values_are_audited_not_discarded(self):
        answer = '**Recorded charge $999**\n\nThe statement records $20.'
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                span = spans[0]
                return {'assessments': [{
                    'unit_id': u['id'], 'status': 'supported', 'temporal_scope': 'historical',
                    'references': [{**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')},
                                    'quote': 'The statement records $20.'}],
                } for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What is recorded?', answer, pack('The statement records $20.'))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['claims'][0]['claim'], answer)
        self.assertIn('value_mismatch', result['claim_ledger']['claims'][0]['rejection_reasons'])

    def test_emphasized_sentences_and_literal_blocks_are_not_labels(self):
        for first in ('**The statement records $20.**', '__The statement records $20.__',
                      'The statement **records $20**.', '**First** and **second**',
                      '**The invoice states “Paid.”**', '**The invoice states "Paid."**',
                      '**The invoice records $20 (paid.)**', '**The invoice states [Paid.]**'):
            answer = first + '\n\nThe next charge is $999.'
            self.assertEqual([u['text'] for u in answer_units(answer)], [first, 'The next charge is $999.'])
        for answer in ('```\n**Literal label**\n```\n\nThe next charge is $999.',
                       '<div>\n**Literal label**\n</div>\n\nThe next charge is $999.'):
            self.assertTrue(any(u['text'] == '**Literal label**' for u in answer_units(answer)))
        for label in ('**Trailing label**', '__Trailing label__'):
            self.assertEqual([u['text'] for u in answer_units(label)], [label])
