"""Source attribution must not become authority for a factual numeric value."""
import unittest

from app.answer_finalization import AnswerFinalizer, value_mismatches
from app.answer_observations import ObservationCandidate, ObservationValidationError


class SourceReferenceLabelTests(unittest.IsolatedAsyncioTestCase):
    async def finalize(self, text, *, reference_document_id=713, structured=True):
        content = 'Payment requested: $250 USD.'
        pack = {'items': [{'id': 'request-source', 'document_id': 713, 'chunk_index': 0,
                           'source_kind': 'ocr', 'title': 'Payment request', 'content': content}]}

        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                span = spans[0]
                return {'assessments': [{'unit_id': unit['id'], 'status': 'supported',
                    'references': [{'span_id': span['span_id'], 'evidence_id': span['evidence_id'],
                                    'document_id': reference_document_id, 'quote': content}]}
                    for unit in units]}

        candidate = ObservationCandidate.from_response({'observations': [text]}) if structured else text
        return await AnswerFinalizer(Auditor()).finalize(
            'What payment is requested?', candidate, pack, plan={'requires_current': False},
            mode='strict', evaluated_at='2026-09-12')

    async def test_bound_source_attribution_keeps_supported_observation(self):
        text = 'In Document 713, the requested payment is $250 USD.'
        result = await self.finalize(text)
        self.assertEqual(result['finalization']['disposition'], 'supported')
        self.assertEqual(result['claim_ledger']['claims'][0]['claim'], '- ' + text)
        self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['document_id'], 713)

    async def test_metadata_cannot_supply_a_factual_value(self):
        for text in ('In Document 713, the requested payment is $713 USD.',
                     'In Document 714, the requested payment is $250 USD.',
                     'In Document 713, account 713 requests $250 USD.'):
            with self.subTest(text=text):
                result = await self.finalize(text)
                self.assertNotEqual(result['finalization']['disposition'], 'supported')

    async def test_forged_reference_identity_still_fails(self):
        result = await self.finalize('In Document 714, the requested payment is $250 USD.',
                                     reference_document_id=714)
        self.assertIn('invalid_reference', result['claim_ledger']['claims'][0]['rejection_reasons'])

    def test_literal_quoted_or_malformed_identifiers_keep_value_checks(self):
        refs = [{'document_id': 713, 'quote': 'Payment requested: $250 USD.'}]
        for text in ('The source prints "Document 713".',
                     "The source prints 'Document 713'.",
                     'Document 713.5 requests $250 USD.',
                     'Document 713/2 requests $250 USD.',
                     'Policy 713 requests $250 USD.',
                     'The required form is Document 713.',
                     'The original prints: Document 713.',
                     'In Document 713, the required form is Document 713.',
                     '"In Document 713, payment requested."',
                     '`In Document 713, payment requested.`',
                     '[In Document 713, payment requested.](https://example.invalid)',
                     'In Document 713.5, payment requested.',
                     'In Document 713e2, payment requested.',
                     'In Document 713/2, payment requested.',
                     'In Document 0713, payment requested.',
                     'In Document +713, payment requested.'):

            with self.subTest(text=text):
                self.assertTrue(value_mismatches(text, refs))

    def test_metadata_ids_are_strict_and_occurrence_local(self):
        for doc_id in (True, '713', 0, -713):
            with self.subTest(doc_id=doc_id):
                self.assertTrue(value_mismatches('In Document 713, payment requested.',
                                [{'document_id': doc_id, 'quote': 'Payment requested.'}]))
        # Unstructured callers cannot assert provenance after markup is stripped.
        self.assertTrue(value_mismatches('- In Document 713, payment requested.',
                        [{'document_id': 713, 'quote': 'Payment requested.'}]))

    async def test_legacy_markup_cannot_acquire_metadata_authority(self):
        for text in ('In Document 713, payment requested.',
                     '[In Document 713, payment requested.](https://example.invalid)',
                     '`In Document 713, payment requested.`',
                     '    In Document 713, payment requested.',
                     '\tIn Document 713, payment requested.',
                     '1.     In Document 713, payment requested.'):
            with self.subTest(text=text):
                result = await self.finalize(text, structured=False)
                self.assertNotEqual(result['finalization']['disposition'], 'supported')

    def test_observation_boundary_rejects_wrappers_and_padding(self):
        for text in ('[In Document 713, payment requested.](https://example.invalid)',
                     '`In Document 713, payment requested.`',
                     '    In Document 713, payment requested.',
                     '\tIn Document 713, payment requested.',
                     '1.     In Document 713, payment requested.'):
            with self.subTest(text=text), self.assertRaises(ObservationValidationError):
                ObservationCandidate.from_response({'observations': [text]})
