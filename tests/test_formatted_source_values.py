"""Value rendering is symmetric without changing certified source references."""
import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, validate_reference, values_match
from tests.test_source_dates import ExactAuditor, pack


class FormattedSourceValueTests(unittest.IsolatedAsyncioTestCase):
    async def test_balanced_source_amounts_and_units_keep_raw_quotes(self):
        cases = [('The charge is $500.', 'The charge is $**500.00**.'),
                 ('The dose is 5 mg.', 'The dose is **5** **mg**.'),
                 ('The charge is -500 USD.', 'The charge is **`-500.00`** USD.')]
        for claim, source in cases:
            result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', claim, pack(source))
            self.assertTrue(result['finalization']['answer_verified'], (claim, source))
            reference = result['claim_ledger']['claims'][0]['references'][0]
            self.assertEqual(reference['quote'], source)
            self.assertEqual(source[reference['start']:reference['end']], source)

    def test_rendering_never_changes_quantity_or_joins_reference_strings(self):
        for claim, source in [('The charge is $500.', 'The charge is $**5000.00**.'),
                              ('The charge is $500.', 'The charge is $**-500.00**.'),
                              ('The charge is $500.', 'The charge is - $**500**.'),
                              ('The dose is 5 mg.', 'The dose is - **5** mg.'),
                              ('The charge is 500 USD.', 'The charge is **500.00** EUR.'),
                              ('The dose is 5 mg.', 'The dose is **5** **g**.'),
                              ('The charge is $500.', 'The charge is $**500.00.'),
                              ('The charge is $500.', 'The charge is $**50** and **0**.')]:
            self.assertFalse(values_match(claim, [{'quote': source}]), (claim, source))
        self.assertFalse(values_match('The charge is $500.', [{'quote': '$**'}, {'quote': '500.00**'}]))

    def test_wrapped_reference_cannot_chop_numeric_tokens_or_signs(self):
        cases = [('$**500**0', '$**500**'), ('1**500** USD', '**500** USD'),
                 ('-$**500** USD', '$**500** USD'), ('-$**500** USD', '**500** USD'),
                 ('Charge - $**500** USD', '$**500** USD'), ('Dose - **5** mg', '**5** mg'),
                 ('$**500**.01', '$**500**'), ('$**500**e2', '$**500**'),
                 ('ID**500**A', '**500**'), ('**1.500** USD', '**500** USD')]
        for source, quote in cases:
            spans = evidence_spans(pack(source))
            ref = {key: spans[0][key] for key in ('span_id', 'evidence_id', 'document_id')}
            self.assertIsNone(validate_reference({**ref, 'quote': quote}, spans), (source, quote))
        # The two raw context characters hide the preceding digit outside the window.
        spans = evidence_spans(pack('1' + '*' * 3799 + '500 USD'))
        span = spans[1]
        ref = {key: span[key] for key in ('span_id', 'evidence_id', 'document_id')}
        self.assertIsNone(validate_reference({**ref, 'quote': '500 USD'}, [span]))

    def test_signed_currency_keeps_the_sign_on_both_comparison_copies(self):
        self.assertTrue(values_match('Charge -$500.', [{'quote': 'Charge -$**500.00**.'}]))
        self.assertFalse(values_match('Charge $500.', [{'quote': 'Charge -$**500.00**.'}]))
        self.assertFalse(values_match('Charge -$500.', [{'quote': 'Charge $**500.00**.'}]))

    async def test_full_formatted_negative_source_cannot_verify_positive_claim(self):
        for spacing in (' ', '\u00a0', '\u2009', '\n'):
            for claim, source in [('The charge is $500.', 'The charge is - $**500**.'),
                                  ('The dose is 5 mg.', 'The dose is - **5** mg.')]:
                source = source.replace('- ', '-' + spacing)
                result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', claim, pack(source))
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertNotIn(claim, result['answer'])
                negative = claim.replace('$500', '-$500').replace('5 mg', '-5 mg')
                result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', negative, pack(source))
                self.assertTrue(result['finalization']['answer_verified'])
                spans = evidence_spans(pack(source))
                span = spans[0]
                quote = source.split(spacing, 1)[-1] if source.startswith('-') else source.split('-' + spacing, 1)[-1]
                ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote}
                self.assertIsNone(validate_reference(ref, spans))

    async def test_formatted_decimal_tail_beyond_window_cannot_be_trimmed(self):
        quote = '$`500`'
        source = ' ' * (4000 - len(quote)) + quote + '*.*01'
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                span = next(s for s in spans if s['start'] == 0)
                ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote}
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                                        'references': [ref]} for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What was charged?', 'The charge was $500.', pack(source))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('The charge was $500.', result['answer'])

    async def test_markdown_list_markers_are_not_numeric_signs(self):
        for claim, source in [('- $500 service fee.', 'The service fee is $500.'),
                              ('The service fee is $500.', 'Recorded charges:\n- $**500** service fee.\n- $**50** tax.')]:
            result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', claim, pack(source))
            self.assertTrue(result['finalization']['answer_verified'])
            self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['quote'], source)
        source = 'Recorded charges:\n- $**500** service fee.\n- $**50** tax.'
        spans = evidence_spans(pack(source))
        span = spans[0]
        ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': '$**500** service fee.'}
        self.assertIsNotNone(validate_reference(ref, spans))

    async def test_quoted_in_sentence_sign_never_becomes_a_list_marker(self):
        for source, quote, claim in [('The charge is - $**500**.', '- $**500**.', 'The charge is $500.'),
                                     ('The dose is - **5** mg.', '- **5** mg.', 'The dose is 5 mg.')]:
            class Auditor:
                async def audit_answer_units(self, question, units, spans, plan):
                    span = spans[0]
                    ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote,
                           'source_list_markers': [[0, 2]]}  # A model cannot supply this authority.
                    return {'assessments': [{'unit_id': u['id'], 'status': 'supported', 'references': [ref]} for u in units]}
            result = await AnswerFinalizer(Auditor()).finalize('What is recorded?', claim, pack(source))
            self.assertTrue(result['claim_ledger']['complete'])
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['source_list_markers'], [])
            self.assertNotIn(claim, result['answer'])

    async def test_fenced_literal_signs_stay_negative_in_full_and_trimmed_quotes(self):
        for fence in ('```', '~~~~'):
            for prefix in ('', 'Context. ' * 450):
                source = prefix + '\n' + fence + '\n- 500 mg\n' + fence
                for quote in ([source] if not prefix else []) + ['- 500 mg', '500 mg']:
                    class Auditor:
                        async def audit_answer_units(self, question, units, spans, plan):
                            span = next(s for s in spans if quote in s['content'])
                            ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote,
                                   'source_list_markers': [[0, 2]]}
                            return {'assessments': [{'unit_id': u['id'], 'status': 'supported', 'references': [ref]} for u in units]}
                    result = await AnswerFinalizer(Auditor()).finalize('What is recorded?', 'The dose is 500 mg.', pack(source))
                    self.assertTrue(result['claim_ledger']['complete'])
                    self.assertFalse(result['finalization']['answer_verified'], (fence, bool(prefix), quote))

    async def test_unknown_continuation_chunk_cannot_authorize_a_list_marker(self):
        evidence = pack('- 500 mg')
        evidence['items'][0]['chunk_index'] = 1
        result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', 'The dose is 500 mg.', evidence)
        self.assertTrue(result['claim_ledger']['complete'])
        self.assertFalse(result['finalization']['answer_verified'])

    async def test_quote_boundary_cannot_create_a_closing_fence(self):
        source = '```\nLiteral abc```\n- 500 mg\n```'
        quote = '```\n- 500 mg\n```'
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                span = spans[0]
                ref = {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote}
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported', 'references': [ref]} for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What is recorded?', 'The dose is 500 mg.', pack(source))
        self.assertTrue(result['claim_ledger']['complete'])
        self.assertFalse(result['finalization']['answer_verified'])

    async def test_container_code_fence_does_not_turn_literal_sign_into_bullet(self):
        source = '- ```\n  - 500 mg\n  ```'
        result = await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', 'The dose is 500 mg.', pack(source))
        self.assertTrue(result['claim_ledger']['complete'])
        self.assertFalse(result['finalization']['answer_verified'])
