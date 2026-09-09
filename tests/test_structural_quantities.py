"""Quantity support from structurally bound original tables, never stitched quotes."""
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans, validate_reference, values_match
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision

TABLE = '| Charge USD | Credit USD | Balance USD |\n| --- | --- | --- |\n| 100 | 0 | 500 |'
MEASURE = '| Measurement | Result | Target |\n| --- | --- | --- |\n| Concentration mg/L | 12 | 4 |'


def pack(text, **metadata):
    return {'items': [dict(id='original', document_id=1, chunk_index=0, title='Original record',
        content=text, source_content=text, source_kind='ocr', **metadata)]}


def references(text, *, chunk_index=0):
    evidence = pack(text)
    evidence['items'][0]['chunk_index'] = chunk_index
    return [validate_reference({'span_id': span['span_id']}, [span])
            for span in evidence_spans(evidence, citation_safe=True)]


class StructuralQuantityTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_finalizer_accepts_structural_units_and_preserves_original_quote(self):
        for original, claim in ((TABLE, 'The record shows a charge of $100 USD and balance of $500 USD.'),
                                (MEASURE, 'The measured concentration is 12 mg/L.')):
            with self.subTest(claim=claim):
                evidence = pack(original)
                before = copy.deepcopy(evidence)
                spans = evidence_spans(evidence, citation_safe=True)
                auditor = StrandsQueryOrchestrator()
                auditor.enabled = True
                row = decision(references=[{'span_id': spans[0]['span_id']}])
                with patch.object(auditor, '_text_agent', AsyncMock(return_value=json.dumps({'assessments': [row]}))):
                    result = await AnswerFinalizer(auditor).finalize('What does the record show?', claim, evidence)
                self.assertTrue(result['finalization']['answer_verified'], result['claim_ledger'])
                self.assertEqual(evidence, before)
                self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['quote'], original)

    def test_wrong_amount_sign_currency_and_scale_remain_rejected(self):
        refs = references(TABLE)
        self.assertTrue(values_match('Charge: $100 USD. Balance: $500 USD.', refs))
        for claim in ('Charge: $101 USD.', 'Charge: -$100 USD.', 'Charge: $100 CAD.', 'Charge: $0.1 USD.'):
            with self.subTest(claim=claim):
                self.assertFalse(values_match(claim, refs))
        self.assertFalse(values_match('Charge: 100 USD.', references(TABLE.replace('USD', '$'))))
        self.assertTrue(values_match('Charge: $100.', references(TABLE)))
        self.assertTrue(values_match('Length: 12 m.', references('| Length m |\n| --- |\n| 12 |')))

    def test_compact_prose_quantities_keep_unit_obligations(self):
        for claim, source in (('Dose: 12mg.', 'Dose: 12 mg.'), ('Rate: 12%.', 'Rate: 12 percent.'),
                              ('Amount: 100 USD.', 'Amount: USD 100.')):
            if '%' in claim:
                # No percent-symbol/name conversion was introduced.
                self.assertFalse(values_match(claim, references(source)))
            else:
                self.assertTrue(values_match(claim, references(source)), (claim, source))
        self.assertFalse(values_match('Dose: 12mg.', references('Amount: 12 USD.')))

    def test_compound_units_are_not_prefixes_or_conversions(self):
        refs = references(MEASURE)
        self.assertTrue(values_match('Concentration: 12 mg/L.', refs))
        for claim in ('Concentration: 12 mg.', 'Concentration: 12 mg/dL.', 'Concentration: 0.012 g/L.'):
            self.assertFalse(values_match(claim, refs), claim)
        self.assertFalse(values_match('Concentration: 12 mg/L.', references('Concentration: 12 mg/L/min.')))
        conflict = '| Measurement | Result mg/L/min |\n| --- | --- |\n| Concentration mg/L | 12 |'
        self.assertFalse(values_match('Concentration: 12 mg/L.', references(conflict)))

    def test_unrecognized_compound_is_retained_as_an_exact_requirement(self):
        self.assertFalse(values_match('Concentration: 12 mg/L/min.', references('The value is 12 USD.')))
        self.assertTrue(values_match('Concentration: 12 mg/L/min.', references('Concentration: 12 mg/L/min.')))
        for unit in ('mg/L²', 'mg/L^2', 'mg/L2', 'mg/L22X', 'mg/L²x', 'mg/Lé'):
            self.assertFalse(values_match('Concentration: 12 mg/L.', references('Concentration: 12 ' + unit + '.')))
            self.assertFalse(values_match('Concentration: 12 ' + unit + '.', references('Concentration: 12 mg/L.')))
            self.assertTrue(values_match('Concentration: 12 ' + unit + '.', references('Concentration: 12 ' + unit + '.')))
            table = '| Concentration ' + unit + ' |\n| --- |\n| 12 |'
            if unit in {'mg/L²', 'mg/L^2', 'mg/L2'}:
                self.assertTrue(values_match('Concentration: 12 ' + unit + '.', references(table)))

    def test_multiple_unit_labels_are_ambiguous_and_normal_words_are_not_units(self):
        ambiguous = '| Charge USD CAD |\n| --- |\n| 100 |'
        self.assertFalse(values_match('Charge: 100 CAD.', references(ambiguous)))
        for label in ('sample', 'mass', 'height'):
            table = '| Item | Amount USD |\n| --- | --- |\n| ' + label + ' | 100 |'
            self.assertTrue(values_match('Amount: 100 USD.', references(table)), label)

    def test_split_or_unknown_context_does_not_authorize_table_inheritance(self):
        header, row = TABLE.rsplit('\n', 1)
        self.assertFalse(values_match('Charge: 100 USD.', references(header) + references(row)))
        self.assertFalse(values_match('Charge: 100 USD.', references(TABLE, chunk_index=1)))
        self.assertFalse(values_match('Charge: 100 USD.', references('Currency USD.\n| Charge |\n| --- |\n| 100 |')))

    def test_scaled_conflicting_and_ragged_tables_remain_unavailable(self):
        invalid = [TABLE.replace('Charge USD', label) for label in
                   ('Charge USD (thousands)', 'Charge thousands USD', 'Charge USD ×10³', 'Charge K USD', 'Charge M USD')]
        invalid += ['| Measurement USD | Amount CAD |\n| --- | --- |\n| Balance USD | 100 |',
                    '| Charge USD | Balance USD |\n| --- | --- |\n| 100 |']
        for source in invalid:
            self.assertFalse(values_match('Charge: 100 USD.', references(source)), source)

    def test_literal_tables_cannot_gain_structure_when_quote_is_clipped(self):
        for start, end in (('```text\n', '\n```'), ('<pre>\n', '\n</pre>')):
            source = start + ('literal text\n' * 400) + TABLE + end
            refs = references(source)
            clipped = [ref for ref in refs if TABLE in ref['quote']]
            self.assertTrue(clipped)
            self.assertTrue(all(start.strip() not in ref['quote'] for ref in clipped))
            self.assertFalse(values_match('Charge: 100 USD.', clipped))

    def test_unicode_separators_cannot_shift_certified_original_table_ranges(self):
        literal = '| Charge USD |\n| --- |\n| 100 |'
        actual = '| Value |\n| --- |\n| 500 |'
        for separator in ('\u2028', '\x85', '\v', '\f', '\u2029'):
            source = ('intro' + separator + separator.join(['x'] * 5)) + '\n```text\n' + literal + '\n```\n\n' + actual
            self.assertFalse(values_match('Charge: 100 USD.', references(source)), repr(separator))
        for newline in ('\r\n', '\r', '\n'):
            source = TABLE.replace('\n', newline)
            self.assertTrue(values_match('Charge: 100 USD.', references(source)))

    def test_long_unit_like_identifiers_have_bounded_parsing(self):
        import subprocess
        import sys
        script = "from app.source_quantities import _label_unit, table_quantities; " \
                 "labels=['Concentration mg'+'1'*10000+'X', 'm'+'²'*10000+'x']; " \
                 "assert all(_label_unit(label) is None for label in labels); " \
                 "table='| Item | Amount USD |\\n| --- | --- |\\n| '+labels[0]+' | 100 |'; " \
                 "table_quantities(table, [[0,len(table)]])"
        result = subprocess.run([sys.executable, '-c', script], capture_output=True, timeout=2)
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_unicode_negative_table_value_preserves_sign(self):
        source = '| Charge USD |\n| --- |\n| −100 |'
        self.assertTrue(values_match('Charge: -100 USD.', references(source)))
        self.assertFalse(values_match('Charge: 100 USD.', references(source)))

    async def test_table_quantities_cannot_override_semantic_role_rejection(self):
        auditor = StrandsQueryOrchestrator()
        auditor.enabled = True
        spans = evidence_spans(pack(MEASURE), citation_safe=True)
        row = decision(references=[{'span_id': spans[0]['span_id']}])
        row['checks']['predicate'] = 'contradicted'
        with patch.object(auditor, '_text_agent', AsyncMock(return_value=json.dumps({'assessments': [row]}))):
            result = await AnswerFinalizer(auditor).finalize('What was measured?', 'The measured concentration is 4 mg/L.', pack(MEASURE))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertIn('semantic_predicate', result['claim_ledger']['claims'][0]['rejection_reasons'])
