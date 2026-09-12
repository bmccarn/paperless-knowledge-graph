"""Calendar equivalence preserves source precision, values and exact quotations."""
import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, values_match
from app.source_dates import source_dates, source_date_occurs


class ExactAuditor:
    def __init__(self): self.calls = 0

    async def audit_answer_units(self, question, units, spans, plan):
        self.calls += 1
        span = spans[0]
        return {"assessments": [{"unit_id": unit["id"], "status": "supported", "temporal_scope": "historical",
                "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                "document_id": span["document_id"], "quote": span["content"]}]} for unit in units]}


def pack(text):
    return {"items": [{"id": "dated-source", "document_id": 101, "chunk_index": 0,
                       "title": "Service record", "content": text, "source_kind": "ocr"}]}


class SourceDateTests(unittest.IsolatedAsyncioTestCase):
    def test_horizontal_slash_spacing_preserves_calendar_precision(self):
        for separator in (' / ', '\t/ ', '\u00a0/\u202f', '/\t'):
            for order, fields in [('mdy', ('8', '4')), ('dmy', ('4', '8'))]:
                for year in ('26', '2026'):
                    written = separator.join((*fields, year))
                    source = f'Service date: {written}.'
                    compact = '/'.join((*fields, year))
                    with self.subTest(written=written, order=order):
                        found = source_dates(source, order)
                        self.assertEqual(len(found), 1)
                        self.assertEqual(found[0].text, written)
                        self.assertEqual(source[found[0].start:found[0].end], written)
                        self.assertTrue(source_date_occurs(compact, source, order))
                        self.assertTrue(source_date_occurs(written, f'Date: {compact}', order))
                        self.assertTrue(values_match(f'Service date {compact}.', [{'quote': source}], date_order=order))
                        if year == '26':
                            self.assertIsNone(found[0].value)
                            self.assertEqual(found[0].reason, 'unspecified_century')
                            self.assertFalse(source_date_occurs('2026-08-04', source, order))
                        else:
                            self.assertEqual(found[0].value, '2026-08-04')

    def test_spaced_dates_do_not_gain_authority_from_invalid_tokens(self):
        for source in ('Date: 8 / 5 / 26', 'Policy #8 / 4 / 26',
                       'Date: 0 / 8 / 4 / 26', 'Date: 8 / 4 / 26 / 7'):
            self.assertFalse(values_match('Service date 8/4/26.', [{'quote': source}]), source)
        for source in ('Date: 0 / 8 / 4 / 2026', 'Date: 8 / 4 / 2026 / 7'):
            self.assertFalse(any(d.value is not None for d in source_dates(source)), source)
        for suffix in ('7.5', '7code', '7-9'):
            source = f'Date: 8 / 4 / 2026 / {suffix}'
            self.assertFalse(values_match('Service date August 4, 2026.', [{'quote': source}]))
            self.assertFalse(any(d.value is not None for d in source_dates(source)))
        self.assertFalse(values_match('Service date 8/4/26.', [{'quote': 'Date: 8 / 4 / 26 / 7code'}]))
        self.assertFalse(source_date_occurs('2026-08-04', 'ID X / 8 / 4 / 2026'))
        self.assertFalse(source_date_occurs('2026-08-04', '8 / 4 / 2026', context_before='ID X / '))
        for scalar in ('Ratio 5 / 10 / 100', 'Record number 8 / 4 / 2026 / 7'):
            self.assertTrue(values_match(scalar, [{'quote': scalar}]))
        for newline in ('\n', '\r', '\r\n', '\u2028', '\u2029'):
            self.assertFalse(source_date_occurs('8/4/26', f'Date: 8 /{newline}4 / 26'))
        self.assertFalse(source_date_occurs('8/4/26', 'Date: 8 / 4 / 26', 'reject_ambiguous'))
        self.assertFalse(source_date_occurs('2/30/26', 'Date: 2 / 30 / 26'))
        self.assertFalse(values_match('Service date 2026-08-04.', [{'quote': 'Date: 8 / 4 / 26'}]))

    def test_prose_slashes_preserve_named_and_iso_calendar_authority(self):
        for date in ('August 4, 2026', '2026-08-04', '8/4/2026', '8 / 4 / 2026'):
            self.assertTrue(values_match('Service date August 4, 2026.',
                                        [{'quote': f'Service date: {date} / Status: completed'}]))
        self.assertTrue(values_match('September 5, 2026',
                                    [{'quote': 'August 4, 2026 / September 5, 2026'}]))

    async def test_spaced_date_survives_finalization_with_original_quote(self):
        source = 'Service date: 8 / 4 / 26. Conditional charge: $217 USD.'
        result = await AnswerFinalizer(ExactAuditor()).finalize(
            'What was recorded?', 'On 8/4/26 the record lists a conditional charge of $217 USD.', pack(source))
        self.assertEqual(result['finalization']['disposition'], 'supported')
        self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['quote'], source)

    def test_equivalent_calendar_formats_and_precision(self):
        for claim, source in [
            ("Service began 2026-09-01.", "Service began September 1, 2026."),
            ("Service began September 1, 2026.", "Service began 2026-09-01."),
            ("Service began 2026-09-01.", "Service began 09/01/2026."),
            ("The September 2026 statement records $321 USD.", "Statement dated 2026-09-01: $321 USD."),
            ("Dose changed to 5 mg on 2026-09-01.", "On Sep 1, 2026, dose changed to 5 mg."),
        ]:
            with self.subTest(claim=claim, source=source):
                self.assertTrue(values_match(claim, [{"quote": source}]))

    def test_wrong_month_day_precision_and_century_do_not_gain_support(self):
        for claim, source in [
            ("Service began September 2, 2026.", "Service began 2026-02-09."),
            ("Service began 2026-09-01.", "Service began September 2026."),
            ("Service began 2026-09-01.", "Service began 09/01/26."),
            ("Service began August 4, 2026.", "Record identifier: 08042026040459."),
            ("Service began February 30, 2026.", "Date: February 28, 2026. Item 30."),
            ("Charge: $9 USD on 2026-09-01.", "Charge: $1 USD on September 9, 2026."),
            ("Dose: 5 mg on 2026-09-01.", "Dose: 5 g on September 1, 2026."),
        ]:
            with self.subTest(claim=claim, source=source):
                self.assertFalse(values_match(claim, [{"quote": source}]))

    def test_numeric_order_is_explicit_and_calendar_invalid_dates_stay_invalid(self):
        refs = [{"quote": "Service date 04/08/2026."}]
        self.assertTrue(values_match("2026-08-04", refs, date_order="dmy"))
        self.assertFalse(values_match("2026-08-04", refs, date_order="mdy"))
        self.assertFalse(values_match("2026-08-04", refs, date_order="reject_ambiguous"))
        for value in ("02/30/26", "02/29/2025", "2026-13-01", "April 31, 2026"):
            self.assertFalse(values_match(value, [{"quote": value}]))
        self.assertFalse(values_match("August 4, 2026", [{"quote": "August"}, {"quote": "4, 2026"}]))
        self.assertFalse(values_match("$2026 USD", [{"quote": "In 2026, $9 USD was recorded."}]))
        self.assertFalse(values_match("Charge -321 USD on 2026-08-04", [{"quote": "Charge 321 USD on August 4, 2026"}]))


    async def test_scalar_quantities_identifiers_and_malformed_dates_keep_their_boundaries(self):
        for claim, source in [("Invoice number 0000.", "Invoice number 0000."),
                              ("Invoice number 2026-001.", "Invoice number 2026-001."),
                              ("Record identifier 2026-150.", "Record identifier 2026-150."),
                              ("Contract no. 2026-09-001.", "Contract no. 2026-09-001."),
                              ("Policy #2026-09-01.", "Policy #2026-09-01."),
                              ("Charge USD 1200.", "Charge USD 1,200."),
                              ("Dose 1200 mg.", "Dose 1,200 mg."),
                              ("Charge USD 1200.00.", "Charge USD 1200."),
                              ("Charge USD 1,200.", "Charge USD 1200."),
                              ("Recorded term 2026-2027.", "Recorded term 2026-2027.")]:
            with self.subTest(claim=claim):
                result = await AnswerFinalizer(ExactAuditor()).finalize("What was recorded?", claim, pack(source))
                self.assertTrue(result["finalization"]["complete"])
        for value in ("2026-02-300", "2026-09-001", "2026-099-01", "2026-02-30.5", "2026-02-01-02"):
            result = await AnswerFinalizer(ExactAuditor()).finalize("Recorded date?", f"Record date {value}.", pack(f"Record date {value}."))
            self.assertEqual(result["finalization"]["disposition"], "unsupported", value)
        self.assertFalse(values_match("Value 1.2026.", [{"quote": "Value 1 recorded in 2026."}]))
        self.assertFalse(values_match("Contract no. 2026-09-002.", [{"quote": "Contract no. 2026-09-001."}]))
        self.assertFalse(values_match("Record date 2026-09-01.", [{"quote": "Policy #2026-09-01."}]))
        for prefix in ("# ", "## ", "### ", "Date #"):
            result = await AnswerFinalizer(ExactAuditor()).finalize(
                "What was recorded?", prefix + "2026-02-30\nThe invoice records a $321 USD charge.",
                pack("Record date 2026-02-30. The invoice records a $321 USD charge."))
            self.assertIn(result["finalization"]["disposition"], {"unsupported", "partial"}, prefix)
            self.assertNotIn("2026-02-30", result["answer"])

    async def test_public_finalizer_preserves_original_written_date_quote(self):
        source = "On September 1, 2026, the monthly service charge became $321 USD."
        result = await AnswerFinalizer(ExactAuditor()).finalize(
            "What changed?", "The monthly service charge became $321 USD on 2026-09-01.", pack(source))
        self.assertEqual(result["finalization"]["disposition"], "supported")
        reference = result["claim_ledger"]["claims"][0]["references"][0]
        self.assertEqual(reference["quote"], source)
        self.assertEqual(source[reference["start"]:reference["end"]], source)
