"""Calendar equivalence preserves source precision, values and exact quotations."""
import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, values_match
from app.timeline import validate_timeline


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

    async def test_timeline_reports_reason_counts_and_semantic_event_mismatch(self):
        source = "Record dated August 4, 2026."
        evidence = pack(source)
        span = evidence_spans(evidence)[0]
        reference = {"span_id": span["span_id"], "evidence_id": span["evidence_id"], "document_id": 101, "quote": source}
        event = {"date": "2026-08-04", "title": "Record submitted", "summary": "Submission.", "document_id": 101, "references": [reference]}
        class Unsupported(ExactAuditor):
            async def audit_answer_units(self, *args):
                result = await super().audit_answer_units(*args)
                result["assessments"][0]["status"] = "unsupported"
                return result
        reasons = {}
        result = await validate_timeline([event, {**event, "date": "2026-02-30"}, {**event, "date": "2026-08-05"}],
                                         evidence, Unsupported(), "When was it submitted?", diagnostics=reasons)
        self.assertEqual(result, [])
        self.assertEqual(reasons, {"unsupported_event": 1, "invalid_date": 1, "unsupported_date": 1})

    async def test_public_finalizer_preserves_original_written_date_quote(self):
        source = "On September 1, 2026, the monthly service charge became $321 USD."
        result = await AnswerFinalizer(ExactAuditor()).finalize(
            "What changed?", "The monthly service charge became $321 USD on 2026-09-01.", pack(source))
        self.assertEqual(result["finalization"]["disposition"], "supported")
        reference = result["claim_ledger"]["claims"][0]["references"][0]
        self.assertEqual(reference["quote"], source)
        self.assertEqual(source[reference["start"]:reference["end"]], source)

    async def test_timeline_accepts_written_date_before_and_after_audit(self):
        source = "Service change effective September 1, 2026."
        evidence = pack(source)
        span = evidence_spans(evidence)[0]
        reference = {"span_id": span["span_id"], "evidence_id": span["evidence_id"], "document_id": 101, "quote": source}
        auditor = ExactAuditor()
        result = await validate_timeline([{"date": "2026-09-01", "title": "Service change", "summary": "Service changed.",
            "document_id": 101, "references": [reference]}], evidence, auditor, "How did service change?")
        self.assertEqual(len(result), 1)
        self.assertEqual(auditor.calls, 1)
        self.assertEqual(result[0]["references"][0]["quote"], source)
