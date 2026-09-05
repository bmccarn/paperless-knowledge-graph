import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, parse_date
from app.timeline import validate_timeline
from app.query_quality import compute_evidence_grade, current_state_summary
from tests.test_answer_finalization import PACK, SupportedAuditor


class TemporalTests(unittest.IsolatedAsyncioTestCase):
    def test_calendar_validation_preserves_precision(self):
        self.assertEqual(parse_date("2024-02-29"), ("2024-02-29", "day"))
        self.assertEqual(parse_date("2026-02"), ("2026-02", "month"))
        self.assertEqual(parse_date("2026"), ("2026", "year"))
        for value in ("2026-02-30", "2026-13", "2026-00-10", "0000", "yesterday"):
            self.assertIsNone(parse_date(value), value)

    def test_any_dated_source_does_not_resolve_current_state(self):
        for date in ("2001-01-01", "2026-09-04", "2099-01-01", "2026-02-30"):
            summary = current_state_summary({"requires_current": True}, [{"date": date}])
            self.assertEqual(summary["status"], "needs_review")

    def test_coarse_verified_status_cannot_override_unsupported_ledger(self):
        grade = compute_evidence_grade("Premium?", {}, [{"title": "Premium", "date": "2026-09-04"}] * 10,
                                       {"graph_nodes": [1]}, {"status": "verified"},
                                       {"coverage": {"average_source_quality": 1, "structured_fact_count": 99}},
                                       {"summary": {"supported": 0, "unsupported": 1}})
        self.assertEqual(grade["dimensions"]["claim_support"], 0)
        self.assertNotEqual(grade["level"], "high")

    async def test_historical_answer_can_be_qualified_without_claiming_current_value(self):
        class Historical(SupportedAuditor):
            async def audit_answer_units(self, *args):
                raw = await super().audit_answer_units(*args)
                for a in raw["assessments"]:
                    a["temporal_scope"] = "historical"
                return raw
        result = await AnswerFinalizer(Historical()).finalize(
            "Current premium?", "The recorded monthly premium was $321.00 USD.", PACK,
            plan={"requires_current": True}, evaluated_at="2026-09-04")
        self.assertEqual(result["finalization"]["disposition"], "qualified")
        self.assertIn("$321.00 USD", result["answer"])
        self.assertIn("not established", result["answer"])
        self.assertEqual(result["evidence"]["level"], "medium")

    async def test_current_claim_is_not_certified_by_an_explicit_active_term_alone(self):
        pack = {"items": [{"id": "term", "document_id": 101, "title": "Term", "content":
                           "Effective: 2026-01-01. Expires: 2026-12-31. Monthly premium: $321.00 USD."}]}
        result = await AnswerFinalizer(SupportedAuditor()).finalize(
            "Current premium?", "My current premium is $321.00 USD.", pack,
            plan={"requires_current": True}, evaluated_at="2026-09-04")
        self.assertNotEqual(result["verification"]["status"], "verified")
        self.assertEqual(result["current_state"]["active_documented_interval_ids"], ["term"])

    async def test_timeline_accepts_sourced_event_and_rejects_unknown_source_and_invalid_date(self):
        pack = {"items": [{"id": "move", "document_id": 88, "title": "Move record", "content": "Moved to Durham in 2024-02."}]}
        span = evidence_spans(pack)[0]
        ref = {"span_id": span["span_id"], "evidence_id": "move", "document_id": 88, "quote": span["content"]}
        class Auditor:
            async def audit_answer_units(self, *args):
                return {"assessments": [{"unit_id": "event", "status": "supported", "references": [ref]}]}
        event = {"date": "2024-02", "title": "Moved to Durham", "summary": "", "document_id": 88, "references": [ref]}
        events = await validate_timeline([event, {**event, "date": "2024-02-30"}, {**event, "document_id": 999}], pack, Auditor(), "When did I move?")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["precision"], "month")
        self.assertEqual(events[0]["date"], "2024-02")


if __name__ == "__main__":
    unittest.main()
