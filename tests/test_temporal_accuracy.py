import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, parse_date
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

    async def test_current_status_failure_can_repair_to_dated_observations(self):
        class Auditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, *args):
                raw = await super().audit_answer_units(question, units, *args)
                for unit, assessment in zip(units, raw["assessments"]):
                    assessment["temporal_scope"] = "current" if "current" in unit["text"] else "historical"
                return raw
        for replacement, expected in (("The statement records a premium of $321.00 USD.", "qualified"),
                                      ("The current premium is $321.00 USD.", "current_unresolved")):
            class Repair:
                async def repair_answer(inner, question, answer, evidence, verification):
                    self.assertEqual(verification["status"], "current_unresolved")
                    return {"answer": replacement}
            result = await AnswerFinalizer(Auditor(), Repair()).finalize(
                "Current premium?", "My current premium is $321.00 USD.", PACK,
                evaluated_at="2026-09-07")
            self.assertEqual(result["finalization"]["disposition"], expected)
            self.assertEqual(result["finalization"]["attempts"], 2)

    async def test_dated_answer_context_reaches_every_batch_without_certifying_new_values(self):
        class ContextAuditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                raw = await super().audit_answer_units(question, units, spans, plan)
                recorded = "The dated statements record these premiums" in plan.get("answer_context", "")
                for assessment in raw["assessments"]:
                    assessment["temporal_scope"] = "historical" if recorded else "current"
                return raw
        candidate = "The dated statements record these premiums:\n" + "\n".join(["Premium: $321.00 USD."] * 8)
        result = await AnswerFinalizer(ContextAuditor()).finalize("Current premium?", candidate, PACK)
        self.assertEqual(result["finalization"]["disposition"], "qualified")
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 8)
        for unsafe in (candidate + "\nThis is the current premium.", candidate + "\nPremium: $999.00 USD."):
            rejected = await AnswerFinalizer(ContextAuditor()).finalize("Current premium?", unsafe, PACK)
            self.assertFalse(rejected["finalization"]["complete"])



if __name__ == "__main__":
    unittest.main()
