"""Adversarial provider outputs exercised through finalization and timeline interfaces."""

import asyncio
import hashlib
import unittest

from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.timeline import validate_timeline


def pack(content):
    return {"items": [{"id": "e1", "document_id": 101, "title": "Synthetic record", "chunk_index": 0, "content": content}]}


class QuoteAuditor:
    def __init__(self, quote, **assessment):
        self.quote = quote
        self.assessment = assessment

    async def audit_answer_units(self, question, units, spans, plan):
        span = next(span for span in spans if self.quote in span["content"])
        return {"assessments": [{"unit_id": unit["id"], "status": "supported", **self.assessment,
                                 "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                                 "document_id": span["document_id"], "quote": self.quote}]} for unit in units]}


class AnswerHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_quote_cannot_cut_digits_or_negative_sign_from_source_value(self):
        for source in ["Balance: 4321 USD.", "Balance: -321 USD.", "Balance: 321.50 USD."]:
            with self.subTest(source=source):
                result = await AnswerFinalizer(QuoteAuditor("321")).finalize("What balance?", "The balance is 321.", pack(source))
                self.assertNotEqual(result["finalization"]["disposition"], "supported")

    async def test_quote_cannot_cut_numeric_value_at_a_selected_span_edge(self):
        source = "x" * 3996 + " 321.50 USD."
        result = await AnswerFinalizer(QuoteAuditor("321")).finalize("Balance?", "Balance: 321.", pack(source))
        self.assertFalse(result["finalization"]["complete"])

    async def test_value_and_unit_must_cooccur_in_support_not_separately(self):
        source = "Dose: 5 mg. Body weight: 90 kg."
        finalizer = AnswerFinalizer(QuoteAuditor(source))
        incorrect = await finalizer.finalize("Dose?", "The dose is 90 mg.", pack(source))
        self.assertNotEqual(incorrect["finalization"]["disposition"], "supported")
        correct = await finalizer.finalize("Dose?", "The dose is 5 mg.", pack(source))
        self.assertEqual(correct["finalization"]["disposition"], "supported")

    async def test_unit_outside_original_small_registry_is_checked(self):
        result = await AnswerFinalizer(QuoteAuditor("Volume: 5 mL.")).finalize("Volume?", "The volume is 5 L.", pack("Volume: 5 mL."))
        self.assertNotEqual(result["finalization"]["disposition"], "supported")

    async def test_impossible_iso_date_is_not_certified_even_if_source_contains_it(self):
        result = await AnswerFinalizer(QuoteAuditor("Meeting: 2024-02-30.")).finalize("Meeting date?", "The meeting date is 2024-02-30.", pack("Meeting: 2024-02-30."))
        self.assertNotEqual(result["finalization"]["disposition"], "supported")

    async def test_conflicting_duplicate_evidence_identity_does_not_pick_first(self):
        evidence = pack("Premium: 321 USD.")
        evidence["items"].append({**evidence["items"][0], "content": "Premium: 999 USD."})
        result = await AnswerFinalizer(QuoteAuditor("Premium: 321 USD.")).finalize("Premium?", "Premium: 321 USD.", evidence)
        self.assertFalse(result["finalization"]["complete"])

    async def test_repair_timeout_cannot_attach_prior_ledger_to_new_revision(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": "Premium: 321 USD."}
        class SecondCallTimesOut(QuoteAuditor):
            calls = 0
            async def audit_answer_units(self, *args):
                self.calls += 1
                if self.calls == 2:
                    await asyncio.sleep(10)
                return await super().audit_answer_units(*args)
        result = await AnswerFinalizer(SecondCallTimesOut("Premium: 321 USD."), Repair(), timeout_seconds=0.01).finalize("Premium?", "Premium: 123 USD.", pack("Premium: 321 USD."))
        expected = hashlib.sha256(b"Premium: 321 USD.").hexdigest()
        self.assertEqual(result["finalization"]["disposition"], "timeout")
        self.assertEqual(result["claim_ledger"].get("candidate_digest"), expected)
        self.assertEqual(result["claim_ledger"]["claims"], [])

    async def test_unscoped_current_assertion_is_not_relabelled_historical(self):
        result = await AnswerFinalizer(QuoteAuditor("Your policy covers you.", temporal_scope="none")).finalize("Am I insured?", "Your policy covers you.", pack("Your policy covers you."), plan={"requires_current": True})
        self.assertEqual(result["finalization"]["disposition"], "current_unresolved")

    async def test_planner_cannot_disable_current_claim_gate(self):
        for question, answer, scope in [
            ("What is recorded?", "Your policy covers you.", "current"),
            ("Current coverage?", "Your policy covers you.", "none"),
            ("What is recorded?", "Your policy currently covers you.", "none"),
        ]:
            with self.subTest(question=question, scope=scope):
                source = answer
                result = await AnswerFinalizer(QuoteAuditor(source, temporal_scope=scope)).finalize(question, answer, pack(source), plan={"requires_current": False})
                self.assertEqual(result["finalization"]["disposition"], "current_unresolved")
                self.assertTrue(result["current_state"]["required"])

    async def test_malformed_temporal_scope_returns_terminal_result(self):
        result = await AnswerFinalizer(QuoteAuditor("Premium: 321 USD.", temporal_scope=[])).finalize("Current premium?", "Premium: 321 USD.", pack("Premium: 321 USD."), plan={"requires_current": True})
        self.assertFalse(result["finalization"]["complete"])

    async def test_model_document_link_is_replaced_by_validated_document_citation(self):
        result = await AnswerFinalizer(QuoteAuditor("Premium: 321 USD.")).finalize("Premium?", "Premium: 321 USD. [Document 999](/documents/999)", pack("Premium: 321 USD."))
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertNotIn("999", result["answer"])
        self.assertIn("[Document 101](/documents/101)", result["answer"])


class TimelineHardeningTests(unittest.IsolatedAsyncioTestCase):
    def event(self, evidence, date="2024-02-29", quote=None):
        span = evidence_spans(evidence)[0]
        return {"date": date, "document_id": 101, "title": "Meeting", "summary": "Recorded meeting date.",
                "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"], "document_id": 101,
                                "quote": quote or span["content"]}]}

    async def test_partial_date_cannot_hide_invalid_full_source_date(self):
        evidence = pack("Meeting: 2024-02-30.")
        event = self.event(evidence, date="2024-02")
        result = await validate_timeline([event], evidence, QuoteAuditor(evidence["items"][0]["content"]), "Meeting?")
        self.assertEqual(result, [])

    async def test_malformed_assessment_records_fail_without_raising(self):
        class Malformed:
            async def audit_answer_units(self, *args):
                return {"assessments": [None]}
        evidence = pack("Meeting: 2024-02-29.")
        result = await validate_timeline([self.event(evidence)], evidence, Malformed(), "Meeting?")
        self.assertEqual(result, [])

    async def test_timeline_references_are_limited_to_extraction_manifest(self):
        evidence = pack("Meeting: 2024-02-29.")
        event = self.event(evidence)
        result = await validate_timeline([event], evidence, QuoteAuditor(evidence["items"][0]["content"]), "Meeting?", manifest=[])
        self.assertEqual(result, [])

    async def test_published_quote_is_the_one_independent_auditor_used(self):
        source = "Meeting: 2024-02-29. Additional source footer."
        evidence = pack(source)
        event = self.event(evidence)
        result = await validate_timeline([event], evidence, QuoteAuditor("Meeting: 2024-02-29."), "Meeting?")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["references"][0]["quote"], "Meeting: 2024-02-29.")

    async def test_other_supplied_source_is_available_for_contradiction_review(self):
        evidence = pack("Meeting: 2024-02-29.")
        evidence["items"].append({"id": "cancellation", "document_id": 102, "title": "Correction", "content": "The meeting of 2024-02-29 was canceled and never occurred."})
        event = self.event(evidence)
        class ContradictionAuditor(QuoteAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                result = await super().audit_answer_units(question, units, spans, plan)
                if any("canceled" in span["content"] for span in spans):
                    result["assessments"][0]["status"] = "conflicting"
                return result
        result = await validate_timeline([event], evidence, ContradictionAuditor("Meeting: 2024-02-29."), "When was the meeting?")
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
