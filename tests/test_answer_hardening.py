"""Adversarial provider outputs exercised through finalization and timeline interfaces."""

import asyncio
import hashlib
import unittest

from app.answer_finalization import AnswerFinalizer, evidence_spans


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
        for amount, unit in (("{}", "mg"), ("**{}**", "mg"), ("{}", "**mg**"),
                             ("*{}*", "mg"), ("{}", "*mg*"), ("`{}`", "mg"),
                             ("{}", "`mg`"), ("``{}``", "mg"), ("{}", "``mg``"),
                             ("```{}```", "mg"), ("{}", "```mg```"),
                             ("__{}__", "mg"), ("{}", "_mg_"),
                             ("****{}****", "mg"), ("{}", "****mg****"),
                             ("____{}____", "mg"), ("{}", "____mg____"),
                             ("[{}]", "mg"), ("{}", "[mg]"),
                             ("**{}* *", "mg**")):
            with self.subTest(amount=amount, unit=unit):
                incorrect = await finalizer.finalize("Dose?", f"The dose is {amount.format(90)} {unit}.", pack(source))
                self.assertFalse(incorrect["finalization"]["complete"])
                correct = await finalizer.finalize("Dose?", f"The dose is {amount.format(5)} {unit}.", pack(source))
                self.assertEqual(correct["finalization"]["disposition"], "supported")
                claim = correct["claim_ledger"]["claims"][0]
                self.assertEqual(claim["references"][0]["quote"], source)
                self.assertEqual(correct["answer"][claim["start"]:claim["end"]], claim["claim"])

    async def test_quantity_checks_span_markdown_and_audit_unit_boundaries(self):
        source = "Dose: 5 mg. Body weight: 90 kg."
        finalizer = AnswerFinalizer(QuoteAuditor(source))
        for template in ("**The dose is\n{}** mg.", "The dose is {}\nmg.",
                         "**The dose is\n*{}*\nmg.**",
                         "1. Dose:\n{} mg.\n2. Weight: 90 kg."):
            with self.subTest(template=template):
                incorrect = await finalizer.finalize("Dose?", template.format(90), pack(source))
                self.assertFalse(incorrect["finalization"]["complete"])
                correct = await finalizer.finalize("Dose?", template.format(5), pack(source))
                self.assertEqual(correct["finalization"]["disposition"], "supported")
                for claim in correct["claim_ledger"]["claims"]:
                    self.assertEqual(claim["references"][0]["quote"], source)
                    self.assertEqual(template.format(5)[claim["start"]:claim["end"]], claim["claim"])

    async def test_link_labels_remain_factual_numeric_prose(self):
        source = "Dose: 5 mg. Body weight: 90 kg."
        finalizer = AnswerFinalizer(QuoteAuditor(source))
        for amount in (90, 5):
            with self.subTest(amount=amount):
                result = await finalizer.finalize(
                    "Dose?", f"Dose: [{amount}](https://example.invalid) mg.", pack(source))
                self.assertEqual(result["finalization"]["complete"], amount == 5)

    async def test_formatted_signed_decimal_cannot_lose_sign_or_precision(self):
        source = "Change: -5.25 mg."
        finalizer = AnswerFinalizer(QuoteAuditor(source))
        for value in ("**5.25**", "**-5.2**", "-**5.25**"):
            with self.subTest(value=value):
                result = await finalizer.finalize("Change?", f"Change: {value} mg.", pack(source))
                self.assertEqual(result["finalization"]["complete"], value == "-**5.25**")

    async def test_separate_references_cannot_manufacture_a_quantity(self):
        class FragmentAuditor:
            def __init__(self, quotes):
                self.quotes = quotes

            async def audit_answer_units(self, question, units, spans, plan):
                references = []
                for quote in self.quotes:
                    span = next(span for span in spans if quote in span["content"])
                    references.append({"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                       "document_id": span["document_id"], "quote": quote})
                return {"assessments": [{"unit_id": unit["id"], "status": "supported",
                                         "references": references} for unit in units]}

        same_document = pack("Dose: 5 mg. Body weight: 90 kg.")
        different_documents = pack("Body weight: 90 kg.")
        different_documents["items"].append({"id": "e2", "document_id": 102,
                                             "title": "Dose record", "content": "Dose: 5 mg."})
        for evidence in (same_document, different_documents):
            with self.subTest(documents=len(evidence["items"])):
                result = await AnswerFinalizer(FragmentAuditor(["90", "mg"])).finalize(
                    "Dose?", "The dose is 90 mg.", evidence)
                self.assertFalse(result["finalization"]["complete"])
                self.assertEqual(len(result["claim_ledger"]["claims"][0]["references"]), 2)
                correct = await AnswerFinalizer(FragmentAuditor(["5 mg", "90 kg"])).finalize(
                    "Dose and weight?", "Dose: 5 mg; weight: 90 kg.", evidence)
                self.assertTrue(correct["finalization"]["complete"])

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

    async def test_dated_observation_gets_application_owned_current_status_limit(self):
        source = "Statement dated 2026-09-01 records premium: 321 USD."
        finalizer = AnswerFinalizer(QuoteAuditor(source, temporal_scope="historical"))
        result = await finalizer.finalize("What is my current premium?",
            "### Recorded premium\n" + source, pack(source), evaluated_at="2026-09-07")
        self.assertEqual(result["finalization"]["disposition"], "qualified")
        self.assertIn(source, result["answer"])
        self.assertIn("Current status as of 2026-09-07 is not established", result["answer"])
        self.assertTrue(result["claim_ledger"]["complete"])
        positive = await finalizer.finalize("What is my current premium?",
            "### Active premium\n" + source, pack(source), evaluated_at="2026-09-07")
        self.assertEqual(positive["finalization"]["disposition"], "current_unresolved")

    async def test_malformed_temporal_scope_returns_terminal_result(self):
        result = await AnswerFinalizer(QuoteAuditor("Premium: 321 USD.", temporal_scope=[])).finalize("Current premium?", "Premium: 321 USD.", pack("Premium: 321 USD."), plan={"requires_current": True})
        self.assertFalse(result["finalization"]["complete"])

    async def test_model_document_link_cannot_silently_replace_fabricated_attribution(self):
        result = await AnswerFinalizer(QuoteAuditor("Premium: 321 USD.")).finalize("Premium?", "Premium: 321 USD. [Document 999](/documents/999)", pack("Premium: 321 USD."))
        self.assertFalse(result["finalization"]["complete"])
        self.assertNotIn("999", result["answer"])
        self.assertNotIn("[Document", result["answer"])


if __name__ == "__main__":
    unittest.main()
