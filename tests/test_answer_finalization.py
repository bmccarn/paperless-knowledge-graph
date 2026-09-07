import unittest
import asyncio
import copy

from app.answer_finalization import AnswerFinalizer


PACK = {"items": [{"id": "policy-jan-v1", "document_id": 101, "chunk_index": 0,
                   "title": "January statement", "content": "Monthly premium: $321.00 USD.",
                   "excerpt": "Monthly premium: $321.00 USD."}], "coverage": {}}


class SupportedAuditor:
    async def audit_answer_units(self, question, units, spans, plan):
        return {"assessments": [{"unit_id": unit["id"], "status": "supported", "references": [{
            "span_id": spans[0]["span_id"], "evidence_id": "policy-jan-v1", "document_id": 101,
            "quote": "Monthly premium: $321.00 USD."}]} for unit in units]}


class AnswerFinalizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_supported_answer_keeps_its_value_and_resolvable_source(self):
        result = await AnswerFinalizer(SupportedAuditor()).finalize(
            "What premium is recorded?", "The recorded monthly premium is $321.00 USD.", PACK,
            plan={"requires_current": False}, mode="strict", evaluated_at="2026-09-04")
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertIn("$321.00 USD", result["answer"])
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 1)
        self.assertEqual(result["claim_ledger"]["claims"][0]["document_id"], 101)
        self.assertEqual(result["verification"]["status"], "verified")

    async def test_requested_short_source_reaches_auditor_among_long_distractors(self):
        from app.evidence import build_evidence_pack
        question = ('For Paperless ID 101, describe the invoice subject using only this document '
                    'and an exact source quotation.')
        content = "For the document describe the invoice subject using only an exact source quotation. " * 45
        chunks = [{"document_id": 101, "title": "Parcel Invoice", "content": "Monthly premium: $321.00 USD."}]
        chunks += [{"document_id": i, "title": "Unrelated notice", "content": content} for i in range(200, 225)]
        pack = build_evidence_pack(question, {}, chunks, [])
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                source = next((s for s in spans if s["document_id"] == 101), None)
                return {"assessments": [{"unit_id": u["id"], "status": "supported" if source else "missing",
                    "references": [{"span_id": source["span_id"], "evidence_id": source["evidence_id"],
                                    "document_id": 101, "quote": source["content"]}] if source else []}
                    for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize(question, "Monthly premium: $321.00 USD.", pack)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(result["finalization"]["cited_document_ids"], [101])

    async def test_wrong_digit_and_unit_fail_even_when_auditor_says_supported(self):
        for answer in ("Monthly premium is $312.00 USD.", "Monthly premium is €321.00 EUR."):
            with self.subTest(answer=answer):
                result = await AnswerFinalizer(SupportedAuditor()).finalize("Premium?", answer, PACK)
                self.assertEqual(result["finalization"]["disposition"], "unsupported")
                self.assertNotIn("321", result["answer"])
                self.assertNotIn("312", result["answer"])
                self.assertEqual(result["evidence"]["score"], 0)

    async def test_fabricated_document_quote_and_evidence_id_are_rejected(self):
        for field, value in (("document_id", 999), ("quote", "$999.00"),
                             ("evidence_id", "invented"), ("span_id", "invented")):
            class Auditor(SupportedAuditor):
                async def audit_answer_units(self, *args):
                    raw = await super().audit_answer_units(*args)
                    raw["assessments"][0]["references"][0][field] = value
                    return raw
            with self.subTest(field=field):
                result = await AnswerFinalizer(Auditor()).finalize("Premium?", "$321.00 USD.", PACK)
                self.assertEqual(result["finalization"]["disposition"], "unsupported")

    async def test_missing_unit_or_duplicate_assessment_prevents_complete_verdict(self):
        for duplicate in (False, True):
            class Auditor(SupportedAuditor):
                async def audit_answer_units(self, *args):
                    raw = await super().audit_answer_units(*args)
                    raw["assessments"] = [raw["assessments"][0]] * (2 if duplicate else 1)
                    return raw
            result = await AnswerFinalizer(Auditor()).finalize(
                "Premium?", "Monthly premium is $321.00 USD. It renews automatically.", PACK)
            self.assertEqual(result["finalization"]["disposition"], "incomplete")

    async def test_tail_evidence_and_answer_are_audited(self):
        pack = copy.deepcopy(PACK)
        pack["items"][0]["content"] = "irrelevant filler " * 1500 + "Monthly premium: $321.00 USD."
        class Auditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                raw = await super().audit_answer_units(question, units, spans, plan)
                for a in raw["assessments"]:
                    source = next(s for s in spans if "Monthly premium:" in s["content"])
                    a["references"][0]["span_id"] = source["span_id"]
                return raw
        result = await AnswerFinalizer(Auditor()).finalize("Premium?", "$321.00 USD.", pack)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertGreater(result["claim_ledger"]["claims"][0]["references"][0]["start"], 18000)
        answer = "Monthly premium: $321.00 USD. " * 40 + "The deductible is $999.00 USD."
        result = await AnswerFinalizer(SupportedAuditor()).finalize("Premium?", answer, PACK)
        self.assertEqual(result["finalization"]["disposition"], "unsupported")
        self.assertEqual(result["claim_ledger"]["summary"]["total"], 41)

    async def test_timeout_empty_evidence_and_provider_failure_do_not_keep_draft(self):
        class Slow:
            async def audit_answer_units(self, *args):
                await asyncio.sleep(10)
        class Broken:
            async def audit_answer_units(self, *args):
                raise RuntimeError("provider failed")
        for auditor, pack, expected in ((Slow(), PACK, "timeout"), (Broken(), PACK, "audit_failed"),
                                        (SupportedAuditor(), {"items": []}, "incomplete")):
            result = await AnswerFinalizer(auditor, timeout_seconds=0.01).finalize("Premium?", "$321.00 USD.", pack)
            self.assertEqual(result["finalization"]["disposition"], expected)
            self.assertNotIn("321", result["answer"])

    async def test_repair_is_reaudited_and_cannot_introduce_new_unsupported_fact(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": "Monthly premium: $321.00 USD. Deductible: $999.00 USD."}
        result = await AnswerFinalizer(SupportedAuditor(), Repair()).finalize("Premium?", "$123.00 USD.", PACK)
        self.assertEqual(result["finalization"]["attempts"], 2)
        self.assertEqual(result["finalization"]["disposition"], "unsupported")
        self.assertNotIn("999", result["answer"])

    async def test_supported_repair_is_accepted(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": "Monthly premium: $321.00 USD."}
        result = await AnswerFinalizer(SupportedAuditor(), Repair()).finalize("Premium?", "$123.00 USD.", PACK)
        self.assertEqual(result["finalization"]["attempts"], 2)
        self.assertEqual(result["finalization"]["disposition"], "supported")

    async def test_budget_overflow_is_explicit(self):
        result = await AnswerFinalizer(SupportedAuditor(), max_units=1).finalize("Premium?", "One. Two.", PACK)
        self.assertEqual(result["finalization"]["disposition"], "incomplete")
        self.assertFalse(result["claim_ledger"]["complete"])

    async def test_flagged_source_cannot_certify_derived_fact(self):
        pack = copy.deepcopy(PACK)
        pack["items"][0]["feedback_open"] = True
        result = await AnswerFinalizer(SupportedAuditor()).finalize("Premium?", "$321.00 USD.", pack)
        self.assertEqual(result["finalization"]["disposition"], "unsupported")


if __name__ == "__main__":
    unittest.main()
