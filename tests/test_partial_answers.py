"""Public finalization verifies a new subset and never publishes failed revisions."""
import asyncio
import hashlib
import unittest
from app.answer_finalization import AnswerFinalizer
from tests.test_source_dates import pack


SOURCE = "The invoice records a $321 USD service charge. The laboratory records a 5 mg dose."


class MixedAuditor:
    def __init__(self, fail_subset=False, fail_second=False, conflict=False):
        self.contexts = []
        self.fail_subset, self.fail_second, self.conflict = fail_subset, fail_second, conflict

    async def repair_answer(self, question, answer, *args):
        return {"answer": answer}

    async def audit_answer_units(self, question, units, spans, plan):
        self.contexts.append(plan["answer_context"])
        if self.fail_second and len(self.contexts) == 2:
            raise RuntimeError("second audit unavailable")
        result = []
        for unit in units:
            unsupported = "$999" in unit["text"] or "90 mg" in unit["text"]
            status = "conflicting" if unsupported and self.conflict else "unsupported" if unsupported else "supported"
            if self.fail_subset and len(self.contexts) > 1:
                status = "unsupported"
            span = spans[0]
            result.append({"unit_id": unit["id"], "status": status, "temporal_scope": "historical",
                "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                "document_id": 101, "quote": SOURCE}]})
        return {"assessments": result}


class PartialAnswerTests(unittest.IsolatedAsyncioTestCase):
    async def test_supported_subset_has_its_own_ledger_and_explicit_incompleteness(self):
        for answer, omitted in [("The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.", "$999"),
                                ("The laboratory records a 5 mg dose.\nThe previous dose was 90 mg.", "90 mg")]:
            with self.subTest(answer=answer):
                auditor = MixedAuditor()
                result = await AnswerFinalizer(auditor).finalize("What do the records show?", answer, pack(SOURCE))
                self.assertEqual(result["finalization"]["disposition"], "partial")
                self.assertFalse(result["finalization"]["complete"])
                self.assertTrue(result["finalization"]["answer_verified"])
                self.assertNotIn(omitted, result["answer"])
                self.assertIn("partial", result["answer"].lower())
                self.assertGreater(result["evidence"]["score"], 0)
                ledger = result["claim_ledger"]
                self.assertTrue(ledger["complete"])
                self.assertEqual(ledger["summary"]["total"], 1)
                self.assertEqual(ledger["summary"]["supported"], 1)
                self.assertEqual(len(auditor.contexts), 2)
                self.assertNotIn(omitted, auditor.contexts[-1])
                self.assertEqual(ledger["candidate_digest"], hashlib.sha256(auditor.contexts[-1].encode()).hexdigest())
                self.assertEqual(result["verification"]["partial"]["omitted_count"], 1)
                self.assertEqual(result["verification"]["partial"]["original_total"], 2)
                self.assertEqual(result["finalization"]["answer_digest"], hashlib.sha256(result["answer"].encode()).hexdigest())

    async def test_failed_subset_and_conflicting_audit_withhold_answer(self):
        answer = "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD."
        for auditor in [MixedAuditor(fail_subset=True), MixedAuditor(conflict=True)]:
            result = await AnswerFinalizer(auditor).finalize("What do records show?", answer, pack(SOURCE))
            self.assertNotIn("$321", result["answer"])
            self.assertFalse(result["finalization"].get("answer_verified", False))
            self.assertNotEqual(result["finalization"]["disposition"], "partial")

    async def test_failed_final_repair_audit_never_uses_earlier_supported_subset(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": "The invoice records a $321 USD service charge.\nAnother charge is $999 USD."}
        auditor = MixedAuditor(fail_second=True)
        result = await AnswerFinalizer(auditor, Repair()).finalize("What do records show?",
            "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.", pack(SOURCE))
        self.assertEqual(result["finalization"]["disposition"], "audit_failed")
        self.assertEqual(len(auditor.contexts), 2)
        self.assertNotIn("$321", result["answer"])
        self.assertNotIn("partial", result["verification"])

    async def test_repair_timeout_does_not_admit_partial(self):
        class Repair:
            async def repair_answer(self, *args): await asyncio.sleep(10)
        auditor = MixedAuditor()
        result = await AnswerFinalizer(auditor, Repair(), timeout_seconds=0.01).finalize("What do records show?",
            "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.", pack(SOURCE))
        self.assertEqual(result["finalization"]["disposition"], "timeout")
        self.assertEqual(len(auditor.contexts), 1)
        self.assertNotIn("$321", result["answer"])

    async def test_document_relative_latest_has_explicit_audited_comparison_scope(self):
        class Auditor(MixedAuditor):
            async def audit_answer_units(self, *args):
                response = await super().audit_answer_units(*args)
                for assessment in response["assessments"]:
                    assessment.update(temporal_scope="documented", comparison_scope="retrieved_documents", comparison_document_ids=[101])
                return response
        result = await AnswerFinalizer(Auditor()).finalize("What is the latest recorded charge?",
            "The latest retrieved invoice records a $321 USD service charge.", pack(SOURCE), plan={"requires_current": True})
        self.assertEqual(result["finalization"]["disposition"], "qualified")
        self.assertTrue(result["finalization"]["answer_verified"])
        self.assertEqual(result["current_state"]["status"], "documented")

    async def test_invalid_comparison_or_present_world_scope_is_withheld(self):
        for fields in [dict(temporal_scope="current"),
                       dict(temporal_scope="documented", comparison_scope="archive", comparison_document_ids=[101]),
                       dict(temporal_scope="documented", comparison_scope="retrieved_documents", comparison_document_ids=[999]),
                       dict(temporal_scope="documented", comparison_scope="retrieved_documents", comparison_document_ids=[True]),
                       dict(temporal_scope="documented", comparison_scope="retrieved_documents", comparison_document_ids=[])]:
            class Auditor(MixedAuditor):
                async def audit_answer_units(self, *args):
                    result = await super().audit_answer_units(*args)
                    result["assessments"][0].update(fields)
                    return result
            result = await AnswerFinalizer(Auditor()).finalize("Latest charge?", "The latest charge is $321 USD.", pack(SOURCE))
            self.assertEqual(result["finalization"]["disposition"], "current_unresolved")
            self.assertFalse(result["finalization"]["answer_verified"])

    async def test_incomplete_final_repair_and_subset_do_not_publish(self):
        for repair in (False, True):
            class Auditor(MixedAuditor):
                async def audit_answer_units(self, *args):
                    result = await super().audit_answer_units(*args)
                    if len(self.contexts) == 2: result["assessments"] = []
                    return result
                async def repair_answer(self, *args):
                    return {"answer": "The invoice records a $321 USD service charge.\nAnother charge is $999 USD."}
            auditor = Auditor()
            result = await AnswerFinalizer(auditor, auditor if repair else None).finalize("Recorded charges?",
                "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.", pack(SOURCE))
            self.assertNotEqual(result["finalization"]["disposition"], "partial")
            self.assertNotIn("321", result["answer"])
            self.assertEqual(len(auditor.contexts), 2)
