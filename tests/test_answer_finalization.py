import unittest
import asyncio
import copy

from app.answer_finalization import AnswerFinalizer, answer_units, values_match


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

    async def test_markdown_structure_keeps_claim_context_without_formatting_claims(self):
        answer = "### Recorded premium\n---\n**Monthly premium:**\nThe premium is $321.00 USD."
        class ContextAuditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                raw = await super().audit_answer_units(question, units, spans, plan)
                for unit, assessment in zip(units, raw["assessments"]):
                    if "The premium is" not in unit["text"]:
                        assessment.update(status="unsupported", references=[])
                return raw
        for label in ("**Monthly premium:**", "**Monthly premium**:", "Monthly premium:"):
            variant = answer.replace("**Monthly premium:**", label)
            result = await AnswerFinalizer(ContextAuditor()).finalize("Recorded premium?", variant, PACK)
            self.assertEqual(result["finalization"]["disposition"], "supported", label)
            claim = result["claim_ledger"]["claims"][0]
            self.assertEqual(variant[claim["start"]:claim["end"]], claim["claim"])
        result = await AnswerFinalizer(ContextAuditor()).finalize("Recorded premium?", answer, PACK)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        claims = result["claim_ledger"]["claims"]
        self.assertEqual(len(claims), 1)
        self.assertIn("Recorded premium", claims[0]["claim"])
        self.assertEqual(answer[claims[0]["start"]:claims[0]["end"]], claims[0]["claim"])
        for unsafe in (answer.replace("Recorded premium", "Deductible $999"),
                       answer + "\n### Annual premium $999", answer + "\n--- $999",
                       "### 999.", "### 2025. Renewal premium\nThe premium is $321.00 USD."):
            rejected = await AnswerFinalizer(ContextAuditor()).finalize("Recorded premium?", unsafe, PACK)
            self.assertNotEqual(rejected["finalization"]["disposition"], "supported")

    def test_number_labels_and_abbreviations_stay_with_their_values(self):
        answer = "1. Policy No. `12345` (Membership No. `67890`). A second fact."
        units = answer_units(answer)
        self.assertEqual([u["text"] for u in units],
                         ["1. Policy No. `12345` (Membership No. `67890`).", "A second fact."])
        for unit in units:
            self.assertEqual(answer[unit["start"]:unit["end"]], unit["text"])
        self.assertFalse(values_match("### Premium $999", [{"quote": "$321.00"}]))
        self.assertEqual(answer_units("---\n***\n___"), [])

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

    async def test_mixed_batch_keeps_source_for_each_assertion(self):
        from app.evidence import build_evidence_pack
        question = "Which records are documented?"
        facts = ["Alpha coverage records a red truck.",
                 "Alpha coverage records a blue car.",
                 "Alpha coverage records a green van.",
                 "Beta declaration records policy 7654321."]
        crowded = " ".join(facts[:3]) + " ordinary source text" * 185
        chunks = [{"document_id": 101, "chunk_index": i, "title": "Alpha records", "content": crowded}
                  for i in range(12)]
        chunks.append({"document_id": 202, "chunk_index": 0, "title": "Beta declaration", "content": facts[3] + " archived attachment" * 170})
        evidence = build_evidence_pack(question, {}, chunks, [])
        seen = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                seen.append(spans)
                assessments = []
                for unit in units:
                    source = next((s for s in spans if unit["text"] in s["content"]), None)
                    assessments.append({"unit_id": unit["id"], "status": "supported" if source else "missing",
                        "temporal_scope": "historical", "references": [{"span_id": source["span_id"],
                        "evidence_id": source["evidence_id"], "document_id": source["document_id"],
                        "quote": unit["text"]}] if source else []})
                return {"assessments": assessments}
        for order in (facts, list(reversed(facts))):
            result = await AnswerFinalizer(Auditor()).finalize(question, "\n".join(order), evidence)
            self.assertEqual(result["finalization"]["disposition"], "supported")
            self.assertEqual(result["claim_ledger"]["summary"]["supported"], 4)
            self.assertEqual(set(result["finalization"]["cited_document_ids"]), {101, 202})
            from app.answer_finalization import evidence_spans
            self.assertEqual(seen[-1], evidence_spans(evidence, citation_safe=True))
            self.assertEqual(len({s["span_id"] for s in seen[-1]}), len(seen[-1]))

    async def test_plain_field_label_quote_maps_to_original_bold_ocr(self):
        source = "Preface. **Policy Number:** ZX123.\n**Policy Period:** From 2026 to 2027."
        evidence = {"items": [{"id": "record", "document_id": 101, "chunk_index": 0,
                              "source_kind": "ocr", "title": "Declaration", "content": source}]}
        class Auditor:
            def __init__(self, quote): self.quote = quote
            async def audit_answer_units(self, question, units, spans, plan):
                return {"assessments": [{"unit_id": u["id"], "status": "supported", "temporal_scope": "historical",
                    "references": [{"span_id": spans[0]["span_id"], "evidence_id": "record", "document_id": 101,
                                    "quote": self.quote}]} for u in units]}
        quote = "Policy Number: ZX123. Policy Period: From 2026 to 2027."
        result = await AnswerFinalizer(Auditor(quote)).finalize(
            "What declaration is recorded?", "Policy ZX123 has a recorded term from 2026 to 2027.", evidence)
        self.assertTrue(result["finalization"]["complete"])
        ref = result["claim_ledger"]["claims"][0]["references"][0]
        self.assertEqual(ref["quote"], source[len("Preface. "):])
        self.assertEqual(source[ref["start"]:ref["end"]], ref["quote"])
        for bad in (quote.replace("2027", "2028"), quote.replace("Period:", "Period"),
                    quote.replace("ZX123.", "ZX123. Invented text.")):
            result = await AnswerFinalizer(Auditor(bad)).finalize(
                "What declaration is recorded?", "Policy ZX123 has a recorded term from 2026 to 2027.", evidence)
            self.assertFalse(result["finalization"]["complete"])
        for unsupported_source in (source.replace("**Policy Number:**", "**Policy Number:"),
                                   source.replace("**Policy Number:**", "~~Policy Number:~~"),
                                   source.replace("**Policy Number:**", "**Policy 9 Number:**")):
            other = {"items": [{**evidence["items"][0], "content": unsupported_source}]}
            result = await AnswerFinalizer(Auditor(quote)).finalize(
                "What declaration is recorded?", "Policy ZX123 has a recorded term from 2026 to 2027.", other)
            self.assertFalse(result["finalization"]["complete"])

    async def test_field_label_fallback_respects_window_context_and_escapes(self):
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                source = max(spans, key=lambda s: s["start"])
                return {"assessments": [{"unit_id": u["id"], "status": "supported", "references": [{
                    "span_id": source["span_id"], "evidence_id": source["evidence_id"],
                    "document_id": 101, "quote": "Policy: ZX123."}]} for u in units]}
        for prefix in ("X", "*", "\\", "é", "e\u0301"):
            for padding in ("", " " * 3799):
                source = padding + prefix + "**Policy:** ZX123."
                evidence = {"items": [{"id": "record", "document_id": 101, "chunk_index": 0,
                                      "title": "Record", "source_kind": "ocr", "content": source}]}
                result = await AnswerFinalizer(Auditor()).finalize(
                    "Which policy is recorded?", "The recorded policy is ZX123.", evidence)
                self.assertFalse(result["finalization"]["complete"], (prefix, len(padding)))

    async def test_field_label_match_skips_an_outside_prefix_occurrence(self):
        source = "x" * 3798 + "**Policy:** ZX123.\nAn intervening line.\n**Policy:** ZX123."
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                selected = max(spans, key=lambda s: s["start"])
                return {"assessments": [{"unit_id": u["id"], "status": "supported", "references": [{
                    "span_id": selected["span_id"], "evidence_id": "record", "document_id": 101,
                    "quote": "Policy: ZX123."}]} for u in units]}
        evidence = {"items": [{"id": "record", "document_id": 101, "chunk_index": 0,
                              "title": "Record", "source_kind": "ocr", "content": source}]}
        result = await AnswerFinalizer(Auditor()).finalize(
            "Which policy is recorded?", "The recorded policy is ZX123.", evidence)
        self.assertTrue(result["finalization"]["complete"])
        ref = result["claim_ledger"]["claims"][0]["references"][0]
        self.assertEqual(ref["start"], source.rindex("**Policy:"))
        self.assertEqual(source[ref["start"]:ref["end"]], "**Policy:** ZX123.")

    async def test_partial_label_cannot_borrow_an_outside_bold_wrapper(self):
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                selected = max(spans, key=lambda s: s["start"])
                return {"assessments": [{"unit_id": u["id"], "status": "supported", "references": [{
                    "span_id": selected["span_id"], "evidence_id": "record", "document_id": 101,
                    "quote": "Number: ZX123."}]} for u in units]}
        for prefix in ("x", "*", "\\"):
            source = " " * 3797 + prefix + "**Policy Number:** ZX123."
            evidence = {"items": [{"id": "record", "document_id": 101, "chunk_index": 0,
                                  "title": "Record", "source_kind": "ocr", "content": source}]}
            result = await AnswerFinalizer(Auditor()).finalize(
                "Which policy is recorded?", "The recorded policy is ZX123.", evidence)
            self.assertFalse(result["finalization"]["complete"], prefix)

    async def test_quote_cannot_truncate_a_combining_character(self):
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                return {"assessments": [{"unit_id": u["id"], "status": "supported", "references": [{
                    "span_id": spans[0]["span_id"], "evidence_id": "record", "document_id": 101,
                    "quote": "Jose"}]} for u in units]}
        evidence = {"items": [{"id": "record", "document_id": 101, "chunk_index": 0,
                              "title": "Record", "source_kind": "ocr", "content": "Jose\u0301"}]}
        result = await AnswerFinalizer(Auditor()).finalize("Who is recorded?", "Jose", evidence)
        self.assertFalse(result["finalization"]["complete"])

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
        self.assertEqual(result["finalization"]["disposition"], "partial")
        self.assertEqual(result["verification"]["partial"]["original_total"], 41)
        self.assertEqual(result["claim_ledger"]["summary"]["total"], 40)
        self.assertNotIn("999", result["answer"])

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
        self.assertEqual(result["finalization"]["disposition"], "partial")
        self.assertFalse(result["finalization"]["complete"])
        self.assertEqual(result["claim_ledger"]["summary"]["total"], 1)
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
        self.assertEqual(result["finalization"]["disposition"], "incomplete")


if __name__ == "__main__":
    unittest.main()


class RepairDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_legacy_repair_has_safe_diagnostic(self):
        for response, reason in ((None, 'transport_unavailable'), ({}, 'invalid_object'),
                                 ({'answer': []}, 'invalid_object')):
            with self.subTest(response=response):
                class Repair:
                    async def repair_answer(self, *args):
                        return response
                result = await AnswerFinalizer(SupportedAuditor(), Repair()).finalize(
                    'Premium?', '$123.00 USD.', PACK)
                self.assertEqual(result['finalization']['disposition'], 'audit_failed')
                self.assertEqual(result['finalization']['repair_diagnostic'], {'reason': reason})
                self.assertFalse(result['finalization']['answer_verified'])

    async def test_editor_outer_timeout_has_safe_diagnostic(self):
        class Repair:
            async def repair_answer(self, *args):
                await asyncio.sleep(10)
        result = await AnswerFinalizer(SupportedAuditor(), Repair(), timeout_seconds=.01).finalize(
            'Premium?', '$123.00 USD.', PACK)
        self.assertEqual(result['finalization']['disposition'], 'timeout')
        self.assertEqual(result['finalization']['repair_diagnostic'], {'reason': 'transport_unavailable'})
        self.assertFalse(result['finalization']['answer_verified'])

    async def test_repair_receives_independent_value_and_reference_rejection_reasons(self):
        for candidate, invalid_reference, expected in (
                ("The premium is $999.00 USD.", False, "value_mismatch"),
                ("The premium is $321.00 USD.", True, "invalid_reference")):
            class Auditor(SupportedAuditor):
                async def audit_answer_units(self, *args):
                    result = await super().audit_answer_units(*args)
                    if invalid_reference:
                        result["assessments"][0]["references"].append({"span_id": "unknown"})
                    return result
            class Repair:
                async def repair_answer(inner, question, answer, evidence, verification):
                    self.assertIn(expected, verification["claims"][0]["rejection_reasons"])
                    return None
            result = await AnswerFinalizer(Auditor(), Repair()).finalize("Premium?", candidate, PACK)
            self.assertEqual(result["finalization"]["disposition"], "audit_failed")
            self.assertIn(expected, result["claim_ledger"]["claims"][0]["rejection_reasons"])
