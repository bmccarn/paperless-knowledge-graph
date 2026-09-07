"""Citation metadata must match both supplied evidence and audited references."""
import copy
import hashlib
import subprocess
import sys
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer
from tests.test_answer_hardening import QuoteAuditor

QUOTE = "The listed policy provides liability coverage."
PACK = {"items": [{"id": "policy", "document_id": 101, "title": "Policy 2026 revision 4",
                  "chunk_index": 0, "source_kind": "ocr", "source_content": QUOTE, "content": QUOTE}]}


class AnswerCitationTests(unittest.IsolatedAsyncioTestCase):
    async def test_supported_quote_accepts_verified_attribution_formats(self):
        for attribution in ('[Source: "Policy 2026 revision 4"]', '(Source: "Policy 2026 revision 4")',
                            '(Paperless document 101)', '[Document 101](/documents/101)',
                            '(*Policy 2026 revision 4*, Paperless ID 101)'):
            with self.subTest(attribution=attribution):
                result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize("What is documented?", f'{QUOTE} {attribution}', PACK)
                self.assertEqual(result["finalization"]["disposition"], "supported")
                self.assertEqual(result["finalization"]["cited_document_ids"], [101])
                self.assertNotIn("revision 4", result["answer"])
                ledger = result["claim_ledger"]
                self.assertEqual(ledger["summary"]["total"], 1)
                self.assertEqual(ledger["candidate_digest"], result["finalization"]["candidate_digest"])
                self.assertEqual(ledger["claims"][0]["claim"], QUOTE)

    async def test_wrong_or_malformed_attributions_cannot_be_blessed_by_auditor(self):
        for attribution in ('[Source: "Unknown title"]', '(Source: "Unknown title")',
                            '(Paperless document 999)', '[Document 999](/documents/999)',
                            '[Document 101](/documents/999)', '[Document 101](https://evil.invalid)',
                            '[Source: "Policy 2026 revision 4"; coverage is unlimited]',
                            '[Source: "Policy 2026 revision 4"] coverage is $900000 USD.',
                            '(Paperless document 101; coverage is unlimited)',
                            '[Source: "Unknown title"', '( Source: "Unknown title")',
                            '[Source: "[Policy 2026 revision 4]"]',
                            '[ Source: "Unknown title"](/documents/999)',
                            '[(Source: "Unknown title")](https://example.invalid)',
                            '[[Source: "Policy 2026 revision 4"]]',
                            '[see [Source: "Policy 2026 revision 4"]]',
                            '([Document 101](/documents/101))'):
            with self.subTest(attribution=attribution):
                result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize("What is documented?", f'{QUOTE} {attribution}', PACK)
                self.assertFalse(result["finalization"]["complete"])

    async def test_repair_cannot_hide_an_unknown_source_inside_link_label(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": QUOTE + ' [(Source: "Unknown title")](https://example.invalid)'}
        result = await AnswerFinalizer(QuoteAuditor(QUOTE), Repair()).finalize(
            "What is documented?", QUOTE + ' (Paperless document 999)', PACK)
        self.assertEqual(result["finalization"]["attempts"], 2)
        self.assertFalse(result["finalization"]["complete"])

    async def test_title_link_target_cannot_borrow_a_number_from_supported_prose(self):
        quote = "The recorded premium is 999 USD."
        pack = {"items": [{**PACK["items"][0], "title": "Policy",
                           "content": quote, "source_content": quote}]}
        candidate = quote + ' [Source: "Policy"](/documents/999)'
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": candidate}
        for repair in (None, Repair()):
            result = await AnswerFinalizer(QuoteAuditor(quote), repair).finalize(
                "Recorded premium?", candidate, pack)
            self.assertFalse(result["finalization"]["complete"])

    async def test_title_must_unambiguously_identify_certifying_evidence(self):
        for other in ({**PACK["items"][0], "id": "other", "document_id": 102},
                      {**PACK["items"][0], "source_kind": "generated"}):
            evidence = copy.deepcopy(PACK)
            if other["document_id"] == 102:
                evidence["items"].append(other)
            else:
                evidence["items"] = [other]
            for attribution in ('[Source: "Policy 2026 revision 4"]',
                                '(*Policy 2026 revision 4*, Paperless ID 101)'):
                result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize(
                    "What is documented?", QUOTE + ' ' + attribution, evidence)
                self.assertFalse(result["finalization"]["complete"])

    async def test_document_in_pack_but_not_claim_references_is_not_valid_attribution(self):
        evidence = copy.deepcopy(PACK)
        evidence["items"].append({**PACK["items"][0], "id": "other", "document_id": 102, "title": "Other source"})
        result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize(
            "What is documented?", QUOTE + ' (Paperless document 102)', evidence)
        self.assertFalse(result["finalization"]["complete"])

    async def test_attributions_remain_attached_to_correct_claim_after_link_normalization(self):
        answer = 'The [listed](https://example.invalid) policy provides liability coverage. (Paperless document 101)\n' + QUOTE + ' (Paperless document 102)'
        evidence = copy.deepcopy(PACK)
        evidence["items"].append({**PACK["items"][0], "id": "other", "document_id": 102, "title": "Other source"})
        result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize("What is documented?", answer, evidence)
        self.assertFalse(result["finalization"]["complete"])
        self.assertEqual([c["status"] for c in result["claim_ledger"]["claims"]], ["supported", "unsupported"])

    async def test_repaired_candidate_has_same_citation_validation_and_digest(self):
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": QUOTE + ' [Source: "Policy 2026 revision 4"]'}
        result = await AnswerFinalizer(QuoteAuditor(QUOTE), Repair()).finalize(
            "What is documented?", QUOTE + ' (Paperless document 999)', PACK)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(result["finalization"]["attempts"], 2)
        self.assertEqual(result["claim_ledger"]["candidate_digest"], hashlib.sha256(QUOTE.encode()).hexdigest())

    async def test_paired_title_id_requires_exact_matching_source_and_own_references(self):
        quote = "Policy costs 101 USD; revision 4 was recorded in 2026."
        evidence = {"items": [{**PACK["items"][0], "source_content": quote, "content": quote},
                              {**PACK["items"][0], "id": "other", "document_id": 102,
                               "title": "Other source", "source_content": quote, "content": quote}]}
        for attribution in ("(*Unknown title*, Paperless ID 101)",
                            "(*Policy 2026 revision 4*, Paperless ID 102)",
                            "(*Other source*, Paperless ID 102)",
                            "(*Policy 2026 revision 4; coverage is unlimited*, Paperless ID 101)",
                            "(*Policy 2026 revision 4*, Paperless ID 101; coverage is unlimited)",
                            "[(*Policy 2026 revision 4*, Paperless ID 101)](https://example.invalid)",
                            "((*Policy 2026 revision 4*, Paperless ID 101))",
                            "(*Policy 2026 revision 4*, Paperless ID 101",
                            "(*Policy 2026 revision 4*, Paperless ID 101) costs 999 USD"):
            with self.subTest(attribution=attribution):
                result = await AnswerFinalizer(QuoteAuditor(quote)).finalize(
                    "What is documented?", quote + " " + attribution, evidence)
                self.assertFalse(result["finalization"]["complete"])

    async def test_repair_accepts_paired_source_metadata_without_waiving_factual_numbers(self):
        candidate = QUOTE + ' (*Policy 2026 revision 4*, Paperless ID 101)'
        class Repair:
            async def repair_answer(self, *args):
                return {"answer": candidate}
        result = await AnswerFinalizer(QuoteAuditor(QUOTE), Repair()).finalize(
            "What is documented?", QUOTE + ' (Paperless document 999)', PACK)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(result["finalization"]["attempts"], 2)
        self.assertEqual(result["claim_ledger"]["candidate_digest"], hashlib.sha256(QUOTE.encode()).hexdigest())
        rejected = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize(
            "What is documented?", "The annual premium is 2026 USD. " + candidate, PACK)
        self.assertFalse(rejected["finalization"]["complete"])

    async def test_malformed_paired_boundaries_fail_initial_and_repaired_answers(self):
        quote = "The recorded premium is 999 USD, revision 4 in 2026 for account 101."
        pack = {"items": [{**PACK["items"][0], "source_content": quote, "content": quote}]}
        for attribution in ("(*Unknown title*,\nPaperless ID 999)",
                            "(*Policy 2026 revision 4*,\r\nPaperless ID 999)",
                            "(*Policy 2026 revision 4*, Paperless ID 101))",
                            "(*Policy 2026 revision 4*, Paperless ID 101)]",
                            "[*Unknown title*, Paperless ID 999]",
                            "*Unknown title*, Paperless ID 999)"):
            candidate = quote + " " + attribution
            class Repair:
                async def repair_answer(self, *args):
                    return {"answer": candidate}
            for repair in (None, Repair()):
                with self.subTest(attribution=attribution, repair=bool(repair)):
                    result = await AnswerFinalizer(QuoteAuditor(quote), repair).finalize(
                        "What is documented?", quote + ' (Paperless document 999)' if repair else candidate, pack)
                    self.assertFalse(result["finalization"]["complete"])
                    if repair:
                        self.assertEqual(result["finalization"]["attempts"], 2)

    async def test_citation_only_repair_can_remove_wrong_or_orphan_declaration(self):
        evidence = copy.deepcopy(PACK)
        evidence["items"].append({**PACK["items"][0], "id": "other", "document_id": 102, "title": "Other source"})
        for candidate in (QUOTE + ' (*Other source*, Paperless ID 102)',
                          '(*Policy 2026 revision 4*, Paperless ID 101) ' + QUOTE):
            class Repair:
                async def repair_answer(inner, question, answer, spans, verification):
                    reasons = verification.get("rejection_reasons", []) + [reason
                        for claim in verification["claims"] for reason in claim.get("rejection_reasons", [])]
                    self.assertIn("invalid_attribution", reasons)
                    return {"answer": QUOTE}
            result = await AnswerFinalizer(QuoteAuditor(QUOTE), Repair()).finalize(
                "What is documented?", candidate, evidence)
            self.assertEqual(result["finalization"]["disposition"], "supported")
            self.assertEqual(result["finalization"]["attempts"], 2)

    def test_malformed_nesting_does_not_stall_before_audit_deadline(self):
        # Canonicalization runs before the async audit timeout. Bound this probe
        # in a separate process so the regression cannot stall the test runner.
        code = ("from app.answer_finalization import canonical_candidate; "
                "_, declarations = canonical_candidate('(' * 50000 + 'Paperless ID 101', {'items': []}); "
                "assert declarations and all(d['document_id'] is None for d in declarations); "
                "_, repeated = canonical_candidate('(*Unknown*, Paperless ID 101) ' * 3000, {'items': []}); "
                "assert len(repeated) == 3000 and all(d['document_id'] is None for d in repeated)")
        subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, timeout=2)
