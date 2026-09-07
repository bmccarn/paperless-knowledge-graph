"""Citation metadata must match both supplied evidence and audited references."""
import copy
import hashlib
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
                            '(Paperless document 101)', '[Document 101](/documents/101)'):
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
            result = await AnswerFinalizer(QuoteAuditor(QUOTE)).finalize(
                "What is documented?", QUOTE + ' [Source: "Policy 2026 revision 4"]', evidence)
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
