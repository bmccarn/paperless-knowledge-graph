"""Independent corpus expectations must beat keyword and confidence gaming."""

import hashlib
import json
from pathlib import Path
import unittest

from scripts.eval_harness import load_cases, score_case

ROOT = Path(__file__).resolve().parents[1]


def source_reference(document_id=9101, quote="Amount due: 125.00 USD."):
    corpus = json.loads((ROOT / "evals/fixtures/accuracy-v1.json").read_text())
    source = next(doc for doc in corpus["documents"] if doc["document_id"] == document_id)["chunks"][0]["content"]
    start = source.index(quote)
    return {"document_id": document_id, "evidence_id": "synthetic-evidence", "span_id": "synthetic-span",
            "content_digest": hashlib.sha256(source.encode()).hexdigest(), "start": start, "end": start + len(quote), "quote": quote}


def supported_response(answer="The amount due is 125.00 USD."):
    return {"answer": answer, "confidence": 0.01, "sources": [{"document_id": 9101}],
            "finalization": {"disposition": "supported", "complete": True},
            "claim_ledger": {"complete": True, "claims": [{"claim": answer, "status": "supported", "references": [source_reference()]}]}}


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        manifest = json.loads((ROOT / "evals/fixtures/accuracy-v1.json").read_text())
        self.cases = [dict(case, fixture_documents=manifest["documents"], fixture_version=manifest["version"]) for case in manifest["cases"]]

    def test_all_six_old_keyword_only_forgeries_fail(self):
        cases = json.loads((ROOT / "evals/canonical_questions.json").read_text())
        for case in cases:
            forged = {"answer": " ".join(case["required_terms"]), "confidence": 0.99, "sources": [],
                      "source_summary": {"trust_score": 0.99, "verification_status": "not_run"},
                      "trace": [{"made_up": True}], "timeline_events": [{"date": "2099-02-31"}]}
            with self.subTest(case=case["id"]):
                self.assertFalse(score_case(case, forged)["passed"])

    def test_correct_low_confidence_supported_answer_passes(self):
        self.assertTrue(score_case(self.cases[0], supported_response())["passed"])

    def test_wrong_high_confidence_value_or_unit_fails(self):
        for answer in ["The amount due is 152.00 USD.", "The amount due is 125.00 EUR."]:
            result = supported_response(answer)
            result["confidence"] = 0.99
            self.assertFalse(score_case(self.cases[0], result)["passed"])

    def test_unknown_document_and_real_quote_with_wrong_offsets_fail(self):
        for field, value in [("document_id", 999999), ("start", 0), ("quote", "Amount due: 999 USD.")]:
            response = supported_response()
            response["confidence"] = 0.99
            response["claim_ledger"]["claims"][0]["references"][0][field] = value
            with self.subTest(field=field):
                self.assertFalse(score_case(self.cases[0], response)["passed"])

    def test_expected_abstention_passes_but_appended_factual_guess_fails(self):
        case = next(case for case in self.cases if case["id"] == "missing-source-abstention")
        response = {"answer": case["expected_answer"], "sources": [], "confidence": 0,
                    "finalization": {"disposition": "incomplete", "complete": False}}
        self.assertTrue(score_case(case, response)["passed"])
        response["answer"] += " The balance is 999 USD."
        self.assertFalse(score_case(case, response)["passed"])

    def test_invalid_timeline_date_is_rejected_even_with_valid_claim(self):
        result = supported_response()
        result["confidence"] = 0.99
        result["timeline_events"] = [{"date": "2099-02-31", "document_id": 9101}]
        self.assertFalse(score_case(self.cases[0], result)["passed"])

    def test_manifest_loading_attaches_independent_corpus(self):
        loaded = load_cases(ROOT / "evals/fixtures/accuracy-v1.json")
        self.assertEqual(loaded[0]["fixture_version"], "accuracy-v1")
        self.assertEqual(loaded[0]["fixture_documents"][0]["document_id"], 9101)

    def test_correct_timeline_passes_and_invented_event_with_real_quote_fails(self):
        case = next(case for case in self.cases if case["mode"] == "timeline")
        answer = "The due date is 2026-01-31."
        ref = source_reference(quote="Due date: 2026-01-31.")
        from app.timeline import project_timeline
        from app.answer_delivery import render_verified_answer
        result = supported_response(answer)
        result['claim_ledger'].update(candidate_text=answer, candidate_digest=hashlib.sha256(answer.encode()).hexdigest(),
            unitization='prose_v1', summary={'total':1,'audited':1,'supported':1})
        result['claim_ledger']['claims'][0].update(id='u1',start=0,end=len(answer),references=[ref])
        result['answer'] = render_verified_answer(answer,result['claim_ledger']['claims'])
        result['finalization'].update(answer_verified=True, candidate_digest=result['claim_ledger']['candidate_digest'],
            answer_digest=hashlib.sha256(result['answer'].encode()).hexdigest())
        result['timeline_events'], result['finalization']['timeline'] = project_timeline(
            result['answer'], result['claim_ledger'], result['finalization'])
        self.assertTrue(score_case(case, result)["passed"])
        result["timeline_events"][0]["title"] = "Coverage canceled"
        self.assertFalse(score_case(case, result)["passed"])

    def test_answer_cannot_hide_an_extra_claim_behind_correct_keywords(self):
        response = supported_response("The amount due is 125.00 USD. A second bill is 999 USD.")
        self.assertFalse(score_case(self.cases[0], response)["passed"])


if __name__ == "__main__":
    unittest.main()
