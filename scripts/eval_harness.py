#!/usr/bin/env python3
"""Run canonical document QA evaluations against a Paperless KG API."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib import request, error
from datetime import date


def load_cases(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("cases"), list) and isinstance(data.get("documents"), list) and data.get("version"):
        return [dict(case, fixture_documents=data["documents"], fixture_version=data["version"]) for case in data["cases"]]
    raise ValueError("eval cases must be a JSON array or a versioned cases/documents manifest")


def post_json(base_url: str, path: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def score_case(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    answer = str(result.get("answer") or "")
    sources = result.get("sources") if isinstance(result.get("sources"), list) else []
    summary = result.get("source_summary") or {}
    suite = case.get("suite", "smoke")
    errors = []
    missing_terms = [term for term in case.get("required_terms", []) if term.lower() not in answer.lower()]
    required_docs = {int(doc_id) for doc_id in case.get("required_source_doc_ids") or []}
    actual_docs = {s["document_id"] for s in sources if isinstance(s, dict) and type(s.get("document_id")) is int}
    missing_docs = sorted(required_docs - actual_docs)
    missing_trace = bool(case.get("require_trace")) and not result.get("trace")
    missing_timeline = bool(case.get("require_timeline")) and not result.get("timeline_events")
    finalization = result.get("finalization") if isinstance(result.get("finalization"), dict) else {}
    disposition = finalization.get("disposition")
    if not disposition:
        errors.append("Missing terminal finalization disposition")
    if missing_terms or missing_docs or missing_trace or missing_timeline:
        errors.append("Missing required case output")
    for term in case.get("forbidden_terms", []):
        if term.casefold() in answer.casefold():
            errors.append(f"Forbidden answer fact: {term}")
    for event in result.get("timeline_events") or []:
        if not isinstance(event, dict) or not _calendar_date(event.get("date")):
            errors.append("Invalid timeline calendar date")
        elif suite == "factual" and event.get("document_id") not in {doc["document_id"] for doc in case.get("fixture_documents", [])}:
            errors.append("Timeline references an unknown fixture document")
    if suite == "smoke":
        if not actual_docs:
            errors.append("Smoke query returned no sources")
    elif suite == "factual":
        expected = case.get("expected_disposition")
        if expected == "abstained":
            if disposition not in case.get("allowed_abstention_dispositions", []) or finalization.get("complete"):
                errors.append("Expected an evidence-limited abstention")
            if _answer_text(answer) != _answer_text(str(case.get("expected_answer") or "")):
                errors.append("Abstention text includes an unexpected assertion or differs from the fixture")
        else:
            if disposition != "supported" or finalization.get("complete") is not True:
                errors.append("Expected complete supported finalization")
            accepted = [_answer_text(text) for text in case.get("accepted_answers", [])]
            if not accepted or _answer_text(answer) not in accepted:
                errors.append("Answer does not match independently accepted factual wording")
            claims = (result.get("claim_ledger") or {}).get("claims", [])
            if not isinstance(claims, list) or not claims:
                claims = []
                errors.append("No source-bound claims")
            references = []
            for claim in claims:
                refs = claim.get("references", []) if isinstance(claim, dict) else []
                if not isinstance(claim, dict) or claim.get("status") != "supported" or not _answer_text(str(claim.get("claim") or "")) or _answer_text(str(claim.get("claim") or "")) not in _answer_text(answer):
                    errors.append("Claim ledger contradicts or does not describe the final answer")
                if not isinstance(refs, list) or not refs:
                    errors.append("Claim has no source reference")
                    continue
                for ref in refs:
                    if not _fixture_reference(ref, case.get("fixture_documents", [])):
                        errors.append("Reference does not match independent fixture source/offsets")
                    else:
                        references.append(ref)
            expected_events = case.get("expected_timeline_events", [])
            actual_events = result.get("timeline_events", [])
            if expected_events or actual_events:
                expected_keys = {(event["date"], event["document_id"], event["title"], event["summary"], event["precision"]) for event in expected_events}
                actual_keys = {(event.get("date"), event.get("document_id"), event.get("title"), event.get("summary"), event.get("precision")) for event in actual_events if isinstance(event, dict)}
                if actual_keys != expected_keys:
                    errors.append("Timeline differs from independent expected events")
                for event in actual_events:
                    refs = event.get("references") if isinstance(event, dict) else None
                    if not isinstance(refs, list) or not refs or not all(_fixture_reference(ref, case.get("fixture_documents", [])) for ref in refs):
                        errors.append("Timeline event lacks valid independent source references")
            cited_docs = {ref["document_id"] for ref in references}
            if not required_docs <= cited_docs:
                errors.append("Required sources are not cited by validated claims")
            for fact in case.get("expected_facts", []):
                value = str(fact["value"])
                unit = str(fact.get("unit") or "")
                if value not in answer or (unit and unit not in answer):
                    errors.append(f"Missing expected value/unit: {fact['name']}")
                if not any(ref["document_id"] == fact["document_id"] and fact["quote"] in ref["quote"] for ref in references):
                    errors.append(f"Expected fact lacks its fixture evidence: {fact['name']}")
    else:
        errors.append("Unknown evaluation suite")
    return {
        "id": case.get("id"),
        "question": case.get("question"),
        "passed": not errors,
        "suite": suite,
        "factual_ground_truth": suite == "factual",
        "disposition": disposition,
        "expected_disposition": case.get("expected_disposition"),
        "errors": list(dict.fromkeys(errors)),
        "confidence": result.get("confidence"),
        "trust_score": summary.get("trust_score"),
        "verification_status": summary.get("verification_status"),
        "source_count": len(sources),
        "missing_terms": missing_terms,
        "missing_source_doc_ids": missing_docs,
        "missing_trace": missing_trace,
        "missing_timeline": missing_timeline,
    }


def _answer_text(text: str) -> str:
    text = re.sub(r"\[Document \d+\]\(/documents/\d+\)", "", text)
    return " ".join(text.split())


def _calendar_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        if re.fullmatch(r"\d{4}", value):
            date(int(value), 1, 1)
        elif re.fullmatch(r"\d{4}-\d{2}", value):
            date.fromisoformat(value + "-01")
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            date.fromisoformat(value)
        else:
            return False
        return True
    except ValueError:
        return False


def _fixture_reference(ref: Any, documents: list[dict]) -> bool:
    if not isinstance(ref, dict) or type(ref.get("document_id")) is not int:
        return False
    start, end, quote = ref.get("start"), ref.get("end"), ref.get("quote")
    if type(start) is not int or type(end) is not int or not isinstance(quote, str) or not quote or not 0 <= start < end:
        return False
    for document in documents:
        if document["document_id"] != ref["document_id"]:
            continue
        for chunk in document["chunks"]:
            content = chunk["content"]
            if end <= len(content) and content[start:end] == quote and hashlib.sha256(content.encode()).hexdigest() == ref.get("content_digest"):
                return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8484", help="Paperless KG API URL")
    parser.add_argument("--cases", type=Path, default=Path("evals/fixtures/accuracy-v1.json"))
    parser.add_argument("--model", default="", help="Optional LiteLLM model route override")
    parser.add_argument("--mode", default="", choices=["", "quick", "deep", "timeline", "strict"], help="Force one query mode for all cases")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON only")
    args = parser.parse_args()

    cases = load_cases(args.cases)
    results = []
    started = time.time()

    for case in cases:
        payload = {"question": case["question"], "mode": args.mode or case.get("mode", "deep")}
        if args.model:
            payload["model"] = args.model
        try:
            response = post_json(args.base_url, "/query", payload, args.timeout)
            scored = score_case(case, response)
        except Exception as exc:
            scored = {
                "id": case.get("id"),
                "question": case.get("question"),
                "suite": case.get("suite", "smoke"),
                "expected_disposition": case.get("expected_disposition"),
                "passed": False,
                "error": str(exc),
            }
        results.append(scored)

    passed = sum(1 for r in results if r.get("passed"))
    summary = {
        "passed": passed,
        "failed": len(results) - passed,
        "total": len(results),
        "elapsed_seconds": round(time.time() - started, 1),
        "results": results,
        "factual_case_count": sum(r.get("suite") == "factual" for r in results),
        "incorrect_factual_answer_count": sum(r.get("suite") == "factual" and r.get("disposition") == "supported" and not r.get("passed") for r in results),
        "abstention_count": sum(r.get("disposition") in {"unsupported", "incomplete", "current_unresolved", "audit_failed", "timeout"} for r in results),
        "execution_error_count": sum(bool(r.get("error")) for r in results),
    }
    summary["incorrect_factual_answer_rate"] = summary["incorrect_factual_answer_count"] / summary["factual_case_count"] if summary["factual_case_count"] else None
    summary["abstention_rate"] = summary["abstention_count"] / len(results) if results else None

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"Paperless KG eval: {passed}/{len(results)} passed in {summary['elapsed_seconds']}s")
        for result in results:
            status = "PASS" if result.get("passed") else "FAIL"
            print(f"{status} {result['id']}: confidence={result.get('confidence', 'n/a')} sources={result.get('source_count', 0)}")
            if result.get("trust_score") is not None:
                print(f"  trust={result.get('trust_score')} verification={result.get('verification_status')}")
            if result.get("missing_terms"):
                print(f"  missing terms: {', '.join(result['missing_terms'])}")
            if result.get("missing_source_doc_ids"):
                print(f"  missing source docs: {result['missing_source_doc_ids']}")
            if result.get("missing_trace"):
                print("  missing trace output")
            if result.get("missing_timeline"):
                print("  missing timeline events")
            if result.get("error"):
                print(f"  error: {result['error']}")
            for issue in result.get("errors", []):
                print(f"  {issue}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
