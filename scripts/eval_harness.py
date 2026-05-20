#!/usr/bin/env python3
"""Run canonical document QA evaluations against a Paperless KG API."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib import request, error


def load_cases(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("eval cases must be a JSON array")
    return data


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
    answer = (result.get("answer") or "").lower()
    sources = result.get("sources") or []
    required_terms = case.get("required_terms") or []
    missing_terms = [term for term in required_terms if term.lower() not in answer]

    required_docs = {int(doc_id) for doc_id in case.get("required_source_doc_ids") or []}
    actual_docs = {int(s["document_id"]) for s in sources if s.get("document_id") is not None}
    missing_docs = sorted(required_docs - actual_docs)

    confidence = float(result.get("confidence") or 0)
    passed = not missing_terms and not missing_docs and confidence >= float(case.get("min_confidence", 0.35))

    return {
        "id": case.get("id"),
        "question": case.get("question"),
        "passed": passed,
        "confidence": confidence,
        "source_count": len(sources),
        "missing_terms": missing_terms,
        "missing_source_doc_ids": missing_docs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8484", help="Paperless KG API URL")
    parser.add_argument("--cases", type=Path, default=Path("evals/canonical_questions.json"))
    parser.add_argument("--model", default="", help="Optional LiteLLM model route override")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON only")
    args = parser.parse_args()

    cases = load_cases(args.cases)
    results = []
    started = time.time()

    for case in cases:
        payload = {"question": case["question"]}
        if args.model:
            payload["model"] = args.model
        try:
            response = post_json(args.base_url, "/query", payload, args.timeout)
            scored = score_case(case, response)
        except Exception as exc:
            scored = {
                "id": case.get("id"),
                "question": case.get("question"),
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
    }

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"Paperless KG eval: {passed}/{len(results)} passed in {summary['elapsed_seconds']}s")
        for result in results:
            status = "PASS" if result.get("passed") else "FAIL"
            print(f"{status} {result['id']}: confidence={result.get('confidence', 'n/a')} sources={result.get('source_count', 0)}")
            if result.get("missing_terms"):
                print(f"  missing terms: {', '.join(result['missing_terms'])}")
            if result.get("missing_source_doc_ids"):
                print(f"  missing source docs: {result['missing_source_doc_ids']}")
            if result.get("error"):
                print(f"  error: {result['error']}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
