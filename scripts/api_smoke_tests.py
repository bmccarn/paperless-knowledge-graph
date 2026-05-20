#!/usr/bin/env python3
"""Smoke test the core Paperless KG API without mutating data by default."""

from __future__ import annotations

import argparse
import json
import sys
from urllib import request


def get_json(base_url: str, path: str) -> dict:
    with request.urlopen(f"{base_url.rstrip('/')}{path}", timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_json(base_url: str, path: str, payload: dict | None = None) -> dict:
    body = json.dumps(payload or {}).encode("utf-8")
    req = request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8484")
    parser.add_argument("--mutating", action="store_true", help="Also smoke-test /sync. /reindex is never invoked.")
    args = parser.parse_args()

    checks = []
    status = get_json(args.base_url, "/status")
    checks.append(("status", status.get("status") in {"healthy", "degraded"}))

    health = get_json(args.base_url, "/health")
    checks.append(("health", health.get("status") in {"healthy", "degraded"}))

    guardrails = get_json(args.base_url, "/ops/guardrails")
    checks.append(("guardrails", guardrails.get("status") in {"ok", "alerting"}))

    query = post_json(args.base_url, "/query", {"question": "What documents are available?"})
    checks.append(("query", bool(query.get("answer")) and isinstance(query.get("sources", []), list)))

    if args.mutating:
        sync = post_json(args.base_url, "/sync")
        checks.append(("sync", sync.get("status") == "started"))

    for name, passed in checks:
        print(f"{'PASS' if passed else 'FAIL'} {name}")

    return 0 if all(passed for _, passed in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
