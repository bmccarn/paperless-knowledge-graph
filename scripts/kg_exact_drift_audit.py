#!/usr/bin/env python3
"""Post-restore/post-migration exact drift audit for Paperless KG."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DRIFT_KEYS = (
    ("missing_from_graph", "missing graph docs"),
    ("extra_in_graph", "extra graph docs"),
    ("missing_embeddings", "missing embeddings"),
    ("extra_embeddings", "extra embeddings"),
    ("missing_hashes", "missing hashes"),
    ("extra_hashes", "extra hashes"),
    ("modified_after_last_sync", "modified after sync"),
)


def _api_url(base_url: str, path: str, params: dict[str, str] | None = None) -> str:
    base = base_url.rstrip("/")
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    return f"{base}{path}{query}"


def _request_json(url: str, method: str = "GET", timeout: int = 60) -> dict[str, Any]:
    req = urllib.request.Request(url, method=method)
    if method != "GET":
        req.add_header("Content-Length", "0")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: HTTP {exc.code} {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc


def _doc_label(item: Any) -> str:
    if isinstance(item, dict):
        doc_id = item.get("id")
        title = item.get("title")
        modified = item.get("modified")
        suffix = f" {title}" if title else ""
        changed = f" ({modified})" if modified else ""
        return f"#{doc_id}{suffix}{changed}"
    return f"#{item}"


def _drift_items(snapshot: dict[str, Any]) -> list[tuple[str, list[Any]]]:
    drift = snapshot.get("drift") or {}
    items: list[tuple[str, list[Any]]] = []
    for key, label in DRIFT_KEYS:
        values = drift.get(key) or []
        if values:
            items.append((label, values))
    return items


def _has_drift(snapshot: dict[str, Any]) -> bool:
    return bool(snapshot.get("stale") or _drift_items(snapshot))


def _print_summary(snapshot: dict[str, Any], base_url: str) -> None:
    print("Paperless KG exact drift audit")
    print(f"Base URL: {base_url.rstrip('/')}")
    print(f"Stale: {bool(snapshot.get('stale'))}")
    print(
        "Counts: "
        f"paperless={snapshot.get('paperless_documents')} "
        f"graph={snapshot.get('indexed_documents')} "
        f"embeddings={snapshot.get('docs_with_embeddings')} "
        f"hashes={snapshot.get('hashed_documents')}"
    )
    print(f"Last sync: {snapshot.get('last_sync') or 'never'}")
    print(f"Latest Paperless doc: #{snapshot.get('latest_paperless_id')} {snapshot.get('latest_paperless_title') or ''}".rstrip())

    drift_items = _drift_items(snapshot)
    if not drift_items:
        print("Exact drift: none")
        return

    sample_limit = (snapshot.get("drift") or {}).get("sample_limit")
    print(f"Exact drift: {len(drift_items)} category/categories" + (f" (sample limit {sample_limit})" if sample_limit else ""))
    for label, values in drift_items:
        rendered = ", ".join(_doc_label(item) for item in values)
        print(f"- {label}: {len(values)} reported -> {rendered}")


def _wait_for_task(base_url: str, task_id: str, timeout_seconds: int) -> dict[str, Any]:
    started = time.monotonic()
    while time.monotonic() - started < timeout_seconds:
        task = _request_json(_api_url(base_url, f"/task/{task_id}"), timeout=30)
        status = task.get("status")
        if status in {"completed", "failed", "cancelled"}:
            return task
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for task {task_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("KG_URL", "https://kg.mccarn.tech/api"), help="KG API base URL")
    parser.add_argument("--json", action="store_true", help="Print the raw freshness JSON")
    parser.add_argument("--no-strict", action="store_true", help="Exit 0 even when drift is present")
    parser.add_argument("--repair", action="store_true", help="Start targeted repair for the exact drift IDs reported by freshness")
    parser.add_argument("--wait", action="store_true", help="Wait for --repair task completion")
    parser.add_argument("--wait-timeout", type=int, default=900, help="Seconds to wait for --repair task")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    try:
        snapshot = _request_json(_api_url(base_url, "/freshness", {"force": "true"}))
        if args.json:
            print(json.dumps(snapshot, indent=2, sort_keys=True))
        else:
            _print_summary(snapshot, base_url)

        has_drift = _has_drift(snapshot)
        if args.repair and has_drift:
            repair = _request_json(_api_url(base_url, "/freshness/repair"), method="POST")
            print(f"Repair response: {json.dumps(repair, sort_keys=True)}")
            if args.wait and repair.get("task_id"):
                task = _wait_for_task(base_url, repair["task_id"], args.wait_timeout)
                print(f"Repair task finished: {task.get('status')}")
                if task.get("status") != "completed":
                    return 1
                snapshot = _request_json(_api_url(base_url, "/freshness", {"force": "true"}))
                has_drift = _has_drift(snapshot)
                if not args.json:
                    _print_summary(snapshot, base_url)
        elif args.repair:
            print("Repair skipped: no exact drift reported")

        return 1 if has_drift and not args.no_strict else 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
