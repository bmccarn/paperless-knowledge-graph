"""Deterministic query planning, evidence scoring, and trace helpers."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.evidence import exact_term_hits as evidence_exact_term_hits


DOMAIN_TERMS: dict[str, tuple[str, ...]] = {
    "insurance": ("insurance", "policy", "coverage", "premium", "deductible", "carrier", "declaration"),
    "tax": ("tax", "irs", "return", "w-2", "w2", "1099", "k-1", "schedule", "rapidroute"),
    "mortgage": ("mortgage", "loan", "escrow", "servicer", "statement", "home loan"),
    "medical": ("medical", "doctor", "diagnosis", "medication", "lab", "provider", "prescription", "health"),
    "vehicle": ("vehicle", "auto", "car", "truck", "vin", "registration", "title"),
    "legal": ("contract", "legal", "lease", "agreement", "settlement"),
    "financial": ("bank", "account", "balance", "statement", "finance", "financial", "invoice"),
    "military": ("military", "va", "veteran", "disability", "rating", "orders", "dd-214", "service"),
    "property": ("property", "deed", "home", "house", "address", "utility"),
}

CURRENT_TERMS = (
    "current", "latest", "most recent", "active", "now", "today", "final", "updated", "newest",
    "what do i have", "what is my", "coverage", "balance", "premium", "status",
)

TIMELINE_TERMS = (
    "timeline", "history", "progression", "changed", "over time", "chronological",
    "when", "effective", "expired", "expiration", "from", "to",
)

SUPERSEDED_TERMS = ("superseded", "replaced by", "cancelled", "canceled", "expired", "void", "prior version")


def normalize_mode(mode: str | None) -> str:
    mode = (mode or "deep").lower()
    if mode == "fast":
        mode = "quick"
    if mode in {"high_accuracy", "high-stakes", "high_stakes", "strict"}:
        mode = "strict"
    return mode if mode in {"quick", "deep", "timeline", "strict"} else "deep"


def classify_domain(question: str) -> str:
    q = question.lower()
    matches = [
        (domain, sum(1 for term in terms if term in q))
        for domain, terms in DOMAIN_TERMS.items()
    ]
    matches = [(domain, score) for domain, score in matches if score > 0]
    if not matches:
        return "general"
    matches.sort(key=lambda item: item[1], reverse=True)
    if matches[0][0] == "insurance":
        return "insurance"
    if len(matches) > 1 and matches[1][1] == matches[0][1] and len(matches) > 2:
        return "mixed"
    return matches[0][0]


def classify_intent(question: str, mode: str) -> str:
    q = question.lower()
    if mode == "timeline" or any(term in q for term in TIMELINE_TERMS):
        return "timeline"
    if any(term in q for term in CURRENT_TERMS):
        return "current_state"
    if any(term in q for term in ("compare", "difference", "versus", " vs ", "between")):
        return "compare"
    if any(term in q for term in ("all", "list", "inventory", "documents", "referenced")):
        return "broad_inventory"
    return "lookup"


def requires_current_check(question: str, domain: str, intent: str) -> bool:
    q = question.lower()
    if intent in {"current_state", "timeline"}:
        return True
    if any(term in q for term in CURRENT_TERMS):
        return True
    return domain in {"insurance", "tax", "mortgage", "medical", "vehicle", "legal", "financial", "military"}


def heuristic_plan(question: str, mode: str) -> dict[str, Any]:
    mode = normalize_mode(mode)
    domain = classify_domain(question)
    intent = classify_intent(question, mode)
    current_required = requires_current_check(question, domain, intent)

    subqueries: list[dict[str, str]] = [{"role": "primary", "query": question}]
    if mode != "quick":
        if current_required:
            subqueries.append({
                "role": "current_state",
                "query": f"{question} latest current final most recent updated revised effective expiration",
            })
        if domain != "general":
            subqueries.append({
                "role": "domain_inventory",
                "query": f"{domain} documents policy statement record amount date {question}",
            })
        if mode == "timeline" or intent == "timeline":
            subqueries.append({
                "role": "timeline_dates",
                "query": f"{question} effective date expiration date statement period revision history chronological",
            })
        if mode == "strict":
            subqueries.append({
                "role": "source_quality",
                "query": f"{question} original source document exact values effective dates statement period",
            })
            subqueries.append({
                "role": "contradiction_check",
                "query": f"{question} corrected revised superseded previous conflicting updated",
            })

    return {
        "mode": mode,
        "intent": intent,
        "domain": domain,
        "requires_current": current_required,
        "needs_timeline": mode == "timeline" or intent == "timeline",
        "required_doc_types": _required_doc_types(domain),
        "subqueries": _dedupe_subqueries(subqueries),
        "must_answer_current_vs_historical": current_required,
        "strategy": _strategy_for_mode(mode),
        "planner": "heuristic",
        "reasoning": "Heuristic fallback based on domain, current-state, and timeline terms.",
    }


def merge_agent_plan(question: str, mode: str, agent_plan: dict[str, Any] | None) -> dict[str, Any]:
    plan = heuristic_plan(question, mode)
    if not isinstance(agent_plan, dict):
        return plan

    for key in ("intent", "domain", "reasoning"):
        value = agent_plan.get(key)
        if isinstance(value, str) and value.strip():
            plan[key] = value.strip().lower() if key in {"intent", "domain"} else value.strip()

    for key in ("requires_current", "needs_timeline", "must_answer_current_vs_historical"):
        if isinstance(agent_plan.get(key), bool):
            plan[key] = agent_plan[key]

    if isinstance(agent_plan.get("required_doc_types"), list):
        plan["required_doc_types"] = [str(v) for v in agent_plan["required_doc_types"] if str(v).strip()][:8]

    agent_subqueries = []
    for item in agent_plan.get("subqueries", []) if isinstance(agent_plan.get("subqueries"), list) else []:
        if isinstance(item, str):
            agent_subqueries.append({"role": "planned", "query": item})
        elif isinstance(item, dict) and item.get("query"):
            agent_subqueries.append({
                "role": str(item.get("role") or "planned"),
                "query": str(item["query"]),
            })
    if agent_subqueries:
        plan["subqueries"] = _dedupe_subqueries([{"role": "primary", "query": question}] + agent_subqueries)
    plan["planner"] = "strands"
    plan["strategy"] = _strategy_for_mode(normalize_mode(mode))
    return plan


def retrieval_queries(plan: dict[str, Any], max_queries: int = 6) -> list[dict[str, str]]:
    queries = plan.get("subqueries") or []
    normalized = []
    seen = set()
    for item in queries:
        if isinstance(item, str):
            item = {"role": "planned", "query": item}
        query = str(item.get("query", "")).strip()
        if not query:
            continue
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"role": str(item.get("role") or "planned"), "query": query})

    if plan.get("requires_current") and not any(item["role"] == "current_state" for item in normalized):
        primary = normalized[0]["query"] if normalized else ""
        current_query = f"{primary} latest current final most recent updated revised effective expiration".strip()
        if current_query and current_query.lower() not in seen:
            normalized.append({"role": "current_state", "query": current_query})
    return normalized[:max_queries]


def trace_step(step: str, status: str = "ok", detail: str = "", data: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "step": step,
        "status": status,
        "detail": detail,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if data:
        payload["data"] = data
    return payload


def compute_evidence_grade(
    question: str,
    plan: dict[str, Any],
    sources: list[dict[str, Any]],
    context: dict[str, Any],
    verification: dict[str, Any] | None = None,
    evidence_pack: dict[str, Any] | None = None,
    claim_ledger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reasons = []
    penalties = []
    dimensions: dict[str, Any] = {}
    evidence_pack = evidence_pack or {}
    coverage = evidence_pack.get("coverage") or {}
    claim_summary = (claim_ledger or {}).get("summary") or {}
    verification_status = (verification or {}).get("status")

    source_count = len(sources)
    retrieval_score = 0.0
    if source_count >= 5:
        retrieval_score += 0.75
        reasons.append(f"{source_count} source documents retrieved")
    elif source_count >= 2:
        retrieval_score += 0.5
        reasons.append(f"{source_count} source documents retrieved")
    elif source_count == 1:
        retrieval_score += 0.28
        reasons.append("1 source document retrieved")
    else:
        penalties.append("No source documents were retrieved")

    exact_hits = _exact_term_hits(question, sources)
    if exact_hits >= 3:
        retrieval_score += 0.25
        reasons.append("Strong direct term overlap with retrieved sources")
    elif exact_hits >= 1:
        retrieval_score += 0.12
        reasons.append("Some direct term overlap with retrieved sources")
    else:
        penalties.append("Weak direct term overlap with retrieved sources")
    retrieval_score = min(1.0, retrieval_score)

    freshness_score = 0.75
    if plan.get("requires_current"):
        latest_sources = [s for s in sources if s.get("date")]
        if latest_sources:
            freshness_score = 0.75
            reasons.append("Current-state query has dated source coverage")
        else:
            freshness_score = 0.25
            penalties.append("Current-state query lacks dated sources")
    if coverage.get("date_signal_counts"):
        freshness_score = min(1.0, freshness_score + 0.15)

    source_quality_score = float(coverage.get("average_source_quality") or 0)
    if source_quality_score:
        if source_quality_score >= 0.78:
            reasons.append("Evidence pack uses strong original/direct sources")
        elif source_quality_score < 0.55:
            penalties.append("Evidence pack relies on weaker summary/context sources")
    else:
        source_quality_score = 0.45

    superseded_count = sum(1 for s in sources if _looks_superseded_text(s.get("excerpt", "")))
    contradiction_score = 1.0
    if superseded_count:
        contradiction_score -= min(0.45, superseded_count * 0.12)
        penalties.append(f"{superseded_count} retrieved source(s) look superseded or expired")

    claim_support_score = 0.5
    supported = int(claim_summary.get("supported") or 0)
    partial = int(claim_summary.get("partial") or 0)
    unsupported_claims = int(claim_summary.get("unsupported") or 0)
    conflicting_claims = int(claim_summary.get("conflicting") or 0)
    ledger_total = supported + partial + unsupported_claims + conflicting_claims + int(claim_summary.get("unknown") or 0)
    if ledger_total:
        claim_support_score = (supported + 0.5 * partial) / max(ledger_total, 1)
        if supported:
            reasons.append(f"{supported} answer claim(s) source-backed")
        if unsupported_claims:
            penalties.append(f"{unsupported_claims} ledger claim(s) unsupported")
        if conflicting_claims:
            penalties.append(f"{conflicting_claims} ledger claim(s) conflicting")

    if verification:
        unsupported = verification.get("unsupported_claims") or []
        stale = verification.get("stale_or_conflicting_claims") or []
        if not unsupported and not stale and verification.get("status") in {"verified", "ok"}:
            claim_support_score = max(claim_support_score, 0.9)
            reasons.append("Verifier found no unsupported or stale claims")
        if unsupported:
            claim_support_score = min(claim_support_score, max(0.0, 0.65 - 0.08 * len(unsupported)))
            penalties.append(f"{len(unsupported)} unsupported claim(s) flagged by verifier")
        if stale:
            contradiction_score = min(contradiction_score, max(0.0, 0.8 - 0.1 * len(stale)))
            penalties.append(f"{len(stale)} stale/conflicting claim(s) flagged by verifier")

    audit_score = 1.0 if ledger_total else 0.5
    audit_status = "claim_audited" if ledger_total else "not_claim_audited"
    score_cap = 1.0
    if verification_status == "checking":
        audit_score = 0.35
        audit_status = "checking"
        score_cap = min(score_cap, 0.69)
        penalties.append("Claim support audit is still running")
    elif verification_status == "not_run":
        audit_score = 0.25
        audit_status = "verifier_unavailable"
        score_cap = min(score_cap, 0.74)
        penalties.append("Verifier unavailable; answer is retrieval-backed but not claim-audited")
    elif verification_status == "needs_review":
        audit_score = min(audit_score, 0.65)
        audit_status = "needs_review"
        score_cap = min(score_cap, 0.77)
        penalties.append("Verifier marked the answer for review")
    elif verification_status in {"verified", "ok"}:
        if ledger_total:
            audit_score = max(0.75, claim_support_score)
        else:
            audit_score = 0.75
            audit_status = "verifier_checked"
    elif not ledger_total:
        audit_score = 0.3
        audit_status = "audit_missing"
        score_cap = min(score_cap, 0.74)
        penalties.append("No claim-level audit was available; trust is retrieval-backed only")

    structured_score = 0.35
    structured_count = int(coverage.get("structured_fact_count") or 0)
    if structured_count >= 20:
        structured_score = 1.0
        reasons.append("Structured values/tables are present in evidence")
    elif structured_count >= 5:
        structured_score = 0.7

    graph_nodes = len(context.get("graph_nodes", []) or [])
    if graph_nodes:
        reasons.append(f"{graph_nodes} graph entities contributed context")

    dimensions = {
        "retrieval_coverage": round(retrieval_score, 3),
        "source_freshness": round(freshness_score, 3),
        "claim_support": round(claim_support_score, 3),
        "source_quality": round(source_quality_score, 3),
        "contradiction_check": round(max(0.0, contradiction_score), 3),
        "structured_evidence": round(structured_score, 3),
        "audit_coverage": round(audit_score, 3),
    }
    score = (
        0.22 * retrieval_score
        + 0.18 * freshness_score
        + 0.3 * claim_support_score
        + 0.15 * source_quality_score
        + 0.1 * max(0.0, contradiction_score)
        + 0.05 * structured_score
    )
    score = max(0.0, min(1.0, score))
    if score > score_cap:
        score = score_cap
        penalties.append("Trust score capped until claim audit coverage improves")
    level = "high" if score >= 0.78 else "medium" if score >= 0.5 else "low"
    return {
        "score": round(score, 3),
        "level": level,
        "reasons": reasons[:6],
        "penalties": penalties[:6],
        "source_count": source_count,
        "exact_term_hits": exact_hits,
        "dimensions": dimensions,
        "audit_status": audit_status,
        "claim_summary": claim_summary,
        "coverage": coverage,
    }


def current_state_summary(plan: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    dated = [s for s in sources if s.get("date")]
    latest = max((s["date"] for s in dated), default=None)
    expired = [s for s in sources if _looks_superseded_text(s.get("excerpt", ""))]
    return {
        "required": bool(plan.get("requires_current")),
        "latest_source_date": latest,
        "dated_source_count": len(dated),
        "superseded_source_count": len(expired),
        "status": "resolved" if latest else ("not_required" if not plan.get("requires_current") else "needs_review"),
    }


def timeline_fallback_events(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    for source in sources:
        date = source.get("date")
        if not date:
            continue
        events.append({
            "date": str(date),
            "title": source.get("title") or f"Document {source.get('document_id')}",
            "summary": (source.get("excerpt") or "")[:180],
            "document_id": source.get("document_id"),
            "source_title": source.get("title"),
            "status": "source_event",
        })
    return sort_timeline_events(events)[:20]


def sort_timeline_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(event: dict[str, Any]) -> tuple[str, str]:
        raw = str(event.get("date") or "")
        normalized = _normalize_date_key(raw)
        return normalized, str(event.get("title") or event.get("summary") or "")
    return sorted(events, key=key)


def _normalize_date_key(value: str) -> str:
    match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", value)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    match = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", value)
    if match:
        month, day, year = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    match = re.search(r"\b(19|20)\d{2}\b", value)
    return match.group(0) if match else "9999"


def _required_doc_types(domain: str) -> list[str]:
    return {
        "insurance": ["insurance", "policy", "declaration"],
        "tax": ["tax", "w2", "1099", "return"],
        "mortgage": ["mortgage", "statement", "loan"],
        "medical": ["medical", "lab", "visit", "prescription"],
        "vehicle": ["vehicle", "registration", "title", "insurance"],
        "legal": ["contract", "agreement", "legal"],
        "financial": ["statement", "invoice", "financial"],
        "military": ["military", "va", "service"],
        "property": ["property", "deed", "utility", "insurance"],
    }.get(domain, [])


def _strategy_for_mode(mode: str) -> str:
    return {
        "quick": "single-pass deterministic retrieval with no agent gap loop",
        "deep": "Strands-planned multi-query retrieval, graph expansion, evidence grading, verifier pass",
        "timeline": "Strands-planned retrieval, timeline extraction, deterministic event sort, verifier pass",
        "strict": "High-accuracy retrieval, source-quality evidence pack, claim ledger, verifier repair loop",
    }[normalize_mode(mode)]


def _dedupe_subqueries(subqueries: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    result = []
    for item in subqueries:
        query = str(item.get("query", "")).strip()
        if not query:
            continue
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append({"role": str(item.get("role") or "planned"), "query": query})
    return result[:8]


def _exact_term_hits(question: str, sources: list[dict[str, Any]]) -> int:
    source_text = " ".join(
        f"{s.get('title', '')} {s.get('doc_type', '')} {s.get('excerpt', '')}"
        for s in sources
    )
    return evidence_exact_term_hits(question, source_text)


def _looks_superseded_text(text: str) -> bool:
    lower = (text or "").lower()
    return any(term in lower for term in SUPERSEDED_TERMS)
