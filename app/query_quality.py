"""Deterministic query planning, and trace helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from app.answer_finalization import parse_date, current_state
from typing import Any


DOMAIN_TERMS: dict[str, tuple[str, ...]] = {
    "insurance": ("insurance", "policy", "coverage", "premium", "deductible", "carrier", "declaration"),
    "tax": ("tax", "irs", "return", "w-2", "w2", "1099", "k-1", "schedule"),
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


def current_state_summary(plan: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    dated = [parse_date(s.get("date")) for s in sources]
    dates = [d[0] for d in dated if d]
    result = current_state(plan, {"items": []}, plan.get("evaluated_at") or datetime.now(timezone.utc).date().isoformat())
    result.update(latest_source_date=max(dates, default=None), dated_source_count=len(dates))
    return result




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
        "timeline": "Strands-planned retrieval, complete source audit, dates from final verified observations",
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
