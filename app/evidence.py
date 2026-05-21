"""Evidence pack, claim ledger, and trust-calibration helpers.

This module keeps the high-accuracy query path grounded in source-anchored
evidence objects instead of free-form context strings. The LLM can still plan,
explain, and critique, but the answer pipeline can inspect the same evidence
pack at synthesis, verification, and UI time.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Any

QUERY_STOPWORDS = {
    "a", "about", "an", "and", "answer", "are", "as", "at", "be", "by",
    "can", "could", "current", "did", "do", "document", "documents", "does",
    "for", "from", "had", "has", "have", "how", "i", "in", "into", "is",
    "it", "last", "latest", "level", "levels", "me", "my", "newest", "now",
    "of", "on", "or", "our", "show", "source", "sources", "the", "their",
    "this", "to", "was", "were", "what", "when", "where", "which", "who",
    "with", "would", "you", "your",
}


HIGH_STAKES_DOMAINS = {
    "medical",
    "tax",
    "insurance",
    "legal",
    "financial",
    "mortgage",
    "military",
}

DIRECT_SOURCE_TERMS = (
    "laboratory report",
    "lab report",
    "diagnostic report",
    "statement",
    "declaration",
    "policy",
    "contract",
    "agreement",
    "return",
    "w-2",
    "w2",
    "1099",
    "k-1",
    "invoice",
    "receipt",
    "discharge summary",
)

SUMMARY_SOURCE_TERMS = (
    "summary",
    "portal",
    "patient health summary",
    "overview",
)

DATE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("specimen_date", r"\b(?:specimen|collected|collection)\s*(?:date|on)?[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})"),
    ("reported_date", r"\b(?:reported|resulted|date of mailing|issue date)\s*(?:date|on)?[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})"),
    ("service_date", r"\b(?:service date|date of service|visit date|dos)[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})"),
    ("effective_date", r"\b(?:effective date|effective|policy period|coverage period)[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})"),
    ("expiration_date", r"\b(?:expiration date|expires|through|to)[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})"),
    ("statement_period", r"\b(?:statement period|billing period|period)[:\s]+([^\n]{6,80})"),
    ("document_date", r"^Date:\s*([^\n]+)"),
)


def is_high_stakes_query(question: str, plan: dict[str, Any] | None = None) -> bool:
    plan = plan or {}
    domain = str(plan.get("domain") or "").lower()
    if domain in HIGH_STAKES_DOMAINS:
        return True
    q = question.lower()
    return any(
        term in q
        for term in (
            "medical",
            "blood",
            "lab",
            "doctor",
            "diagnosis",
            "tax",
            "irs",
            "insurance",
            "policy",
            "deductible",
            "mortgage",
            "loan",
            "contract",
            "legal",
            "lease",
            "bank",
            "account",
            "balance",
            "va",
            "disability",
        )
    )


def build_evidence_item(result: dict[str, Any], rank: int, question: str = "") -> dict[str, Any]:
    content = str(result.get("content") or "")
    title = str(result.get("title") or "")
    doc_type = str(result.get("doc_type") or "")
    quality = infer_source_quality(title=title, doc_type=doc_type, content=content)
    date_signals = extract_date_signals(content=content, title=title, fallback_date=result.get("date"))
    exact_terms = exact_term_hits(question, f"{title} {doc_type} {content}")
    item_id = evidence_item_id(result)
    return {
        "id": item_id,
        "document_id": result.get("document_id"),
        "chunk_index": result.get("chunk_index", 0),
        "title": title,
        "doc_type": doc_type,
        "rank": rank,
        "retrieval_score": float(result.get("combined_score", result.get("similarity", result.get("rank_score", 0))) or 0),
        "exact_term_hits": exact_terms,
        "source_quality": quality,
        "date_signals": date_signals,
        "structured_fact_count": structured_fact_count(content),
        "excerpt": content[:1200],
        "content": content,
    }


def build_evidence_pack(
    question: str,
    plan: dict[str, Any],
    chunks: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    max_items: int = 60,
) -> dict[str, Any]:
    seen = set()
    items = []
    for result in chunks:
        key = (result.get("document_id"), result.get("chunk_index", 0))
        if key in seen:
            continue
        seen.add(key)
        items.append(build_evidence_item(result, len(items) + 1, question=question))
        if len(items) >= max_items:
            break

    docs: dict[int, dict[str, Any]] = {}
    for item in items:
        doc_id = item.get("document_id")
        if doc_id is None:
            continue
        doc = docs.setdefault(
            int(doc_id),
            {
                "document_id": int(doc_id),
                "title": item.get("title"),
                "doc_type": item.get("doc_type"),
                "chunks": 0,
                "best_quality_score": 0.0,
                "best_quality_tier": "unknown",
                "date_signals": defaultdict(list),
                "structured_fact_count": 0,
            },
        )
        doc["chunks"] += 1
        doc["structured_fact_count"] += int(item.get("structured_fact_count") or 0)
        score = float(item.get("source_quality", {}).get("score") or 0)
        if score > doc["best_quality_score"]:
            doc["best_quality_score"] = score
            doc["best_quality_tier"] = item.get("source_quality", {}).get("tier", "unknown")
        for kind, values in (item.get("date_signals") or {}).items():
            doc["date_signals"][kind].extend(values[:3])

    source_doc_ids = {s.get("document_id") for s in sources if s.get("document_id") is not None}
    source_quality_scores = [float(i.get("source_quality", {}).get("score") or 0) for i in items]
    direct_items = [i for i in items if i.get("source_quality", {}).get("tier") in {"original", "direct"}]
    date_signal_counts = defaultdict(int)
    for item in items:
        for kind, values in (item.get("date_signals") or {}).items():
            date_signal_counts[kind] += len(values)

    source_documents = []
    for doc in docs.values():
        doc["date_signals"] = {k: list(dict.fromkeys(v))[:8] for k, v in doc["date_signals"].items()}
        source_documents.append(doc)
    source_documents.sort(key=lambda d: (d["best_quality_score"], d["chunks"]), reverse=True)

    coverage = {
        "evidence_item_count": len(items),
        "source_document_count": len(docs),
        "citation_document_count": len(source_doc_ids),
        "direct_source_count": len(direct_items),
        "average_source_quality": round(sum(source_quality_scores) / len(source_quality_scores), 3) if source_quality_scores else 0,
        "structured_fact_count": sum(int(i.get("structured_fact_count") or 0) for i in items),
        "date_signal_counts": dict(date_signal_counts),
        "high_stakes": is_high_stakes_query(question, plan),
    }
    return {
        "question": question,
        "mode": plan.get("mode"),
        "domain": plan.get("domain"),
        "intent": plan.get("intent"),
        "coverage": coverage,
        "source_documents": source_documents[:30],
        "items": items,
    }


def format_evidence_pack_for_llm(pack: dict[str, Any], max_items: int = 42, max_chars: int = 26000) -> str:
    if not pack:
        return ""
    lines = [
        "Evidence pack:",
        f"- Domain: {pack.get('domain') or 'unknown'}",
        f"- Intent: {pack.get('intent') or 'unknown'}",
        f"- Coverage: {pack.get('coverage')}",
        "",
    ]
    for item in (pack.get("items") or [])[:max_items]:
        quality = item.get("source_quality") or {}
        dates = item.get("date_signals") or {}
        header = (
            f"[Evidence {item.get('id')} | doc {item.get('document_id')} chunk {item.get('chunk_index')} | "
            f"{item.get('title') or 'Untitled'} | {item.get('doc_type') or 'unknown'} | "
            f"quality={quality.get('tier')}:{quality.get('score')} | dates={dates}]"
        )
        lines.append(header)
        lines.append(str(item.get("excerpt") or "")[:1600])
        lines.append("")
        if len("\n".join(lines)) >= max_chars:
            break
    return "\n".join(lines)[:max_chars]


def infer_source_quality(title: str, doc_type: str, content: str) -> dict[str, Any]:
    text = f"{title} {doc_type} {content[:1200]}".lower()
    score = 0.55
    reasons = []
    tier = "context"

    if any(term in text for term in DIRECT_SOURCE_TERMS):
        score = 0.88
        tier = "original"
        reasons.append("looks like an original/source document")
    if any(term in text for term in SUMMARY_SOURCE_TERMS):
        score = min(score, 0.68)
        tier = "summary"
        reasons.append("looks like a summary or portal rollup")
    if "requisition" in text and "result" not in text:
        score = min(score, 0.45)
        tier = "request"
        reasons.append("looks like an order/request rather than final result")
    if doc_type:
        score += 0.03
        reasons.append(f"typed as {doc_type}")
    if re.search(r"\|.*\|", content[:2000]) or structured_fact_count(content[:3000]) > 0:
        score += 0.06
        reasons.append("contains structured values")

    score = max(0.0, min(1.0, score))
    if score >= 0.82:
        tier = "original" if tier == "context" else tier
    elif score >= 0.66 and tier == "context":
        tier = "direct"
    elif score < 0.5:
        tier = "weak"
    return {"score": round(score, 3), "tier": tier, "reasons": reasons[:4]}


def extract_date_signals(content: str, title: str = "", fallback_date: Any = None) -> dict[str, list[str]]:
    haystack = f"{title}\n{content or ''}"
    signals: dict[str, list[str]] = defaultdict(list)
    if fallback_date:
        signals["document_date"].append(str(fallback_date))
    for kind, pattern in DATE_PATTERNS:
        for match in re.finditer(pattern, haystack, flags=re.I | re.M):
            value = " ".join(match.group(1).strip().split())
            if value and value not in signals[kind]:
                signals[kind].append(value)
            if len(signals[kind]) >= 8:
                break
    for match in re.finditer(r"\b(20\d{2}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/20\d{2}|[A-Z][a-z]+ \d{1,2}, 20\d{2})\b", haystack):
        value = match.group(1)
        if value not in signals["mentioned_date"]:
            signals["mentioned_date"].append(value)
        if len(signals["mentioned_date"]) >= 12:
            break
    return dict(signals)


def structured_fact_count(content: str) -> int:
    if not content:
        return 0
    table_rows = sum(1 for line in content.splitlines() if line.strip().startswith("|") and line.count("|") >= 2)
    numeric_facts = len(re.findall(
        r"""
        (?:
            \$\s*\d[\d,]*(?:\.\d+)?
          | \b\d[\d,]*(?:\.\d+)?\s*
            (?:
                %
              | [a-z]{1,8}(?:/[a-z]{1,8})?
              | months?
              | years?
              | days?
              | miles?
              | gallons?
              | kwh
              | sq\.?\s*ft\.?
            )\b
        )
        """,
        content,
        flags=re.I | re.X,
    ))
    identifier_facts = len(re.findall(
        r"""
        \b(?:
            (?:account|acct|policy|invoice|claim|case|reference|ref|serial|vin|ein|ssn|routing|member|customer|order|id)
            \s*(?:number|no\.?|\#)?\s*[:\#-]?\s*
            [a-z0-9][a-z0-9-]{2,}
          | [a-z]{1,6}[- ]?\d{2,}[a-z0-9-]*
          | (?=[a-hj-npr-z0-9]{8,17}\b)(?=[a-hj-npr-z0-9]*\d)[a-hj-npr-z0-9]+
        )\b
        """,
        content,
        flags=re.I | re.X,
    ))
    labeled_values = len(re.findall(r"\b[A-Za-z][A-Za-z0-9 /()%.-]{2,50}:\s*\$?\d", content))
    return min(80, table_rows + numeric_facts + identifier_facts + labeled_values)


def exact_term_hits(question: str, text: str) -> int:
    return len(exact_term_matches(question, text))


def exact_term_matches(question: str, text: str) -> set[str]:
    return query_terms(question) & _signal_terms(text)


def query_terms(question: str) -> set[str]:
    return {
        term
        for term in _signal_terms(question)
        if term not in QUERY_STOPWORDS and len(term) >= 2
    }


def _signal_terms(text: str) -> set[str]:
    """Tokenize user/source text into comparable exact-match terms.

    This keeps short codes and acronyms useful across domains: IGF, TSH, VIN,
    EIN, W-2, K-1, 1099, SKU, SSN, VA, etc. Matching is token-based instead of
    substring-based, so short terms do not fire inside unrelated words.
    """
    terms: set[str] = set()
    for raw in re.findall(r"[a-z0-9]+(?:[-/][a-z0-9]+)*", text.lower()):
        terms.add(raw)
        compact = re.sub(r"[-/]", "", raw)
        if len(compact) >= 2:
            terms.add(compact)
        for part in re.split(r"[-/]", raw):
            if len(part) >= 2:
                terms.add(part)
    return terms


def evidence_item_id(result: dict[str, Any]) -> str:
    raw = f"{result.get('document_id')}:{result.get('chunk_index', 0)}:{result.get('title', '')}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


def normalize_claim_ledger(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"claims": [], "summary": {"supported": 0, "partial": 0, "unsupported": 0, "conflicting": 0}}
    claims = []
    for item in raw.get("claims", []) if isinstance(raw.get("claims"), list) else []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("support_status") or item.get("status") or "unknown").lower()
        if status not in {"supported", "partial", "unsupported", "conflicting", "unknown"}:
            status = "unknown"
        claims.append({
            "claim": str(item.get("claim") or "")[:500],
            "support_status": status,
            "document_id": item.get("document_id"),
            "source_title": item.get("source_title"),
            "evidence_id": item.get("evidence_id"),
            "evidence_excerpt": str(item.get("evidence_excerpt") or "")[:500],
            "date": item.get("date"),
            "source_quality": item.get("source_quality"),
            "notes": str(item.get("notes") or "")[:300],
        })
    summary = {key: 0 for key in ("supported", "partial", "unsupported", "conflicting", "unknown")}
    for claim in claims:
        summary[claim["support_status"]] += 1
    return {"claims": claims[:80], "summary": summary}


def claim_ledger_from_verification(verification: dict[str, Any] | None) -> dict[str, Any]:
    verification = verification or {}
    claims = []
    for value in verification.get("supported_claims") or []:
        claims.append({
            "claim": str(value)[:500],
            "support_status": "supported",
            "notes": "From verifier supported claims",
        })
    for value in verification.get("unsupported_claims") or []:
        claims.append({
            "claim": str(value)[:500],
            "support_status": "unsupported",
            "notes": "From verifier unsupported claims",
        })
    for value in verification.get("stale_or_conflicting_claims") or []:
        claims.append({
            "claim": str(value)[:500],
            "support_status": "conflicting",
            "notes": "From verifier stale/conflicting claims",
        })
    return normalize_claim_ledger({"claims": claims})


def repair_queries_from_verification(question: str, verification: dict[str, Any], limit: int = 5) -> list[str]:
    parts = []
    for key in ("unsupported_claims", "missing_evidence", "stale_or_conflicting_claims"):
        for value in verification.get(key) or []:
            if value:
                parts.append(str(value))
    queries = []
    for part in parts:
        cleaned = re.sub(r"[^A-Za-z0-9 $%./#:-]+", " ", part)
        cleaned = " ".join(cleaned.split())[:180]
        if cleaned:
            queries.append(f"{question} source evidence {cleaned}")
        if len(queries) >= limit:
            break
    return list(dict.fromkeys(queries))


def answer_needs_repair(verification: dict[str, Any]) -> bool:
    return bool(
        (verification.get("unsupported_claims") or [])
        or (verification.get("stale_or_conflicting_claims") or [])
    )
