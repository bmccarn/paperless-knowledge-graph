"""Bounded historical source reservations; metadata guides retrieval, never proof."""
from collections import Counter, defaultdict
import re
from app.evidence import query_terms
from app.source_dates import source_dates

MAX_CANDIDATES = 500
MAX_DOCUMENTS = 8
HISTORY_WORDS = {"history", "historical", "timeline", "chronological", "changed", "changes", "change", "over", "years", "year", "most", "recent", "records", "record", "previous", "earlier", "since", "through", "time", "across", "compare", "comparison"}
TITLE_WORDS = HISTORY_WORDS | {"updated", "page", "pages", "notice", "copy", "number", "date", "dated", "application", "declaration", "declarations"}


def history_requested(question: str, plan: dict, mode: str) -> bool:
    return mode == "timeline" or bool(re.search(r"\b(?:history|historical|timeline|over (?:the )?(?:years|time)|changed|changes)\b", question, re.I)) or any(
        isinstance(q, dict) and q.get("role") == "historical_timeline" for q in plan.get("subqueries", []))


def subject_terms(question: str) -> list[str]:
    terms = query_terms(question) - HISTORY_WORDS
    # Ordinary morphology only, so unknown subjects do not need a domain map.
    terms |= {term[:-1] for term in terms if term.endswith("s") and len(term) > 3}
    return sorted(term for term in terms if not term.isdigit())[:24]


def choose_documents(candidates: list[dict], question: str, *, date_order="mdy", limit=MAX_DOCUMENTS) -> list[dict]:
    subjects = set(subject_terms(question))
    records = []
    for row in candidates:
        title = str(row.get("title") or "")
        preview = str(row.get("preview") or "")
        terms = query_terms(title + " " + str(row.get("doc_type") or "") + " " + preview)
        if not (subjects & terms):
            continue
        title_dates = [d for d in source_dates(title, date_order) if d.value]
        source = [d for d in source_dates(preview, date_order) if d.value and d.precision != "year"]
        dates = [d for d in title_dates if d.precision != "year"] or title_dates[-1:] or source
        # First recorded metadata date is a diversity signal, not an assertion
        # of effective status; synthesis and audit must establish its meaning.
        period = dates[0].value if dates else ""
        masked_title = title
        for found in reversed(source_dates(title, date_order)):
            masked_title = masked_title[:found.start] + " " + masked_title[found.end:]
        hints = {t for t in query_terms(masked_title) - subjects - TITLE_WORDS if t.isalpha()}
        records.append({**row, "period": period, "hints": hints,
                        "relevance": len(subjects & terms)})
    # Shared title vocabulary refines existing type metadata, without a fixed
    # industry taxonomy or treating one-off identifiers as subject families.
    frequency = Counter(t for row in records for t in row["hints"])
    groups = defaultdict(list)
    for row in records:
        hints = sorted((t for t in row["hints"] if frequency[t] >= 2), key=lambda t: (-frequency[t], t))[:2]
        group = (str(row.get("doc_type") or "unknown"), tuple(hints))
        groups[group].append(row)
    ordered = sorted(groups.values(), key=lambda rows: (-max(r["relevance"] for r in rows), -len(rows), min(r["document_id"] for r in rows)))
    queues = []
    for rows in ordered:
        dated = sorted((r for r in rows if r["period"]), key=lambda r: (r["period"], r["document_id"]))
        undated = sorted((r for r in rows if not r["period"]), key=lambda r: r["document_id"])
        # Oldest/latest first, then one representative from each remaining
        # period. Repeated recent revisions cannot monopolize the reservation.
        by_period = {}
        for row in dated:
            by_period.setdefault(row["period"][:4], row)
        queues.append(([dated[0], dated[-1]] if dated else []) + list(by_period.values()) + undated)
    result, seen = [], set()
    while any(queues) and len(result) < limit:
        for queue in queues:
            while queue and queue[0]["document_id"] in seen:
                queue.pop(0)
            if queue and len(result) < limit:
                row = queue.pop(0)
                seen.add(row["document_id"])
                result.append({"document_id": row["document_id"], "period": row["period"],
                               "doc_type": row.get("doc_type") or "unknown"})
    return result
