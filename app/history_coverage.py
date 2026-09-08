"""Bounded historical source reservations; metadata guides retrieval, never proof."""
from collections import defaultdict
import re
from app.evidence import query_terms, infer_source_quality
from app.source_dates import source_dates

MAX_CANDIDATES = 500
MAX_DOCUMENTS = 8
HISTORY_WORDS = {"history", "historical", "timeline", "chronological", "changed", "changes", "change", "over", "years", "year", "most", "recent", "records", "record", "previous", "earlier", "since", "through", "time", "across", "compare", "comparison"}


def history_requested(question: str, plan: dict, mode: str) -> bool:
    return mode == "timeline" or bool(re.search(r"\b(?:history|historical|timeline|over (?:the )?(?:years|time)|changed|changes)\b", question, re.I)) or any(
        isinstance(q, dict) and q.get("role") == "historical_timeline" for q in plan.get("subqueries", []))


def subject_terms(question: str) -> list[str]:
    terms = query_terms(question) - HISTORY_WORDS
    # Ordinary morphology only, so unknown subjects do not need a domain map.
    terms |= {term[:-1] for term in terms if term.endswith("s") and len(term) > 3}
    return sorted(term for term in terms if not term.isdigit())[:24]


def choose_documents(candidates: list[dict], question: str, *, date_order="mdy", limit=MAX_DOCUMENTS, diagnostics: dict | None = None) -> list[dict]:
    subjects = set(subject_terms(question))
    records = []
    for row in candidates:
        title = str(row.get("title") or "")
        preview = str(row.get("preview") or "")
        terms = query_terms(title + " " + str(row.get("doc_type") or "") + " " + preview)
        if not (subjects & terms):
            continue
        title_dates = [d for d in source_dates(title, date_order) if d.value]
        source = [d for d in source_dates(preview, date_order) if d.value and (d.precision != "year" or
                  re.search(r"\b(?:calendar year|year|dated|date|period|term)\s*:?\s*$", preview[max(0, d.start-30):d.start], re.I))]
        indexed = source_dates(str(row.get("indexed_date") or "")[:10], date_order)
        indexed = [d for d in indexed if d.value]
        dates = [d for d in title_dates if d.precision != "year"] or source or indexed or title_dates[-1:]
        # First recorded metadata date is a diversity signal, not an assertion
        # of effective status; synthesis and audit must establish its meaning.
        period = dates[0].value if dates else ""
        records.append({**row, "period": period,
                        "metadata_relevance": len(subjects & query_terms(title + " " + str(row.get("doc_type") or ""))),
                        "subjects": tuple(sorted(subjects & query_terms(title + " " + str(row.get("doc_type") or ""))) or sorted(subjects & terms)),
                        "quality": infer_source_quality(title, "", "")["score"],
                        "relevance": len(subjects & terms)})
    # An incidental footer mention must not displace documents whose title or
    # indexed type identifies the user's subject. Unknown subjects still use
    # the source-preview fallback when no metadata match exists.
    metadata = [row for row in records if row["metadata_relevance"]]
    if metadata:
        records = metadata
    # Reserve temporal/type strata before repeated revisions. Title vocabulary
    # never partitions source families: added issuer/form words must not strand
    # an older short-title record in a low-population singleton group.
    groups = defaultdict(list)
    for row in records:
        group = (str(row.get("doc_type") or "unknown"), row["subjects"], row["period"][:4])
        groups[group].append(row)
    periods = sorted({key[2] for key in groups if key[2]})
    period_order = []
    while periods:
        period_order.append(periods.pop(0))
        if periods:
            period_order.append(periods.pop())
    period_order.append("")
    keys = sorted(groups, key=lambda key: (period_order.index(key[2]), key[0], key[1]))
    queues = []
    for key in keys:
        rows = groups[key]
        # Prefer direct source genres within each stratum. Stable calendar
        # endpoints then determine order; document count gives no advantage.
        dated = sorted(rows, key=lambda row: (row["period"], row["document_id"]))
        newest = bool(key[2]) and key[2] == max((k[2] for k in groups), default="")
        period_rank = {period: index for index, period in enumerate(sorted({row["period"] for row in rows}))}
        primary = sorted(rows, key=lambda row: (-row["metadata_relevance"], -row["quality"],
                         period_rank[row["period"]] * (-1 if newest else 1), row["document_id"]))
        queues.append([primary[0]] + ([dated[0], dated[-1]] if newest else [dated[-1], dated[0]]) + primary[1:])
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
    if diagnostics is not None:
        omitted = [key for key, rows in groups.items() if not any(row["document_id"] in seen for row in rows)]
        diagnostics.update(relevant_candidate_count=len(records), bucket_count=len(groups),
                           selected_bucket_count=len(groups)-len(omitted), omitted_bucket_count=len(omitted),
                           omitted_buckets=[{"doc_type": key[0], "subjects": list(key[1]), "period": key[2]} for key in sorted(omitted)[:32]])
    return result
