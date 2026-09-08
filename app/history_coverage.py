"""Bounded historical source reservations; metadata guides retrieval, never proof."""
from collections import defaultdict
import re
from app.evidence import query_terms, infer_source_quality
from app.source_dates import source_dates, without_dates

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


def _calendar_dates(text, date_order):
    dates = []
    for found in source_dates(text, date_order):
        if not found.value:
            continue
        if found.precision == "year":
            before, after = text[max(0, found.start-40):found.start], text[found.end:]
            if not re.search(r"\b(?:calendar year|year|dated|date|period|term)\s*:?\s*$", before, re.I):
                continue
            if re.match(r"\s*(?:[$€£%]|USD|EUR|GBP|CAD|AUD|JPY|mg|kg|g|ml|mL|years?|months?|weeks?|days?|hours?|minutes?|seconds?)(?![A-Za-z])", after, re.I):
                continue
        dates.append(found)
    return dates


def _records(candidates, question, date_order):
    subjects = set(subject_terms(question))
    records = []
    for row in candidates:
        title = str(row.get("title") or "")
        preview = str(row.get("preview") or "")
        terms = query_terms(title + " " + str(row.get("doc_type") or "") + " " + preview)
        if not (subjects & terms):
            continue
        title_dates = _calendar_dates(title, date_order)
        source = _calendar_dates(preview, date_order)
        indexed = source_dates(str(row.get("indexed_date") or "")[:10], date_order)
        indexed = [d for d in indexed if d.value]
        dates = [d for d in title_dates if d.precision != "year"] or source or indexed or title_dates
        # First recorded metadata date is a diversity signal, not an assertion
        # of effective status; synthesis and audit must establish its meaning.
        period = dates[0].value if dates else ""
        records.append({**row, "period": period,
                        "metadata_relevance": len(subjects & query_terms(title + " " + str(row.get("doc_type") or ""))),
                        "subjects": tuple(sorted(subjects & query_terms(title + " " + str(row.get("doc_type") or ""))) or sorted(subjects & terms)),
                        "quality": infer_source_quality(title, "", "")["score"],
                        "relevance": len(subjects & terms)})
    return records


def choose_recent_documents(candidates, question, *, date_order="mdy", limit=MAX_DOCUMENTS, diagnostics=None):
    """Reserve distinct recent records; recency and titles are only hints."""
    records = _records(candidates, question, date_order)
    # Month-level ordering puts day-specific and month-only records in the
    # same recent cohort. Stable title families keep repeated notices from
    # consuming every opportunity before another recent record appears.
    groups = defaultdict(list)
    for row in records:
        title_text = str(row.get("title") or "")
        title = tuple(sorted(query_terms(without_dates(title_text, _calendar_dates(title_text, date_order)))))
        groups[(str(row.get("doc_type") or "unknown"), title or (row["document_id"],))].append(row)
    def rank(row):
        parts = row["period"][:7].split("-") if row["period"] else []
        year, month = (int(parts[0]) if parts else 0), (int(parts[1]) if len(parts)>1 else 0)
        return (-row["metadata_relevance"], -year, -month, -row["quality"], row["document_id"])
    queues = []
    for rows in groups.values():
        ordered = sorted(rows, key=rank)
        cohort = ordered[0]["period"][:7]
        queues.append([row for row in ordered if row["period"][:7] == cohort])
    queues.sort(key=lambda rows: rank(rows[0]))
    chosen = []
    while queues and len(chosen) < limit:
        for queue in queues:
            if queue and len(chosen) < limit:
                chosen.append(queue.pop(0))
        queues = [queue for queue in queues if queue]
    if diagnostics is not None:
        diagnostics.update(relevant_candidate_count=len(records), selected_document_count=len(chosen),
                           omitted_document_count=len(records)-len(chosen))
    return [{"document_id":row["document_id"], "period":row["period"],
             "doc_type":row.get("doc_type") or "unknown"} for row in chosen]


def choose_documents(candidates: list[dict], question: str, *, date_order="mdy", limit=MAX_DOCUMENTS, diagnostics: dict | None = None) -> list[dict]:
    records = _records(candidates, question, date_order)
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
    # Reserve directly matched strata first, then source-only strata before
    # repeated representatives. Missing metadata never silently erases history.
    keys = sorted(groups, key=lambda key: (not any(row["metadata_relevance"] for row in groups[key]),
                                          period_order.index(key[2]), key[0], key[1]))
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
