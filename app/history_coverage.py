"""Bounded historical source reservations; metadata guides retrieval, never proof."""
from collections import defaultdict
from itertools import zip_longest
import re
from app.evidence import query_terms, infer_source_quality
from app.source_dates import source_dates, without_dates, calendar_year_context

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
            if not calendar_year_context(text[:found.start], text[found.end:]):
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


def _share_date_precision(rows):
    """Give overlapping partial dates opportunities beside fully dated rows."""
    years = defaultdict(list)
    for row in rows:
        years[row["period"][:4]].append(row)
    ordered = []
    def interleave(left, right):
        return [row for pair in zip_longest(left, right) for row in pair if row is not None]
    for year in sorted(years, reverse=True):
        months, partial_year = defaultdict(list), []
        for row in years[year]:
            if len(row["period"]) < 7:
                partial_year.append(row)
            else:
                months[row["period"][:7]].append(row)
        dated = []
        for month in sorted(months, reverse=True):
            full = [row for row in months[month] if len(row["period"]) > 7]
            partial = [row for row in months[month] if len(row["period"]) == 7]
            dated.extend(interleave(full, partial))
        ordered.extend(interleave(dated, partial_year))
    return ordered


def choose_recent_documents(candidates, question, *, date_order="mdy", limit=MAX_DOCUMENTS, diagnostics=None):
    """Reserve distinct recent records; recency and titles are only hints."""
    records = _records(candidates, question, date_order)
    # Title/precision queues are diversity hints. Exact dates order records
    # within each queue; partial dates retain their own opportunity.
    groups = defaultdict(list)
    for row in records:
        title_text = str(row.get("title") or "")
        title = tuple(sorted(query_terms(without_dates(title_text, _calendar_dates(title_text, date_order)))))
        precision = len(row["period"].split("-")) if row["period"] else 0
        groups[(str(row.get("doc_type") or "unknown"), title or (row["document_id"],), precision)].append(row)
    def rank(row):
        parts = row["period"].split("-") if row["period"] else []
        calendar = [int(part) for part in parts] + [0] * (3-len(parts))
        return (*(-part for part in calendar), -row["metadata_relevance"], -row["quality"], row["document_id"])
    # Share opportunities across title/precision queues, but never treat a
    # matching title as proof that another dated record is superseded.
    queues = [sorted(rows, key=rank) for rows in groups.values()]
    queues.sort(key=lambda rows: rank(rows[0]))
    chosen = []
    while queues and len(chosen) < limit:
        wave = _share_date_precision([queue.pop(0) for queue in queues if queue])
        chosen.extend(wave[:limit-len(chosen)])
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
