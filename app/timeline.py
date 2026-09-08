"""Calendar and provenance validation for public timeline events."""
import json
from app.answer_finalization import date_occurs, evidence_spans, parse_date, select_spans, validate_reference, values_match


async def validate_timeline(events: list, pack: dict, auditor, question: str, *, manifest: list[dict] | None = None,
                            date_order: str = "mdy", diagnostics: dict | None = None) -> list[dict]:
    counts = diagnostics if diagnostics is not None else {}
    def reject(reason, count=1):
        counts[reason] = counts.get(reason, 0) + count
    if not isinstance(events, list):
        reject("invalid_result")
        return []
    try:
        spans = evidence_spans(pack)
    except (ValueError, TypeError):
        reject("invalid_evidence")
        return []
    if manifest is not None:
        if not isinstance(manifest, list):
            reject("invalid_manifest")
            return []
        supplied = {span.get("span_id"): span for span in manifest if isinstance(span, dict)}
        spans = [span for span in spans if span["span_id"] in supplied and all(
            supplied[span["span_id"]].get(key) == span[key]
            for key in ("content_digest", "document_id", "evidence_id", "content", "start", "end"))]
    accepted = []
    if len(events) > 30:
        reject("event_limit", len(events) - 30)
    for event in events[:30]:
        if not isinstance(event, dict) or not isinstance(event.get("title", ""), str) or not isinstance(event.get("summary", ""), str):
            reject("invalid_event")
            continue
        parsed = parse_date(event.get("date"))
        if not parsed:
            reject("invalid_date")
            continue
        refs = event.get("references")
        if type(event.get("document_id")) is not int or event["document_id"] < 1 or not isinstance(refs, list) or not refs:
            reject("invalid_reference")
            continue
        validated = [validate_reference(r, spans) for r in refs]
        if not all(validated) or any(r["document_id"] != event.get("document_id") for r in validated):
            reject("invalid_reference")
            continue
        if not any(date_occurs(parsed[0], r["quote"], date_order, context_before=r.get("date_context_before", "")) for r in validated):
            reject("unsupported_date")
            continue
        text = f"{parsed[0]}: {event.get('title', '')}. {event.get('summary', '')}"
        selected = [s for s in spans if any(r["span_id"] == s["span_id"] for r in validated)]
        used = sum(len(json.dumps(span, ensure_ascii=False)) + 2 for span in selected)
        if used > 28000:
            reject("evidence_budget")
            continue
        # Proposer-selected references cannot hide supplied conflicting sources.
        for span in select_spans(question, [{"text": text}], spans, serialized=True, date_order=date_order):
            cost = len(json.dumps(span, ensure_ascii=False)) + 2
            if span not in selected and used + cost <= 28000:
                selected.append(span)
                used += cost
        try:
            audit = await auditor.audit_answer_units(question, [{"id": "event", "text": text, "start": 0, "end": len(text)}],
                                                     selected, {"source_date_order": date_order})
        except Exception:
            reject("audit_unavailable")
            continue
        assessments = audit.get("assessments", []) if isinstance(audit, dict) else []
        if not isinstance(assessments, list) or len(assessments) != 1 or not isinstance(assessments[0], dict) or assessments[0].get("unit_id") != "event" or assessments[0].get("status") != "supported":
            reject("unsupported_event")
            continue
        raw_refs = assessments[0].get("references")
        if not isinstance(raw_refs, list):
            reject("invalid_audit_reference")
            continue
        audit_refs = [validate_reference(r, selected) for r in raw_refs]
        if not audit_refs or not all(audit_refs) or any(r["document_id"] != event["document_id"] for r in audit_refs):
            reject("invalid_audit_reference")
            continue
        if not any(date_occurs(parsed[0], r["quote"], date_order, context_before=r.get("date_context_before", "")) for r in audit_refs):
            reject("unsupported_audited_date")
            continue
        if not values_match(text, audit_refs, date_order=date_order):
            reject("value_mismatch")
            continue
        accepted.append({"date": parsed[0], "precision": parsed[1], "title": str(event.get("title", "")),
                         "summary": str(event.get("summary", "")), "document_id": event["document_id"],
                         "source_title": audit_refs[0]["source_title"], "references": audit_refs,
                         "status": "source_supported"})
    return sorted(accepted, key=lambda e: (e["date"], e["document_id"], e["title"]))
