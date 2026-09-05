"""Calendar and provenance validation for public timeline events."""
from app.answer_finalization import date_occurs, evidence_spans, parse_date, select_spans, validate_reference, values_match


async def validate_timeline(events: list, pack: dict, auditor, question: str, *, manifest: list[dict] | None = None) -> list[dict]:
    if not isinstance(events, list):
        return []
    try:
        spans = evidence_spans(pack)
    except (ValueError, TypeError):
        return []
    if manifest is not None:
        if not isinstance(manifest, list):
            return []
        supplied = {span.get("span_id"): span for span in manifest if isinstance(span, dict)}
        spans = [span for span in spans if span["span_id"] in supplied and all(
            supplied[span["span_id"]].get(key) == span[key]
            for key in ("content_digest", "document_id", "evidence_id", "content", "start", "end"))]
    accepted = []
    for event in events[:30]:
        if not isinstance(event, dict):
            continue
        if not isinstance(event.get("title", ""), str) or not isinstance(event.get("summary", ""), str):
            continue
        parsed = parse_date(event.get("date"))
        refs = event.get("references")
        if not parsed or type(event.get("document_id")) is not int or event["document_id"] < 1 or not isinstance(refs, list) or not refs:
            continue
        validated = [validate_reference(r, spans) for r in refs]
        if not all(validated) or any(r["document_id"] != event.get("document_id") for r in validated):
            continue
        # Honest ISO precision must occur in the quote. Conversion of free-form
        # dates can be added through a tested parser; never invent day precision.
        if not any(date_occurs(parsed[0], r["quote"]) for r in validated):
            continue
        text = f"{parsed[0]}: {event.get('title', '')}. {event.get('summary', '')}"
        selected = [s for s in spans if any(r["span_id"] == s["span_id"] for r in validated)]
        used = sum(len(span["content"]) for span in selected)
        if used > 28000:
            continue
        # The proposer cannot hide supplied contradictory sources by citing
        # only its preferred record. Include other relevant manifest spans.
        for span in select_spans(question, [{"text": text}], spans):
            if span not in selected and used + len(span["content"]) <= 28000:
                selected.append(span)
                used += len(span["content"])
        try:
            audit = await auditor.audit_answer_units(question, [{"id": "event", "text": text, "start": 0, "end": len(text)}], selected, {})
        except Exception:
            continue
        assessments = audit.get("assessments", []) if isinstance(audit, dict) else []
        if not isinstance(assessments, list) or len(assessments) != 1 or not isinstance(assessments[0], dict) or assessments[0].get("unit_id") != "event" or assessments[0].get("status") != "supported":
            continue
        raw_refs = assessments[0].get("references")
        if not isinstance(raw_refs, list):
            continue
        audit_refs = [validate_reference(r, selected) for r in raw_refs]
        if not audit_refs or not all(audit_refs) or any(r["document_id"] != event["document_id"] for r in audit_refs) or not any(date_occurs(parsed[0], r["quote"]) for r in audit_refs) or not values_match(text, audit_refs):
            continue
        accepted.append({"date": parsed[0], "precision": parsed[1], "title": str(event.get("title", "")),
                         "summary": str(event.get("summary", "")), "document_id": event["document_id"],
                         "source_title": audit_refs[0]["source_title"], "references": audit_refs,
                         "status": "source_supported"})
    return sorted(accepted, key=lambda e: (e["date"], e["document_id"], e["title"]))
