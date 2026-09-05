"""Lossless per-document relationship support, including legacy scalar sources."""
import json


def support_records(props: dict) -> dict[int, dict]:
    records = {}
    for raw in props.get("support_records") or []:
        record = json.loads(raw) if isinstance(raw, str) else dict(raw)
        if type(record.get("source_doc")) is int:
            records[record["source_doc"]] = record
    legacy = props.get("source_doc")
    if type(legacy) is int and legacy not in records:
        records[legacy] = {k: v for k, v in props.items() if k not in {"support_records", "source_doc_ids", "weight"}}
    for source in props.get("source_doc_ids") or []:
        if type(source) is int and source not in records:
            records[source] = {"source_doc": source}
    for record in records.values():
        record["inferred"] = bool(record.get("inferred") or record.get("implied"))
        # Ingestion and older indexes used evidence_json. Normalize before
        # merging so every quote follows the same per-document union policy.
        if "evidence_json" in record or "evidence_spans" in record:
            spans = []
            for key in ("evidence_spans", "evidence_json"):
                value = record.get(key)
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        value = [value]
                for span in value if isinstance(value, list) else [value] if value else []:
                    if span not in spans:
                        spans.append(span)
            record["evidence_spans"] = spans
            record.pop("evidence_json", None)
    return records


def merge_support_properties(*properties: dict) -> dict:
    result, records = {}, {}
    for props in properties:
        result.update({k: v for k, v in props.items() if k not in {"support_records", "source_doc_ids", "source_doc", "weight"}})
        for doc_id, incoming in support_records(props).items():
            old = records.get(doc_id)
            if old:
                combined = {**old, **incoming}
                combined["inferred"] = bool(old.get("inferred") or incoming.get("inferred"))
                confidences = [r["confidence"] for r in (old, incoming) if isinstance(r.get("confidence"), (int, float))]
                if confidences:
                    combined["confidence"] = min(confidences)
                for key in ("evidence_spans", "rationale"):
                    vals = []
                    for r in (old, incoming):
                        value = r.get(key)
                        if isinstance(value, str) and key == "evidence_spans":
                            try:
                                value = json.loads(value)
                            except ValueError:
                                value = [value]
                        for v in value if isinstance(value, list) else [value] if value else []:
                            if v not in vals:
                                vals.append(v)
                    if vals:
                        combined[key] = vals
                records[doc_id] = combined
            else:
                records[doc_id] = dict(incoming)
    if records:
        # Aggregate quote fields cannot identify their source when several
        # documents support one edge. The per-document records are authoritative.
        result.pop("evidence_json", None)
        result.pop("evidence_spans", None)
        result["source_doc_ids"] = sorted(records)
        result["support_records"] = [json.dumps(records[k], sort_keys=True, ensure_ascii=False) for k in sorted(records)]
        result["weight"] = len(records)
        result["inferred"] = any(r.get("inferred") for r in records.values())
        result["implied"] = result["inferred"]
        confidences = [r["confidence"] for r in records.values() if isinstance(r.get("confidence"), (int, float))]
        if len(confidences) == len(records):
            result["confidence"] = min(confidences)
        else:
            result.pop("confidence", None)
        # Preserve scalar compatibility only for an unambiguous single source.
        if len(records) == 1:
            result["source_doc"] = next(iter(records))
            single = next(iter(records.values()))
            if "evidence_spans" in single:
                result["evidence_spans"] = single["evidence_spans"]
    # Neo4j properties cannot contain nested maps or heterogeneous object lists.
    return {k: json.dumps(v, ensure_ascii=False) if isinstance(v, dict) or
            (isinstance(v, list) and any(isinstance(x, (dict, list)) for x in v)) else v
            for k, v in result.items() if v is not None}
