"""Deterministic source-window validation and reconciliation for extraction."""

from __future__ import annotations

import json
import math
import re
from typing import Any

ENTITY_TYPES = {
    "Person", "Organization", "Location", "System", "Product", "Document",
    "Event", "Condition", "FinancialItem", "InsurancePolicy", "Contract",
    "DateEvent", "Address",
}


def source_windows(content: str, size: int, overlap: int):
    start = 0
    while start < len(content):
        end = min(len(content), start + size)
        yield start, end, content[start:end]
        if end == len(content):
            break
        start = end - overlap


def source_span(quote: Any, source: str, offset: int) -> dict | None:
    """Locate a quote without altering its punctuation, digits, or units."""
    if not isinstance(quote, str) or not quote.strip():
        return None
    pattern = r"\s+".join(re.escape(word) for word in quote.split())
    match = re.search(pattern, source)
    if match is None:
        return None
    return {"start": offset + match.start(), "end": offset + match.end(), "quote": match.group()}


def named_mention(name: str, text: str) -> bool:
    pattern = r"\s+".join(re.escape(word) for word in name.split())
    return bool(pattern and re.search(r"(?<!\w)" + pattern + r"(?!\w)", text, re.I))


def literal_value_present(value: Any, quote: str) -> bool:
    literal = str(value).lower() if isinstance(value, bool) else str(value)
    if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)*", literal):
        # A scalar 100 must not match 100.25, -100, or a portion of 1,100.
        # Formatting conversion belongs to an explicitly typed normalizer.
        return bool(re.search(r"(?<![\w.,+\-])" + re.escape(literal) + r"(?![\w.,])", quote))
    return named_mention(literal, quote)


def confidence(value: Any) -> float:
    try:
        number = float(value)
    except (ValueError, TypeError):
        return 0.0
    return number if math.isfinite(number) and 0 <= number <= 1 else 0.0


def validate_entities(raw: list[dict], source: str, offset: int, issues: list[str]) -> list[dict]:
    accepted = []
    for candidate in raw:
        name = candidate.get("name")
        kind = candidate.get("type")
        span = source_span(candidate.get("evidence_quote"), source, offset)
        if not isinstance(name, str) or not name.strip() or kind not in ENTITY_TYPES:
            issues.append("Entity rejected: invalid name or type")
        elif not span or not named_mention(name, span["quote"]):
            issues.append(f"Entity rejected: no source mention for {name}")
        elif confidence(candidate.get("confidence")) < 0.8:
            issues.append(f"Entity rejected: low confidence for {name}")
        else:
            accepted.append({
                "name": name.strip(), "type": kind,
                "confidence": confidence(candidate.get("confidence")),
                "description": str(candidate.get("description") or ""),
                "evidence": [span],
            })
    return accepted


def validate_relationships(raw: list[dict], entities: list[dict], source: str, offset: int, issues: list[str]) -> list[dict]:
    names = {entity["name"] for entity in entities}
    accepted = []
    for rel in raw:
        start, end, kind = rel.get("from_entity"), rel.get("to_entity"), rel.get("relationship_type")
        span = source_span(rel.get("evidence_quote"), source, offset)
        rationale = rel.get("rationale") or rel.get("description")
        if not isinstance(start, str) or not isinstance(end, str) or start not in names or end not in names:
            issues.append("Relationship rejected: endpoint is not an accepted entity")
        elif not isinstance(kind, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", kind):
            issues.append("Relationship rejected: invalid type")
        elif not span or not named_mention(start, span["quote"]) or not named_mention(end, span["quote"]):
            issues.append("Relationship rejected: supporting quote does not contain both endpoints")
        elif not isinstance(rationale, str) or not rationale.strip() or confidence(rel.get("confidence")) < 0.8:
            issues.append("Relationship rejected: missing rationale or low confidence")
        else:
            accepted.append({
                "from_entity": start, "to_entity": end, "relationship_type": kind,
                "confidence": confidence(rel.get("confidence")), "rationale": rationale,
                "inferred": True, "evidence": [span],
            })
    return accepted


def merge_unique(items: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
    merged: dict[tuple, dict] = {}
    for item in items:
        key = tuple(str(item.get(field) or "").casefold() for field in key_fields)
        if key not in merged:
            merged[key] = dict(item, evidence=list(item.get("evidence") or []))
            continue
        existing = merged[key]
        for span in item.get("evidence") or []:
            if span not in existing["evidence"]:
                existing["evidence"].append(span)
        # Preserve uncertainty; repetition is not increased confidence.
        existing["confidence"] = min(existing["confidence"], item["confidence"])
        if "inferred" in existing:
            existing["inferred"] = existing["inferred"] or item.get("inferred", True)
    return list(merged.values())


def covered_characters(windows: list[dict]) -> int:
    total = end = 0
    for window in windows:
        if window["status"] != "complete":
            continue
        total += max(0, window["end"] - max(end, window["start"]))
        end = max(end, window["end"])
    return total


def validate_metadata(raw: dict, source: str, offset: int, issues: list[str]) -> tuple[dict, dict]:
    """Keep only metadata leaves with a matching source quote at their field path."""
    evidence = {}
    for ref in raw["evidence"]:
        span = source_span(ref.get("quote"), source, offset)
        path = ref.get("path")
        if isinstance(path, str) and span:
            evidence.setdefault(path, []).append(span)

    def visit(value, path):
        if isinstance(value, dict):
            return {key: visit(item, f"{path}.{key}" if path else key) for key, item in value.items()}
        if isinstance(value, list):
            return [visit(item, f"{path}.{i}") for i, item in enumerate(value)]
        if value is None:
            return None
        if path not in evidence:
            issues.append(f"Metadata omitted: no source quote for {path}")
            return None
        # Literal scalar support is deliberately conservative. Paraphrases and
        # converted/derived numbers require a separate assessment mechanism.
        if not any(literal_value_present(value, span["quote"]) for span in evidence[path]):
            issues.append(f"Metadata omitted: value not present in source quote for {path}")
            evidence.pop(path, None)
            return None
        return value

    return visit(raw["metadata"], ""), evidence


def reconcile_metadata(windows: list[tuple[dict, dict]]) -> tuple[dict, dict, list[dict]]:
    """Union distinct list rows; never overwrite disagreeing scalar fields."""
    result: dict = {}
    provenance: dict = {}
    conflicts: dict[str, list[Any]] = {}

    def merge(left, right, path):
        if path in conflicts:
            if right is not None and right not in conflicts[path]:
                conflicts[path].append(right)
            return None
        if left is None:
            return right
        if right is None or left == right:
            return left
        if isinstance(left, dict) and isinstance(right, dict):
            merged = dict(left)
            for key, value in right.items():
                child = f"{path}.{key}" if path else key
                merged[key] = merge(merged.get(key), value, child)
            return merged
        if isinstance(left, list) and isinstance(right, list):
            values = {json.dumps(value, sort_keys=True): value for value in left}
            values.update({json.dumps(value, sort_keys=True): value for value in right})
            return list(values.values())
        conflicts[path] = [left, right]
        return None

    for index, (metadata, evidence) in enumerate(windows):
        result = merge(result, metadata, "")
        # Original window paths remain stable even if list union changes indices.
        provenance[str(index)] = evidence
    return result, provenance, [{"path": path, "values": values} for path, values in conflicts.items()]
