"""Deterministic source-window validation and reconciliation for extraction."""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any

ENTITY_TYPES = {
    "Person", "Organization", "Location", "System", "Product", "Document",
    "Event", "Condition", "FinancialItem", "InsurancePolicy", "Contract",
    "DateEvent", "Address",
}
RECONCILIATION_VERSION = "review-admission-v2"


def source_name_key(name: str) -> str:
    """Only casing and whitespace at the proposal/review boundary."""
    return " ".join(name.split()).casefold()


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
            hint = candidate.get("identity_hint") or ""
            if not isinstance(hint, str) or (hint and not literal_value_present(hint, span["quote"])):
                issues.append("Entity rejected: identity discriminator is not source-grounded")
                continue
            name = " ".join(name.split())
            identity_id = entity_key(name, kind, hint)
            accepted.append({
                "entity_id": identity_id, "identity_hint": hint,
                "name": name, "type": kind,
                "confidence": confidence(candidate.get("confidence")),
                "description": str(candidate.get("description") or ""),
                "evidence": [span],
            })
    return accepted


def adjudicate_types(candidates, reviewed, raw_reviews, source, offset, issues):
    """Name-only alternate meanings cannot overwrite the proposed source type.

    A changed type needs a separate exact contextual quote plus rationale from
    the source-aware verifier. On insufficient evidence, keep the proposal's
    type AND description, not the reviewer's guessed alternate meaning.
    """
    accepted = []
    for entity in reviewed:
        originals = [item for item in candidates if source_name_key(item["name"]) == source_name_key(entity["name"])
                     and item.get("identity_hint", "") == entity.get("identity_hint", "")]
        originals = list({item["entity_id"]: item for item in originals}.values())
        same_type = [item for item in originals if item["type"] == entity["type"]]
        if len(same_type) == 1:
            # Keep the proposal's spelling/ID so all later receipt consumers see
            # the same identity, while retaining the independently reviewed span.
            entity = {**entity, "name": same_type[0]["name"], "entity_id": same_type[0]["entity_id"]}
            accepted.append(entity)
            continue
        if len(originals) != 1:
            issues.append("Entity verifier additions rejected: missing or ambiguous proposed identity")
            continue
        entity = {**entity, "name": originals[0]["name"]}
        entity["entity_id"] = entity_key(entity["name"], entity["type"], entity.get("identity_hint", ""))
        reviews = [item for item in raw_reviews if isinstance(item.get("name"), str)
                   and source_name_key(item["name"]) == source_name_key(entity["name"])
                   and item.get("type") == entity["type"]
                   and (item.get("identity_hint") or "") == entity.get("identity_hint", "")]
        raw = reviews[0] if len(reviews) == 1 else {}
        span = source_span(raw.get("type_evidence_quote"), source, offset)
        rationale = raw.get("type_rationale")
        name_pattern = r"\s+".join(re.escape(word) for word in entity["name"].split())
        context_words = re.findall(r"\w+", re.sub(name_pattern, "", span["quote"], flags=re.I)) if span else []
        if (span and named_mention(entity["name"], span["quote"]) and len(context_words) >= 2
                and isinstance(rationale, str) and rationale.strip()):
            entity["type_assessment"] = {"provenance": "source_review", "original_type": originals[0]["type"],
                                         "evidence": span, "rationale": rationale}
            accepted.append(entity)
        else:
            issues.append("Unsupported type correction ignored; proposed source type preserved")
            accepted.append(dict(originals[0]))
    return accepted


def coreference_candidates(entities, source, offset=0):
    """Deterministic proposals for review; literal OCR never grants identity."""
    from app.entity_policy import coreference_span, digest, identity_proof_id
    proposals = []
    for index, left in enumerate(entities):
        for right in entities[index+1:]:
            if (left["type"] != right["type"] or left["entity_id"] == right["entity_id"]
                    or left.get("identity_hint", "") != right.get("identity_hint", "")):
                continue
            span = coreference_span(left["name"], right["name"], source)
            if not span:
                continue
            proof = {"left_id": left["entity_id"], "right_id": right["entity_id"],
                     "left_name": left["name"], "right_name": right["name"], "type": left["type"],
                     "evidence": {**span, "start": span["start"] + offset, "end": span["end"] + offset},
                     "scope": {"start": offset, "end": offset + len(source), "source_hash": digest(source)}}
            proof["proof_id"] = identity_proof_id(proof)
            proposals.append(proof)
    return proposals


def adjudicate_coreferences(proposals, reviews, accepted, source, offset, issues):
    """Bind the independent verdict to exact proposed IDs, accepted types/spans."""
    accepted_ids = {entity["entity_id"]: entity for entity in accepted}
    reviews = reviews if isinstance(reviews, list) else []
    for proposal in proposals:
        matching = [row for row in reviews if isinstance(row, dict) and row.get("proof_id") == proposal["proof_id"]]
        if (len(matching) != 1 or proposal["left_id"] not in accepted_ids
                or proposal["right_id"] not in accepted_ids):
            continue
        review = matching[0]
        span = proposal["evidence"]
        if (review.get("status") not in {"affirmed", "denied", "unknown"}
                or review.get("assertion_scope") not in {"current_direct", "quoted", "hypothetical", "historical", "disputed", "unknown"}
                or type(review.get("explicit")) is not bool
                or review.get("evidence_quote") != span["quote"]
                or review.get("left_id") != proposal["left_id"] or review.get("right_id") != proposal["right_id"]
                or not isinstance(review.get("rationale"), str) or not review["rationale"].strip()
                or source[span["start"]-offset:span["end"]-offset] != span["quote"]):
            issues.append("Co-reference not affirmed by source-aware identity review")
            continue
        # Retain negative/unknown receipts too: an affirmative overlap window
        # must not silently win over a second window's disputed identity.
        proof = {**proposal, "status": review["status"], "assertion_scope": review["assertion_scope"], "explicit": review["explicit"],
                 "provenance": "source_identity_review", "rationale": review["rationale"]}
        for entity_id in (proposal["left_id"], proposal["right_id"]):
            accepted_ids[entity_id].setdefault("identity_proofs", []).append(proof)


def adjudicate_name_usage(accepted, reviews, source, offset):
    """Review abbreviation ambiguity independently of original capitalization."""
    for entity in accepted:
        matches = [row for row in reviews if isinstance(row.get("name"), str)
                   and source_name_key(row["name"]) == source_name_key(entity["name"]) and row.get("type") == entity["type"]
                   and (row.get("identity_hint") or "") == entity.get("identity_hint", "")]
        if len(matches) != 1:
            continue
        row = matches[0]
        span = source_span(row.get("name_usage_evidence_quote"), source, offset)
        if (row.get("name_usage") in {"abbreviation", "initials", "ambiguous", "brand", "full_name"}
                and span and named_mention(entity["name"], span["quote"])
                and len(span["quote"].split()) > len(entity["name"].split())
                and isinstance(row.get("name_usage_rationale"), str) and row["name_usage_rationale"].strip()):
            entity["name_usage"] = row["name_usage"]
            entity["name_usage_evidence"] = span


def validate_relationships(raw: list[dict], entities: list[dict], source: str, offset: int, issues: list[str]) -> list[dict]:
    accepted = []
    for rel in raw:
        start, end, kind = rel.get("from_entity"), rel.get("to_entity"), rel.get("relationship_type")
        span = source_span(rel.get("evidence_quote"), source, offset)
        rationale = rel.get("rationale") or rel.get("description")
        left = select_endpoint(entities, start, rel.get("from_entity_id"), rel.get("from_type"))
        right = select_endpoint(entities, end, rel.get("to_entity_id"), rel.get("to_type"))
        if not left or not right:
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
                "from_entity_id": left["entity_id"], "to_entity_id": right["entity_id"],
                "from_type": left["type"], "to_type": right["type"],
                "confidence": confidence(rel.get("confidence")), "rationale": rationale,
                "inferred": True, "evidence": [span],
            })
    return accepted


def entity_key(name: str, kind: str, hint: str = "") -> str:
    payload = json.dumps([source_name_key(name), kind, hint], ensure_ascii=False)
    return "entity-" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def select_endpoint(entities, name, entity_id=None, kind=None):
    if not isinstance(name, str):
        return None
    matches = [entity for entity in entities if source_name_key(entity["name"]) == source_name_key(name)
               and (not entity_id or entity.get("entity_id") == entity_id)
               and (not kind or entity["type"] == kind)]
    # Repeated extraction rows of one identity do not make the endpoint ambiguous.
    unique = {entity["entity_id"]: entity for entity in matches}
    return next(iter(unique.values())) if len(unique) == 1 else None


def relationship_key(rel):
    return (rel.get("from_entity_id"), rel.get("to_entity_id"), rel.get("relationship_type"))


def reconcile_entities(entities, issues):
    """Preserve qualified homonyms; contradictory readings of one span abstain."""
    entities = merge_unique(entities, ("entity_id",))
    rejected = set()
    for index, left in enumerate(entities):
        for right in entities[index+1:]:
            if left["name"].casefold() != right["name"].casefold():
                continue
            if left["type"] != right["type"] and any(
                    a["start"] < b["end"] and b["start"] < a["end"]
                    for a in left["evidence"] for b in right["evidence"]):
                rejected.update([left["entity_id"], right["entity_id"]])
            elif left["type"] == right["type"] and bool(left.get("identity_hint")) != bool(right.get("identity_hint")):
                # A bare repeated name cannot be assigned to one of its qualified
                # identities merely by aggregation order.
                rejected.add(left["entity_id"] if not left.get("identity_hint") else right["entity_id"])
    if rejected:
        issues.append(f"Ambiguous entity readings omitted: {len(rejected)}")
    return [entity for entity in entities if entity["entity_id"] not in rejected]


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
        for proof in item.get("identity_proofs") or []:
            if proof not in existing.setdefault("identity_proofs", []):
                existing["identity_proofs"].append(proof)
        if item.get("name_usage") in {"abbreviation", "initials", "ambiguous"}:
            existing["name_usage"] = item["name_usage"]
            existing["name_usage_evidence"] = item.get("name_usage_evidence")
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
