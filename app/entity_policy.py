"""Identity evidence rules. Similarity and co-occurrence are never identity proof.

This module has no clients or I/O; it is shared by ingestion, review and tests.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata

RESOLUTION_POLICY = "evidence-identity-v1"
ENTITY_TYPES = frozenset({
    "Person", "Organization", "Location", "System", "Product", "Document",
    "DocumentRef", "Event", "Condition", "FinancialItem", "InsurancePolicy",
    "Contract", "DateEvent", "Address",
})


def display_name(name: str) -> str:
    """Preserve distinguishing tokens, punctuation and diacritics in storage."""
    return " ".join(unicodedata.normalize("NFC", name).split())


def name_key(name: str, kind: str = "") -> str:
    """Narrow orthography, not fuzzy normalization or legal-identity inference.

    Keep initials, generations, legal suffixes, bank/insurance/holdings etc.
    Reorder a comma only for a Person; 'Example, Inc.' is not 'Inc Example'.
    """
    value = display_name(name).casefold().replace("’", "'")
    if kind == "Person" and value.count(",") == 1:
        last, first = value.split(",")
        # Suffixes are identifying tokens, not inverted first names.
        if first.strip().rstrip(".") not in {"jr", "sr", "ii", "iii", "iv"}:
            value = first.strip() + " " + last.strip()
    # Periods on initialisms or legal suffixes are orthographic. Do not erase
    # arbitrary dots (domains), hyphens, slash/composite names or apostrophes.
    value = re.sub(r"\b(?:[a-z]\.){2,}", lambda m: m[0].replace(".", ""), value)
    value = re.sub(r"\b([a-z])\.(?=\s|$)", r"\1", value)
    if kind == "Organization":
        value = re.sub(r",?\s+(inc|llc|ltd|corp|llp|plc)\.?$", r" \1", value)
    return " ".join(value.split())


def context_bound_name(name: str, kind: str) -> bool:
    """Short/initial-only references are not globally unique names."""
    words = name_key(name, kind).split()
    letters = "".join(c for c in name if c.isalpha())
    return (len(letters) <= 2 or (len(words) == 1 and letters.isupper() and len(letters) <= 10)
            or (kind == "Person" and (len(words) < 2 or any(len(w) == 1 for w in words))))


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def verified_spans(name: str, evidence: list, source: str) -> list[dict]:
    """Only exact offsets/quotes in this revision can grant source authority."""
    from app.extraction_evidence import named_mention
    spans = []
    for span in evidence or []:
        if not isinstance(span, dict):
            continue
        start, end, quote = span.get("start"), span.get("end"), span.get("quote")
        if (type(start) is int and type(end) is int and 0 <= start < end <= len(source)
                and isinstance(quote, str) and source[start:end] == quote
                and named_mention(name, quote)):
            spans.append(span)
    return spans


def coreference_span(left: str, right: str, source: str) -> dict | None:
    """Recognize explicit aliases, not issuer/brand/parent/subsidiary proximity.

    Deliberately small grammar. Unsupported co-reference goes to human review.
    Parentheses alone prove only a mechanically matching initialism.
    """
    if not source or left == right:
        return None
    def literal(value):
        return r"(?<!\w)" + r"\s+".join(re.escape(w) for w in value.split()) + r"(?!\w)"
    patterns = []
    for full, alias in ((left, right), (right, left)):
        a, b = literal(full), literal(alias)
        patterns.append(a + r'\s*,?\s+(?:also known as|doing business as|d/b/a|aka)\s+[\"“]?'+ b + r'[\"”]?')
        words = re.findall(r"[^\W\d_]+", full, re.UNICODE)
        initials = "".join(w[0] for w in words if w.casefold() not in {"of", "the", "and"}).casefold()
        all_initials = "".join(w[0] for w in words).casefold()
        compact = re.sub(r"[.\s]", "", alias).casefold()
        if len(words) >= 2 and 2 <= len(compact) <= 10 and compact in {initials, all_initials}:
            patterns.append(a + r'\s*\(\s*[\"“]?' + b + r'[\"”]?\s*\)')
            patterns.append(b + r'\s*\(\s*' + a + r'\s*\)')
    for pattern in patterns:
        match = re.search(pattern, source, re.I)
        if match:
            # Negated statements cannot authorize an alias.
            prefix = source[max(0, match.start()-30):match.start()]
            if re.search(r"\b(?:not|never|incorrectly|mistakenly)\b[^.!?\n]*$", prefix, re.I):
                continue
            return {"start": match.start(), "end": match.end(), "quote": match[0]}
    return None


def alias_records(node: dict) -> list[dict]:
    records = []
    for value in node.get("alias_records") or []:
        try:
            value = json.loads(value) if isinstance(value, str) else value
        except (ValueError, TypeError):
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def trusted_aliases(node: dict, kind: str, doc_id: int, source: str) -> list[str]:
    result = []
    for record in alias_records(node):
        if record.get("status") in {"quarantined", "revoked", "untrusted"}:
            continue
        alias = record.get("alias")
        if not isinstance(alias, str) or record.get("type") != kind:
            continue
        if name_key(record.get("canonical_name", ""), kind) != name_key(node.get("name", ""), kind):
            continue
        if record.get("provenance") == "human_review" and record.get("review_id"):
            result.append(alias)
        elif (source and record.get("provenance") == "source_coreference"
              and record.get("policy") == RESOLUTION_POLICY and record.get("source_doc_id") == doc_id
              and record.get("source_hash") == digest(source)):
            span = coreference_span(node["name"], alias, source)
            if span and digest(span["quote"]) == record.get("quote_hash"):
                result.append(alias)
    return result


def source_alias_record(canonical: str, alias: str, kind: str, doc_id: int, source: str, span: dict) -> dict:
    return {"alias": alias, "canonical_name": canonical, "type": kind,
            "provenance": "source_coreference", "policy": RESOLUTION_POLICY,
            "source_doc_id": doc_id, "source_hash": digest(source),
            "start": span["start"], "end": span["end"], "quote_hash": digest(span["quote"])}


def human_alias_record(canonical: str, alias: str, kind: str, review_id: str) -> dict:
    return {"alias": alias, "canonical_name": canonical, "type": kind,
            "provenance": "human_review", "policy": RESOLUTION_POLICY, "review_id": review_id}
