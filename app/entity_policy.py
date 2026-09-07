"""Identity evidence rules. Similarity and co-occurrence are never identity proof.

This module has no clients or I/O; it is shared by ingestion, review and tests.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata

RESOLUTION_POLICY = "evidence-identity-v2"
EXPLICIT_REVIEW_METHOD = "entity_review_api"
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
    if kind == "Person":
        value = re.sub(r"^(?:dr|mr|mrs|ms|prof)\.?\s+", "", value)
    # Periods on initialisms or legal suffixes are orthographic. Do not erase
    # arbitrary dots (domains), hyphens, slash/composite names or apostrophes.
    value = re.sub(r"\b(?:[a-z]\.){2,}", lambda m: m[0].replace(".", ""), value)
    value = re.sub(r"\b([a-z])\.(?=\s|$)", r"\1", value)
    if kind == "Organization":
        value = re.sub(r"^(?:[a-z]\s+){2,}(?=\w{2,})", lambda m: m[0].replace(" ", "") + " ", value)
        legal_forms = {"incorporated": "inc", "corporation": "corp", "limited": "ltd",
                       "limited liability company": "llc", "limited liability partnership": "llp"}
        for full, short in legal_forms.items():
            value = re.sub(r",?\s+" + full + r"\.?$", " " + short, value)
        value = re.sub(r",?\s+(inc|llc|ltd|corp|llp|plc)\.?$", r" \1", value)
    return " ".join(value.split())


def context_bound_name(name: str, kind: str, name_usage: str = "") -> bool:
    """Scope by reviewed usage/type, never by invoice typography."""
    words = name_key(name, kind).split()
    letters = "".join(c for c in name if c.isalpha())
    return (name_usage in {"abbreviation", "initials", "ambiguous"}
            or len(letters) <= 2
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
    """Find a candidate alias phrase; NEVER an affirmative identity verdict.

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
        patterns.append(a + r'\s*,?\s+(?:also known as|doing business as|d/b/a|aka|hereinafter referred to as)\s+[\"“]?'+ b + r'[\"”]?')
        words = re.findall(r"[^\W\d_]+", full, re.UNICODE)
        initials = "".join(w[0] for w in words if w.casefold() not in {"of", "the", "and", "inc", "incorporated", "llc", "ltd", "corp", "corporation"}).casefold()
        all_initials = "".join(w[0] for w in words).casefold()
        compact = re.sub(r"[.\s]", "", alias).casefold()
        if len(words) >= 2 and 2 <= len(compact) <= 10 and compact in {initials, all_initials}:
            patterns.append(a + r'\s*\(\s*[\"“]?' + b + r'[\"”]?\s*\)')
            patterns.append(b + r'\s*\(\s*' + a + r'\s*\)')
    for pattern in patterns:
        match = re.search(pattern, source, re.I)
        if match:
            # Negated statements cannot authorize an alias.
            prefix = source[max(0, match.start()-120):match.start()]
            preceding = re.search(r"([\w'-]+)\s+$", prefix)
            if preceding and preceding[1].casefold() not in {"is", "named", "called", "by", "from", "with", "to", "for", "and", "or", "as", "at"}:
                # A matching suffix of a longer proper name is not the named
                # subject of the source's alias assertion (e.g. country/parent).
                continue
            if re.search(r"\b(?:not|never|incorrectly|mistakenly|false|incorrect|rejected|denied|alleged|hypothetical)\b[^.!?\n]*$", prefix, re.I):
                continue
            suffix = source[match.end():].split("\n", 1)[0]
            suffix = re.split(r"[.!?]", suffix, maxsplit=1)[0]
            if re.search(r"\b(?:false|incorrect|not|never|denied|rejected)\b", suffix, re.I):
                continue
            if source[match.end():match.end()+1] in {"'", "’"}:
                continue
            return {"start": match.start(), "end": match.end(), "quote": match[0]}
    return None


def initialism_expansions(alias: str, source: str) -> list[str]:
    """Find mechanically proved local expansions, including ones not in the graph.

    These constrain a context-specific abbreviation; they do not create typed
    entities or invent a meaning from a model's general knowledge.
    """
    compact = re.sub(r"[.\s]", "", alias)
    if not compact.isalpha() or not 2 <= len(compact) <= 10:
        return []
    escaped = r"\s+".join(re.escape(word) for word in alias.split())
    matches = re.finditer(r'([^()\n!?;]{1,240})\(\s*["“]?' + escaped + r'["”]?\s*\)', source, re.I)
    expansions = {}
    for match in matches:
        words = match[1].strip().split()
        for index in range(len(words)):
            full = " ".join(words[index:]).strip(' ,.:"“”')
            if coreference_span(full, alias, source):
                expansions[name_key(full, "Organization")] = full
                break  # preserve the longest proved name; never drop prefixes
    # Reverse glossary notation: ABBR (Expanded Name).
    for match in re.finditer(r'(?<!\w)' + escaped + r'\s*\(([^()\n]{1,240})\)', source, re.I):
        full = match[1].strip()
        if coreference_span(full, alias, source):
            expansions[name_key(full, "Organization")] = full
    return list(expansions.values())


def has_local_alias_definition(alias: str, source: str) -> bool:
    escaped = r"\s+".join(re.escape(word) for word in alias.split())
    parenthetical = re.search(r'\(\s*["“]?' + escaped + r'["”]?\s*\)', source, re.I)
    compact = re.sub(r"[^\w]", "", alias)
    abbreviation_like = compact.isalpha() and len(compact) <= 5
    return bool((parenthetical and abbreviation_like) or re.search(r'(?:also known as|doing business as|d/b/a|aka|hereinafter referred to as)\s+["“]?'
                          + escaped + r'(?!\w)', source, re.I))


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


def alias_is_quarantined(node: dict, alias: str, kind: str) -> bool:
    return any(record.get("status") in {"quarantined", "revoked", "untrusted"}
               and record.get("type") == kind
               and name_key(record.get("alias", ""), kind) == name_key(alias, kind)
               for record in alias_records(node))


def trusted_aliases(node: dict, kind: str, doc_id: int, source: str) -> list[str]:
    result = []
    for record in alias_records(node):
        if record.get("status") in {"quarantined", "revoked", "untrusted"}:
            continue
        alias = record.get("alias")
        if not isinstance(alias, str) or record.get("type") != kind or alias_is_quarantined(node, alias, kind):
            continue
        if name_key(record.get("canonical_name", ""), kind) != name_key(node.get("name", ""), kind):
            continue
        if (record.get("provenance") == "human_review" and record.get("review_id")
                and record.get("review_method") == EXPLICIT_REVIEW_METHOD):
            result.append(alias)
        elif (source and record.get("provenance") == "source_coreference"
              and record.get("policy") == RESOLUTION_POLICY and record.get("source_doc_id") == doc_id
              and record.get("source_hash") == digest(source)):
            proof = record.get("identity_proof")
            span = verified_coreference(node["name"], alias, kind, source, [proof])
            if (span and digest(span["quote"]) == record.get("quote_hash")
                    and span["start"] == record.get("start") and span["end"] == record.get("end")):
                result.append(alias)
    return result


def source_alias_record(canonical: str, alias: str, kind: str, doc_id: int, source: str, span: dict) -> dict:
    # A literal regex span cannot mint positive authority. Keep the public shape
    # compatible, but require the independently verified structured receipt.
    proof = span.get("identity_proof")
    if not verified_coreference(canonical, alias, kind, source, [proof]):
        raise ValueError("Source alias requires an accepted source-review identity proof")
    return {"alias": alias, "canonical_name": canonical, "type": kind,
            "provenance": "source_coreference", "policy": RESOLUTION_POLICY,
            "source_doc_id": doc_id, "source_hash": digest(source),
            "start": span["start"], "end": span["end"], "quote_hash": digest(span["quote"]),
            "identity_proof": proof}


def identity_proof_id(proof: dict) -> str:
    fields = ("left_id", "right_id", "left_name", "right_name", "type", "evidence", "scope")
    return "coref-" + digest(json.dumps({key: proof.get(key) for key in fields}, sort_keys=True))


def valid_identity_receipt(proof: dict, source: str) -> bool:
    """Check receipt integrity independently of whether the verdict is positive."""
    if not isinstance(proof, dict) or any(not isinstance(proof.get(field), str) or not proof[field].strip()
            for field in ("left_name", "right_name", "left_id", "right_id", "rationale")):
        return False
    if (proof.get("type") not in ENTITY_TYPES or proof["left_id"] == proof["right_id"]
            or proof.get("status") not in {"affirmed", "denied", "unknown"}
            or proof.get("assertion_scope") not in {"current_direct", "quoted", "hypothetical", "historical", "disputed", "unknown"}
            or type(proof.get("explicit")) is not bool
            or proof.get("provenance") != "source_identity_review" or proof.get("proof_id") != identity_proof_id(proof)):
        return False
    evidence, scope = proof.get("evidence"), proof.get("scope")
    if not isinstance(evidence, dict) or not isinstance(scope, dict):
        return False
    if (not verified_spans(proof["left_name"], [evidence], source)
            or not verified_spans(proof["right_name"], [evidence], source)):
        return False
    start, end = scope.get("start"), scope.get("end")
    return (type(start) is int and type(end) is int and 0 <= start < end <= len(source)
            and digest(source[start:end]) == scope.get("source_hash")
            and start <= evidence["start"] < evidence["end"] <= end)


def verified_coreference(left: str, right: str, kind: str, source: str, proofs: list) -> dict | None:
    """Validate a source-aware review receipt, not semantic truth from a regex.

    The reviewer must have accepted both typed identities AND explicitly affirmed
    their present, direct equivalence against the full recorded source window.
    Missing, contradictory, stale, or out-of-scope receipts grant no authority.
    """
    pair = {name_key(left, kind), name_key(right, kind)}
    matches = []
    for proof in proofs or []:
        if not isinstance(proof, dict):
            continue
        if any(not isinstance(proof.get(field), str) or not proof[field].strip()
               for field in ("left_name", "right_name", "left_id", "right_id", "rationale")):
            continue
        proof_kind = "DocumentRef" if proof.get("type") == "Document" else proof.get("type")
        if proof_kind != ("DocumentRef" if kind == "Document" else kind):
            continue
        if {name_key(proof.get("left_name", ""), kind), name_key(proof.get("right_name", ""), kind)} != pair:
            continue
        if (not valid_identity_receipt(proof, source) or proof.get("status") != "affirmed"
                or proof.get("assertion_scope") != "current_direct" or proof.get("explicit") is not True):
            return None
        matches.append({**proof["evidence"], "identity_proof": proof})
    return matches[0] if matches else None


def human_alias_record(canonical: str, alias: str, kind: str, review_id: str, *, review_method: str) -> dict:
    if not review_id or review_method != EXPLICIT_REVIEW_METHOD:
        raise ValueError("Human alias provenance requires an explicit review origin")
    return {"alias": alias, "canonical_name": canonical, "type": kind,
            "provenance": "human_review", "policy": RESOLUTION_POLICY, "review_id": review_id,
            "review_method": review_method}
