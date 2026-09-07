"""One acceptance boundary for factual answers, independent of HTTP delivery.

Semantic entailment remains a model assessment. This module independently checks
coverage, source membership and obvious value mismatches; it does not equate a
model's confidence with proof or infer completeness of the underlying archive.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from app.source_text import certifying_text

POLICY_VERSION = "source-audit-v5"
ABSTENTION = ("I could not verify a complete answer from the retrieved source text. "
              "Please review the source documents or narrow the question before relying on specific facts.")


def normalize_quote(text: str) -> str:
    return " ".join(unicodedata.normalize("NFC", text).split())


def canonical_prose(text: str) -> str:
    # Link labels are factual prose. Attribution removal needs evidence and is
    # performed separately, before splitting the audited candidate.
    return re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text.strip())


def canonical_candidate(text: str, pack: dict) -> tuple[str, list[dict]]:
    """Separate restricted citation declarations without trusting their claims.

    Offsets locate the preceding unit in the canonical candidate. Membership in
    this pack is only a precondition: _audit also checks that unit's references.
    Invalid declarations remain visible to the auditor and fail independently.
    """
    titles, document_ids = {}, set()
    for item in pack.get("items", []):
        if not isinstance(item, dict) or certifying_text(item) is None or item.get("feedback_open"):
            continue
        doc_id = item.get("document_id")
        if type(doc_id) is not int or doc_id < 1:
            continue
        document_ids.add(doc_id)
        title = item.get("title")
        if isinstance(title, str) and title.strip():
            titles.setdefault(normalize_quote(title), set()).add(doc_id)
    # Recognize citation-shaped text broadly enough that malformed declarations
    # cannot be silently erased. Ordinary Markdown links keep their labels.
    # Consume complete Markdown links before considering a bracketed title:
    # otherwise a verified title could hide an unverified attached link target.
    pattern = re.compile(r'\[([^\]\n]+)\]\([^\)\n]*\)|\[Source:[^\n]*?\]'
                         r'|\(Source:[^\n]*?\)|\(Paperless document[^)\n]*\)', re.I)
    parts, declarations, end, length = [], [], 0, 0
    text = text.strip()
    def unparsed_declarations(fragment, start):
        return [{"offset": start + match.start(), "document_id": None} for match in re.finditer(
            r'[\[(]\s*Source:|\(\s*Paperless\s+document\b|\[Document\s+', fragment, re.I)]
    def enclosed(position):
        # Attributions inside another bracket/parenthesis are outside the
        # restricted grammar, including nested Markdown link labels.
        stack = []
        for char in text[:position]:
            if char in "[(":
                stack.append(char)
            elif stack and (stack[-1], char) in {("[", "]"), ("(", ")")}:
                stack.pop()
        return bool(stack)
    for match in pattern.finditer(text):
        prefix = text[end:match.start()]
        declarations.extend(unparsed_declarations(prefix, length))
        parts.append(prefix)
        length += len(prefix)
        raw = match.group()
        doc_id = None
        title = re.fullmatch(r'(?:\[Source:\s*"([^"\[\]\n]*)"\s*\]|\(Source:\s*"([^"()\n]*)"\s*\))', raw, re.I)
        numeric = re.fullmatch(r'\(Paperless document\s+([1-9]\d*)\)', raw, re.I)
        link = re.fullmatch(r'\[Document\s+([1-9]\d*)\]\(/documents/([1-9]\d*)\)', raw, re.I)
        if title:
            ids = titles.get(normalize_quote(title[1] if title[1] is not None else title[2]), set())
            if len(ids) == 1:
                doc_id = next(iter(ids))
        elif numeric and int(numeric[1]) in document_ids:
            doc_id = int(numeric[1])
        elif link and link[1] == link[2] and int(link[1]) in document_ids:
            doc_id = int(link[1])
        if enclosed(match.start()):
            doc_id = None
        if match.group(1) is not None and not link:
            replacement = match.group(1)
            # A generic link must not launder unknown/malformed source syntax
            # into ordinary prose when its Markdown wrapper is removed.
            if unparsed_declarations(raw, 0):
                declarations.append({"offset": length, "document_id": None})
        else:
            declarations.append({"offset": length, "document_id": doc_id})
            replacement = "" if doc_id is not None else canonical_prose(raw)
        parts.append(replacement)
        length += len(replacement)
        end = match.end()
    declarations.extend(unparsed_declarations(text[end:], length))
    parts.append(text[end:])
    return "".join(parts).rstrip(), declarations


def parse_date(value: Any) -> tuple[str, str] | None:
    """Accept explicit calendar precision without inventing a month or day."""
    value = str(value or "").strip()
    try:
        if re.fullmatch(r"\d{4}", value):
            date(int(value), 1, 1)
            return value, "year"
        if re.fullmatch(r"\d{4}-\d{2}", value):
            date.fromisoformat(value + "-01")
            return value, "month"
        date.fromisoformat(value)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return value, "day"
    except ValueError:
        pass
    return None


def answer_units(answer: str) -> list[dict]:
    units = []
    # Retain offsets into the exact revision, including text after old audit caps.
    for match in re.finditer(r"\S(?:.*?)(?:(?<=[.!?])(?=\s)|(?=\n)|$)", answer, re.S):
        start, end = match.span()
        while start < end:
            stop = min(start + 1200, end)
            text = answer[start:stop]
            if text.strip():
                units.append({"id": f"u{len(units) + 1}", "start": start, "end": stop, "text": text})
            start = stop
    return units


def evidence_spans(pack: dict) -> list[dict]:
    spans = []
    seen = {}
    for item in pack.get("items", []):
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
            continue
        if type(item.get("document_id")) is not int or item["document_id"] < 1:
            continue
        content = certifying_text(item)
        if content is None:
            continue
        if not isinstance(content, str):
            raise ValueError("Evidence source content must be text")
        identity = (item["document_id"], item.get("chunk_index"), item.get("title"), content, bool(item.get("feedback_open")))
        if item["id"] in seen:
            if seen[item["id"]] != identity:
                raise ValueError("Evidence identity refers to conflicting source records")
            continue
        seen[item["id"]] = identity
        digest = hashlib.sha256(content.encode()).hexdigest()
        for start in range(0, len(content), 3800):
            text = content[start:start + 4000]
            spans.append({"span_id": f"{item['id']}:{digest[:16]}:{start}",
                          "evidence_id": item["id"], "document_id": item.get("document_id"),
                          "title": item.get("title", ""), "start": start, "end": start + len(text),
                          "content": text, "content_digest": digest,
                          "boundary_before": content[max(0, start - 2):start],
                          "boundary_after": content[start + len(text):start + len(text) + 2],
                          "feedback_open": bool(item.get("feedback_open"))})
    return spans


def select_spans(question: str, units: list[dict], spans: list[dict], budget: int = 28000) -> list[dict]:
    tokens = set(re.findall(r"[\w$%]+", question.lower() + " " + " ".join(u["text"].lower() for u in units)))
    # Retrieval metadata identifies explicitly requested documents even when
    # their short OCR has fewer shared words than a long unrelated notice.
    # This affects relevance only; reference validation still requires OCR.
    requested_ids = {int(value) for value in re.findall(
        r"\b(?:paperless\s+(?:document\s+)?(?:id\s*[:#]?\s*)?|document\s+(?:id\s*[:#]?\s*)?)"
        r"([1-9]\d{0,18})\b", question, re.I)}
    quoted_titles = {normalize_quote(value).casefold() for value in re.findall(r'["“]([^"”\n]+)["”]', question)}
    def rank(pair):
        index, span = pair
        title = normalize_quote(str(span.get("title") or "")).casefold()
        requested = span.get("document_id") in requested_ids or bool(title and title in quoted_titles)
        overlap = len(tokens & set(re.findall(r"[\w$%]+", span["content"].lower())))
        return (-requested, -overlap, index)
    ranked = sorted(enumerate(spans), key=rank)
    result, used = [], 0
    for _, span in ranked:
        if used + len(span["content"]) > budget:
            continue
        result.append(span)
        used += len(span["content"])
    return result


def validate_reference(reference: Any, spans: list[dict]) -> dict | None:
    if not isinstance(reference, dict):
        return None
    span = next((s for s in spans if s["span_id"] == reference.get("span_id")), None)
    if not span or span["feedback_open"]:
        return None
    if reference.get("evidence_id") != span["evidence_id"] or type(reference.get("document_id")) is not int:
        return None
    if reference["document_id"] != span["document_id"]:
        return None
    quote = reference.get("quote")
    if not isinstance(quote, str) or not normalize_quote(quote):
        return None
    normalized = normalize_quote(quote)
    source = span["content"]
    if normalized not in normalize_quote(source):
        return None
    # Map normalized matching back to an exact source range. Start with the
    # common literal case, then use whitespace-separated source token offsets.
    start = source.find(quote)
    end = start + len(quote)
    if start < 0:
        words = list(re.finditer(r"\S+", source))
        tokens = [unicodedata.normalize("NFC", w.group()) for w in words]
        wanted = normalized.split(" ")
        found = next((i for i in range(len(tokens) - len(wanted) + 1)
                      if tokens[i:i + len(wanted)] == wanted), None)
        if found is None:
            return None
        start, end = words[found].start(), words[found + len(wanted) - 1].end()
    before = (span.get("boundary_before", "") + source[:start])[-1:]
    after = (source[end:] + span.get("boundary_after", ""))[:2]
    if (before and source[start].isalnum() and (before.isalnum() or before == "_")) or (
        after and source[end - 1].isalnum() and (after[0].isalnum() or after[0] == "_")
    ):
        return None
    if source[start].isdigit() and before in {"+", "-", "−", ".", ","}:
        return None
    if source[end - 1].isdigit() and len(after) > 1 and after[0] in ".," and after[1].isdigit():
        return None
    if source[end - 1] in ".," and end - start > 1 and source[end - 2].isdigit() and after[:1].isdigit():
        return None
    return {"span_id": span["span_id"], "evidence_id": span["evidence_id"],
            "document_id": span["document_id"], "source_title": span["title"],
            "quote": source[start:end], "start": span["start"] + start,
            "end": span["start"] + end, "content_digest": span["content_digest"]}


def values_match(text: str, references: list[dict]) -> bool:
    # Supplement (never replace) semantic audit. Exact source values are needed
    # for generated precise numbers and named units. Computations need their own
    # explicit calculation evidence; the auditor cannot simply bless a new value.
    # Compare rendered prose without changing the audited revision or offsets.
    text = canonical_prose(text)
    text = re.sub(r"^\s*\d+\.(?:\s|$)", "", text, flags=re.MULTILINE)
    # Peel nested delimiters; every successful pass strictly shortens the copy.
    while True:
        previous_length = len(text)
        for markup in (r"(?<!`)(`+)(?!`)(.+?)(?<!`)\1(?!`)",
                       r"(?<!\*)(\*+)(?!\*)(.+?)(?<!\*)\1(?!\*)",
                       r"(?<!\w)(_+)(?!_)(.+?)(?<!_)\1(?!\w)",
                       r"(\[)([^\[\]]+)\]"):
            text = re.sub(markup, r"\2", text, flags=re.DOTALL)
        if len(text) == previous_length:
            break
    text = text.replace("−", "-")
    # A quote boundary is not source adjacency, even within the same document.
    sources = [r["quote"].replace("−", "-") for r in references]
    # A valid date prefix must not disguise an impossible or more precise date.
    for value in re.findall(r"(?<!\w)\d{4}-\d{1,2}(?:-\d{1,2})?(?!\d)", text):
        if not parse_date(value) or not any(date_occurs(value, source) for source in sources):
            return False
    number = r"[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?"
    def numbers(value):
        return {Decimal(n.replace(",", "")) for n in re.findall(r"(?<![\w.,])(" + number + r")(?!\w|[.,]\d)", value)}
    if not numbers(text) <= {value for source in sources for value in numbers(source)}:
        return False
    units = r"(?:mmol/L|mg/dL|g/dL|USD|EUR|GBP|CAD|AUD|JPY|mL|ml|mcg|µg|μg|mg|kg|ng|kWh|ppm|lbs|lb|oz|km|cm|mm|ft|mi|°C|°F|percent|L|g|m|s|h|[$€£%])"
    unit_pattern = r"(?<![A-Za-z'’])" + units + r"(?![A-Za-z])"
    if not set(re.findall(unit_pattern, text)) <= {unit for source in sources for unit in re.findall(unit_pattern, source)}:
        return False
    def quantities(value):
        pairs = {(Decimal(amount.replace(",", "")), unit) for amount, unit in re.findall(
            r"(?<![\w.,])(" + number + r")\s*(" + units + r")(?![A-Za-z])", value)}
        for unit, amount in re.findall(r"(USD|EUR|GBP|CAD|AUD|JPY|[$€£])\s*(" + number + r")(?!\w|[.,]\d)", value):
            pairs.add((Decimal(amount.replace(",", "")), unit))
        return pairs
    return quantities(text) <= {pair for source in sources for pair in quantities(source)}


def date_occurs(value: str, source: str) -> bool:
    return bool(re.search(r"(?<![\w-])" + re.escape(value) + r"(?![\w-])", source))


def empty_ledger(candidate: str) -> dict:
    total = len(answer_units(candidate))
    return {"claims": [], "summary": {"total": total, "supported": 0, "audit_coverage": 0},
            "complete": False, "spans": [], "available_span_count": 0,
            "candidate_digest": hashlib.sha256(candidate.encode()).hexdigest()}


class AnswerFinalizer:
    def __init__(self, auditor, repairer=None, *, timeout_seconds: float = 60, max_units: int = 80,
                 concurrency: int = 4):
        self.auditor = auditor
        self.repairer = repairer
        self.timeout_seconds = timeout_seconds
        self.max_units = max_units
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or type(concurrency) is not int or concurrency < 1:
            raise ValueError("Audit timeout and concurrency must be positive")
        self.concurrency = concurrency

    def _audit_timeout(self, candidate):
        batches = math.ceil(min(len(answer_units(candidate)), self.max_units) / 4)
        return self.timeout_seconds * max(1, math.ceil(batches / self.concurrency))

    async def _audit(self, question, answer, pack, plan, declarations=()):
        units = answer_units(answer)
        spans = evidence_spans(pack)
        manifest, claims = {}, []
        checked = 0
        complete = bool(units) and len(units) <= self.max_units and bool(spans)
        if complete:
            batches = [units[offset:offset + 4] for offset in range(0, len(units), 4)]
            pending = iter(enumerate(batches))
            results = [None] * len(batches)
            async def worker():
                for index, batch in pending:
                    selected = select_spans(question, batch, spans)
                    raw = await self.auditor.audit_answer_units(question, batch, selected, plan)
                    results[index] = (selected, raw)
            async with asyncio.TaskGroup() as group:
                for _ in range(min(self.concurrency, len(batches))):
                    group.create_task(worker())
            for batch, (selected, raw) in zip(batches, results):
                manifest.update({s["span_id"]: s for s in selected})
                assessments = raw.get("assessments", []) if isinstance(raw, dict) else []
                if not isinstance(assessments, list):
                    assessments = []
                expected_ids = {u["id"] for u in batch}
                if any(not isinstance(a, dict) or a.get("unit_id") not in expected_ids for a in assessments):
                    assessments = []
                for unit in batch:
                    matches = [a for a in assessments if isinstance(a, dict) and a.get("unit_id") == unit["id"]]
                    assessment = matches[0] if len(matches) == 1 else {}
                    raw_refs = assessment.get("references", [])
                    raw_refs = raw_refs if isinstance(raw_refs, list) else []
                    refs = [validate_reference(r, selected) for r in raw_refs]
                    valid = bool(refs) and all(refs)
                    status = assessment.get("status", "unchecked")
                    if status not in {"supported", "unsupported", "conflicting", "missing"}:
                        status = "unchecked"
                    if status != "unchecked":
                        checked += 1
                    if status == "supported" and (not valid or not values_match(unit["text"], refs)):
                        status = "unsupported"
                    claims.append({"id": unit["id"], "claim": unit["text"], "start": unit["start"],
                                   "end": unit["end"], "status": status, "references": [r for r in refs if r],
                                   "document_id": refs[0]["document_id"] if valid else None,
                                   "evidence_ids": [r["evidence_id"] for r in refs if r],
                                   "evidence_quote": refs[0]["quote"] if valid else "",
                                   "source_title": refs[0]["source_title"] if valid else ""})
                    scope = assessment.get("temporal_scope", "unknown")
                    claims[-1]["temporal_scope"] = scope if isinstance(scope, str) and scope in {"historical", "current", "none", "unknown"} else "unknown"
        for declaration in declarations:
            preceding = [claim for claim in claims if claim["start"] < declaration["offset"]]
            if not preceding:
                complete = False
                continue
            claim = preceding[-1]
            if declaration["document_id"] not in {r["document_id"] for r in claim["references"]}:
                claim["status"] = "unsupported"
        # Formatting and quantities can cross audit-unit boundaries. Recheck the
        # complete revision before certifying it; references remain separate quotes.
        if claims and all(claim["status"] == "supported" for claim in claims):
            references = [reference for claim in claims for reference in claim["references"]]
            if not values_match(answer, references):
                for claim in claims:
                    claim["status"] = "unsupported"
        complete = complete and checked == len(units)
        summary = {status: sum(c["status"] == status for c in claims)
                   for status in ("supported", "unsupported", "conflicting", "missing", "unchecked")}
        summary.update(total=len(units), audited=checked, audit_coverage=checked / len(units) if units else 0,
                       support_ratio=summary["supported"] / len(units) if units else 0)
        return {"claims": claims, "summary": summary, "complete": complete,
                "spans": list(manifest.values()), "available_span_count": len(spans),
                "candidate_digest": hashlib.sha256(answer.encode()).hexdigest()}

    async def finalize(self, question: str, answer: str, evidence_pack: dict, *, plan: dict | None = None,
                       mode: str = "strict", evaluated_at: str | None = None) -> dict:
        plan = dict(plan or {})
        evaluated_at = evaluated_at or datetime.now(timezone.utc).date().isoformat()
        if not parse_date(evaluated_at) or parse_date(evaluated_at)[1] != "day":
            raise ValueError("evaluated_at must be a valid ISO calendar day")
        plan["evaluated_at"] = evaluated_at
        disposition, attempts, error = "incomplete", 0, None
        candidate, declarations = canonical_candidate(str(answer or ""), evidence_pack)
        ledger = empty_ledger(candidate)
        if mode == "quick":
            disposition = "unaudited"
        else:
            try:
                for attempt in range(2 if self.repairer else 1):
                    attempts += 1
                    # A failed second audit must not attach the prior
                    # candidate's ledger to the replacement's digest.
                    ledger = empty_ledger(candidate)
                    async with asyncio.timeout(self._audit_timeout(candidate)):
                        ledger = await self._audit(question, candidate, evidence_pack, plan, declarations)
                    summary = ledger["summary"]
                    if ledger["complete"] and summary["supported"] == summary["total"]:
                        disposition = "supported"
                        break
                    disposition = "unsupported" if ledger["complete"] else "incomplete"
                    if attempt == 0 and self.repairer and len(candidate) <= 96000:
                        async with asyncio.timeout(self.timeout_seconds):
                            repaired = await self.repairer.repair_answer(
                                question, candidate, json.dumps(ledger["spans"], ensure_ascii=False),
                                {"status": disposition, "claims": ledger["claims"]})
                        replacement = repaired.get("answer") if isinstance(repaired, dict) else None
                        if not isinstance(replacement, str) or not replacement.strip() or replacement.strip() == candidate:
                            break
                        candidate, declarations = canonical_candidate(replacement, evidence_pack)
                    else:
                        break
            except TimeoutError:
                disposition, error = "timeout", "The source audit exceeded its time budget."
            except Exception:
                disposition, error = "audit_failed", "The source audit was unavailable or returned invalid data."
        # Current status is a separate claim. A date on a document is insufficient;
        # require explicit intervals and a semantic no-conflict assessment in plan.
        current_words = r"\b(?:currently|current|active|today|now|still|latest)\b"
        asserts_current = re.search(current_words, candidate, re.I)
        # A fallible planner cannot switch off temporal acceptance for an
        # explicitly current question or a claim the auditor labels current.
        plan["requires_current"] = bool(plan.get("requires_current")) or bool(
            asserts_current or re.search(current_words, question, re.I)
            or any(claim["temporal_scope"] == "current" for claim in ledger["claims"]))
        current = current_state(plan, evidence_pack, evaluated_at)
        if disposition == "supported" and plan.get("requires_current") and current["status"] != "resolved":
            historical_only = all(c["temporal_scope"] == "historical" for c in ledger["claims"])
            disposition = "qualified" if historical_only and not asserts_current else "current_unresolved"
        supported = disposition in {"supported", "qualified"}
        public_answer = candidate if supported else ABSTENTION
        if disposition == "unaudited" and evidence_pack.get("items"):
            public_answer = "Unaudited quick answer — verify the source documents before relying on it.\n\n" + candidate
        refs = [r for claim in ledger["claims"] for r in claim["references"]] if supported else []
        doc_ids = list(dict.fromkeys(r["document_id"] for r in refs))
        if supported:
            for claim in reversed(ledger["claims"]):
                ids = list(dict.fromkeys(r["document_id"] for r in claim["references"]))
                citations = " " + " ".join(f"[Document {i}](/documents/{i})" for i in ids)
                public_answer = public_answer[:claim["end"]] + citations + public_answer[claim["end"]:]
        if disposition == "qualified":
            public_answer += f"\n\nThese are documented facts. Current status as of {evaluated_at} is not established by the retrieved evidence."
        verification = {"status": "verified" if supported else disposition,
                        "supported_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "supported"],
                        "unsupported_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "unsupported"],
                        "stale_or_conflicting_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "conflicting"],
                        "missing_evidence": [] if supported else [error or "Not every material claim has validated source support."],
                        "notes": ["Semantic support is model-assessed; source membership and audit coverage are checked independently."]}
        revision = hashlib.sha256(candidate.encode()).hexdigest()
        # Public manifest records exactly what was sent without duplicating full OCR.
        ledger["spans"] = [{k: v for k, v in span.items() if k not in {"content", "boundary_before", "boundary_after"}} for span in ledger["spans"]]
        return {"answer": public_answer, "verification": verification, "claim_ledger": ledger,
                "current_state": current,
                "finalization": {"policy_version": POLICY_VERSION, "disposition": disposition,
                                 "answer_digest": hashlib.sha256(public_answer.encode()).hexdigest(),
                                 "candidate_digest": revision, "evaluated_at": evaluated_at,
                                 "attempts": attempts, "complete": supported, "cited_document_ids": doc_ids},
                "evidence": {"score": 0.65 if disposition == "qualified" else 0.8 if supported else 0.25 if disposition == "unaudited" else 0.0,
                             "level": "medium" if disposition == "qualified" else "high" if supported else "low", "audit_status": disposition,
                             "source_count": len(doc_ids), "claim_summary": ledger["summary"],
                             "coverage": {"answer_complete": ledger["complete"],
                                          "selected_span_count": len(ledger["spans"]),
                                          "available_span_count": ledger["available_span_count"]},
                             "reasons": ["All answer units have validated source references."] if supported else [],
                             "penalties": [] if supported else verification["missing_evidence"],
                             "dimensions": {"claim_support": ledger["summary"].get("support_ratio", 0),
                                            "audit_coverage": ledger["summary"].get("audit_coverage", 0)}}}


def current_state(plan: dict, pack: dict, evaluated_at: str) -> dict:
    required = bool(plan.get("requires_current"))
    intervals = []
    for item in pack.get("items", []):
        if not isinstance(item, dict) or item.get("feedback_open"):
            continue
        content = str(item.get("content") or item.get("excerpt") or "")
        start = re.search(r"\beffective(?: date)?\s*:?\s*(\d{4}-\d{2}-\d{2})\b", content, re.I)
        end = re.search(r"\b(?:expires|expiration(?: date)?|through)\s*:?\s*(\d{4}-\d{2}-\d{2})\b", content, re.I)
        if start and end and parse_date(start[1]) and parse_date(end[1]) and start[1] <= evaluated_at <= end[1]:
            intervals.append(item["id"])
    # An interval proves the documented term, not that cancellation/newer records
    # do not exist. Keep that distinction public rather than guessing completeness.
    return {"required": required, "evaluated_at": evaluated_at,
            "status": "needs_review" if required else "not_required",
            "active_documented_interval_ids": intervals,
            "note": "Retrieved dates and terms alone do not establish current real-world status."}
