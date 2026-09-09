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
from collections import Counter
from itertools import zip_longest
import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from markdown_it import MarkdownIt

from app.answer_structure import audit_context, is_colon_label, strong_label_offsets, supported_revision
from app.source_audit import PROTOCOL_ERRORS
from app.answer_observations import ObservationCandidate, ObservationValidationError
from app.timeline import project_timeline
from app.answer_delivery import render_verified_answer
from app.source_text import certifying_text, certified_document_context
from app import source_quantities
from app.source_dates import source_dates, date_supported, source_date_occurs, without_dates, date_context, VALUE_UNITS

POLICY_VERSION = "source-audit-v25"

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
    paired_pattern = (r'\(\*(?P<paired_title>[^*\[\]()\n]+)\*,[ \t]*Paperless ID[ \t]+'
                      r'(?P<paired_id>[1-9]\d*)\)')
    pattern = re.compile(r'\[([^\]\n]+)\]\([^\)\n]*\)|\[Source:[^\n]*?\]'
                         r'|\(Source:[^\n]*?\)|\(Paperless document[^)\n]*\)'
                         r'|' + paired_pattern, re.I)
    parts, declarations, end, length = [], [], 0, 0
    text = text.strip()
    def unparsed_declarations(fragment, start):
        return [{"offset": start + match.start(), "document_id": None} for match in re.finditer(
            r'[\[(]\s*Source:|\(\s*Paperless\s+document\b|\[Document\s+'
            r'|\bPaperless\s+ID\b', fragment, re.I)]
    stack, scanned = [], 0
    def enclosed(position):
        # Attributions inside another bracket/parenthesis are outside the
        # restricted grammar, including nested Markdown link labels. Match
        # positions advance, so scan each prefix character only once.
        nonlocal scanned
        for char in text[scanned:position]:
            if char in "[(":
                stack.append(char)
            elif stack and (stack[-1], char) in {("[", "]"), ("(", ")")}:
                stack.pop()
        scanned = position
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
        paired = re.fullmatch(paired_pattern, raw, re.I)
        if title:
            ids = titles.get(normalize_quote(title[1] if title[1] is not None else title[2]), set())
            if len(ids) == 1:
                doc_id = next(iter(ids))
        elif numeric and int(numeric[1]) in document_ids:
            doc_id = int(numeric[1])
        elif link and link[1] == link[2] and int(link[1]) in document_ids:
            doc_id = int(link[1])
        elif paired and titles.get(normalize_quote(paired["paired_title"]), set()) == {int(paired["paired_id"])}:
            doc_id = int(paired["paired_id"])
        if enclosed(match.start()) or (paired and re.match(r'\s*[\])]', text[match.end():])):
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


_SENTENCE_OPENERS = {'The', 'A', 'An', 'This', 'That', 'These', 'Those', 'It', 'Its', 'They', 'Their',
                     'We', 'Our', 'You', 'Your', 'He', 'She', 'Another', 'However', 'Also'}


def _abbreviation_continues(prefix: str, suffix: str) -> bool:
    abbreviation = re.search(r"\b(No|Mr|Mrs|Ms|Dr|Prof|Sr|Jr|St|Inc|Corp|Co|Ltd|approx|etc|vs|Fig|Eq|Vol|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.$", prefix, re.I)
    if not abbreviation:
        return bool(re.search(r"(?:\b[A-Za-z]\.){2,}$", prefix))
    if abbreviation[1].lower() in {'no', 'mr', 'mrs', 'ms', 'dr', 'prof', 'fig', 'eq', 'vol'}:
        return True
    following = re.match(r"\s+(\w+)", suffix)
    return not following or following[1] not in _SENTENCE_OPENERS


def _name_initial_continues(prefix: str, suffix: str) -> bool:
    # Normalize an inspection copy only: persisted offsets refer to raw prose.
    prefix, suffix = unicodedata.normalize('NFC', prefix), unicodedata.normalize('NFC', suffix)
    initial = re.search(r"\b([^\W\d_])\.$", prefix)
    following = re.match(r"[ \t]*(?:(?:\r\n?|\n)[ \t]*)?([^\W\d_][\w’'-]*)(\.)?", suffix)
    if not initial or not initial[1].isupper() or not following:
        return False
    word = following[1]
    if word in _SENTENCE_OPENERS:
        return False
    if not (word[0].isupper() or word in {'de', 'del', 'da', 'di', 'van', 'von'}):
        return False
    before = re.sub(r"^\s*(?:\d+[.)]|[-+*])\s+", "", prefix[:initial.start()])
    prior = re.search(r"([^\W\d_][\w’'-]*)(\.)?\s+$", before)
    if prior and prior[1].lower() == 'and':
        prior = re.search(r"([^\W\d_][\w’'-]*)(\.)?\s+$", before[:prior.start()])
    # Capitalization on the right alone also describes a new sentence after
    # a letter value. Require name-shaped context on the left: a leading
    # initial, another name/initial, or a name-introducing relation.
    if prior is None:
        return not re.search(r"\w", before)
    return (prior[1][0].isupper() and (len(prior[1]) > 1 or bool(prior[2]))) or prior[1].lower() in {
        'names', 'named', 'by', 'to', 'for', 'from', 'with',
        'is', 'was', 'are', 'were', 'lists', 'listed', 'includes', 'included',
        'identifies', 'identified', 'called', 'records', 'reports',
    }


def answer_units(answer: str) -> list[dict]:
    units = []
    strong_labels = strong_label_offsets(answer)
    pending_heading = None
    pending_content_start = None
    pending_end = 0

    def append(start, end):
        # Retain exact revision offsets and the existing per-unit size bound.
        while start < end:
            stop = min(start + 1200, end)
            units.append({"id": f"u{len(units) + 1}", "start": start,
                          "end": stop, "text": answer[start:stop]})
            start = stop

    for line_match in re.finditer(r"[^\r\n]+", answer):
        line = line_match.group()
        if not line.strip() or re.fullmatch(r"([*_-])(?:[ \t]*\1){2,}", line.strip()):
            continue  # A Markdown thematic break contains no factual prose.
        first = line_match.start() + len(line) - len(line.lstrip())
        last = line_match.end() - len(line) + len(line.rstrip())
        heading = re.match(r" {0,3}#{1,6}[ \t]+\S", line)
        label = is_colon_label(line)
        if heading or label or line_match.start() in strong_labels:
            pending_heading = first if pending_heading is None else pending_heading
            pending_end = last
            continue  # Audit heading meaning together with the following claim.
        start = first
        for boundary in re.finditer(r"[.!?](?=\s|$)", line):
            end = line_match.start() + boundary.end()
            prefix = answer[pending_heading if pending_heading is not None else start:end]
            name_prefix = answer[pending_content_start if pending_content_start is not None else start:end]
            if boundary.group() == "." and (
                re.fullmatch(r"\s*\d+\.", answer[start:end])
                or _abbreviation_continues(prefix, answer[end:])
                or _name_initial_continues(name_prefix, answer[end:])
            ):
                continue
            append(pending_heading if pending_heading is not None else start, end)
            pending_heading = None
            pending_content_start = None
            start = end
            while start < last and answer[start].isspace():
                start += 1
        if start < last:
            prefix = answer[pending_heading if pending_heading is not None else start:last]
            name_prefix = answer[pending_content_start if pending_content_start is not None else start:last]
            continuation = re.match(r"[ \t]*(?:\r\n?|\n)[ \t]*(?![#>]|[-+*]\s|\d+[.)]\s)\S", answer[last:])
            if continuation and (_name_initial_continues(name_prefix, answer[last:])
                                 or _abbreviation_continues(prefix, answer[last:])):
                pending_heading = start if pending_heading is None else pending_heading
                pending_content_start = start if pending_content_start is None else pending_content_start
                pending_end = last
                continue
            append(pending_heading if pending_heading is not None else start, last)
            pending_heading = None
            pending_content_start = None
    if pending_heading is not None:
        append(pending_heading, pending_end)  # A trailing factual heading is audited.
    return units



_MARKDOWN = MarkdownIt("commonmark")
_CODE_SPANS = re.compile(r"(?<!`)(`+)(?!`)(.+?)(?<!`)\1(?!`)", re.DOTALL)


def _list_marker_ranges(text: str, *, known_start: bool = True, parsed=None) -> list[list[int]]:
    """Identify structural markers on the complete source, never a quote slice."""
    if not known_start:
        return []  # A continuation chunk cannot establish its enclosing block.
    # CommonMark normalizes CR/LF, but offsets must still point into raw OCR.
    starts = [0, *(match.end() for match in re.finditer(r"\r\n?|\n", text))]
    protected = [match.span() for match in _CODE_SPANS.finditer(text)]
    ranges = set()
    for token in _MARKDOWN.parse(text) if parsed is None else parsed:
        if token.type != "list_item_open" or token.markup not in {"-", "+", "*"} or not token.map:
            continue
        line = token.map[0]
        first, last = starts[line], starts[line + 1] if line + 1 < len(starts) else len(text)
        marker = re.match(r" {0,3}([-+*][ \t]+)(?=\S|$)", text[first:last])
        if marker:
            start, end = first + marker.start(1), first + marker.end(1)
            # A block-interrupting marker inside balanced raw code delimiters
            # is ambiguous literal text, so it cannot remove a numeric sign.
            if not any(a <= start < b for a, b in protected):
                ranges.add((start, end))
    return [list(pair) for pair in sorted(ranges)]


def _field_leader_ranges(text: str, *, known_start: bool = True, parsed=None) -> list[list[int]]:
    """Recognize explicit display-field leaders only in ordinary source prose."""
    if not known_start:
        return []
    starts = [0, *(match.end() for match in re.finditer(r"\r\n?|\n", text))]
    ranges = []
    # A bold uppercase multiword field label makes the presentation role
    # explicit. Ordinary arithmetic and free-form dashed text remain ambiguous.
    pattern = r" {0,3}\*\*[A-Z]{2,}(?:[ \t]+[A-Z]{2,})+:?\*\*:?[ \t]+((?:-[ \t]+){3,})(?=(?:\*\*)?(?:[-+](?:[ \t]+)?)?(?:[$€£]|USD[ \t]+|EUR[ \t]+|GBP[ \t]+)?\d)"
    for token in _MARKDOWN.parse(text) if parsed is None else parsed:
        if (token.type != "inline" or not token.map
                or any(child.type in {"code_inline", "html_inline"} for child in token.children or [])):
            continue
        fields = []
        for line in range(*token.map):
            first, last = starts[line], starts[line+1] if line+1 < len(starts) else len(text)
            match = re.match(pattern, text[first:last])
            if not match:
                break
            tail = text[first+match.end(1):last].rstrip("\r\n")
            visible = presentation_text(tail, list_markers=[], field_leaders=[])
            scalar = r"(?:[$€£]|USD[ \t]+|EUR[ \t]+|GBP[ \t]+)?[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[ \t]+" + VALUE_UNITS + r")?[ \t]*\.?[ \t]*"
            if not re.fullmatch(scalar, visible):
                break
            fields.append([first+match.start(1), first+match.end(1)])
        else:
            # Every line must be an independently complete display field.
            # A wrapped equation/prose continuation invalidates the paragraph.
            ranges.extend(fields)
    return ranges


def _slice_markers(markers: list, start: int, end: int) -> list[list[int]]:
    return [[first - start, last - start] for first, last in markers if start <= first < last <= end]


def _without_presentation_ranges(text: str, markers: list) -> str:
    for first, last in reversed(markers):
        text = text[:first] + text[last:]
    return text


def _value_context(text: str, markers: list = ()) -> str:
    # Guard adjacency only; never use this copy as a matching quote or evidence.
    return re.sub(r"[^\S\r\n]+", " ", re.sub(r"[*_`\[\]]", "", _without_presentation_ranges(text, markers)))


def evidence_spans(pack: dict, *, citation_safe: bool = False, diagnostics: dict | None = None) -> list[dict]:
    spans = []
    seen = {}
    source_structures = {}
    # A history pack has more priority documents to compare. Use smaller whole
    # windows throughout so requested and historical sources can coexist.
    window = 1800 if any((item.get("history_reserved") or item.get("recent_reserved")) for item in pack.get("items", []) if isinstance(item, dict)) else 4000
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
        identity = (item["document_id"], item.get("chunk_index"), item.get("title"), content, bool(item.get("feedback_open")),
                    item.get('source_context'), item.get('_source_document_content'))
        if item["id"] in seen:
            if seen[item["id"]] != identity:
                raise ValueError("Evidence identity refers to conflicting source records")
            continue
        seen[item["id"]] = identity
        digest = hashlib.sha256(content.encode()).hexdigest()
        context = certified_document_context(item, content)
        if not context and ('source_context' in item or '_source_document_content' in item):
            if diagnostics is not None:
                diagnostics['unbound_source_chunks'] = diagnostics.get('unbound_source_chunks', 0) + 1
            continue
        if context:
            document_text, offset = context
            key = (item['document_id'], item['source_context']['digest'])
            if key not in source_structures:
                parsed = _MARKDOWN.parse(document_text)
                source_structures[key] = (_list_marker_ranges(document_text, parsed=parsed),
                                          _field_leader_ranges(document_text, parsed=parsed),
                                          source_quantities.table_ranges(document_text))
            lists, fields, tables = source_structures[key]
            list_markers = _slice_markers(lists, offset, offset + len(content))
            field_leaders = _slice_markers(fields, offset, offset + len(content))
            quantity_tables = _slice_markers(tables, offset, offset + len(content))
        else:
            # A failed full-source binding never falls back to chunk-local
            # authority. Unknown continuation context also stays conservative.
            known_start = item.get('chunk_index', 0) == 0 and item.get('source_context') is None and '_source_document_content' not in item
            list_markers = _list_marker_ranges(content, known_start=known_start)
            field_leaders = _field_leader_ranges(content, known_start=known_start)
            quantity_tables = source_quantities.table_ranges(content) if known_start else []
        presentation_ranges = sorted(list_markers + field_leaders)
        guard_content, guard_offset, guard_ranges = content, 0, presentation_ranges
        if context:
            guard_content, guard_offset = context
            guard_ranges = sorted(lists + fields)
        for start in range(0, len(content), window - 200):
            text = content[start:start + window]
            guard_start, guard_end = guard_offset + start, guard_offset + start + len(text)
            spans.append({"span_id": f"{item['id']}:{digest[:16]}:{start}",
                          "evidence_id": item["id"], "document_id": item.get("document_id"),
                          "history_reserved": item.get("history_reserved") is True,
                          "recent_reserved": item.get("recent_reserved") is True,
                          "chunk_index": item.get("chunk_index", 0),
                          "title": item.get("title", ""), "start": start, "end": start + len(text),
                          "content": text, "content_digest": digest,
                          **({'source_context': item['source_context']} if context else {}),
                          "boundary_before": guard_content[max(0, guard_start - 2):guard_start],
                          "date_context_before": date_context(guard_content[:guard_start]),
                          "boundary_after": guard_content[guard_end:guard_end + 2],
                          # Bounded guard context is computed from the whole
                          # certified chunk, so markup cannot hide a token tail.
                          "list_markers": _slice_markers(list_markers, start, start + len(text)),
                          "quantity_tables": _slice_markers(quantity_tables, start, start + len(text)),
                          "field_leaders": _slice_markers(field_leaders, start, start + len(text)),
                          "value_boundary_before": _value_context(guard_content[:guard_start], _slice_markers(guard_ranges, 0, guard_start))[-16:],
                          "value_boundary_after": _value_context(guard_content[guard_end:], _slice_markers(guard_ranges, guard_end, len(guard_content)))[:2],
                          "feedback_open": bool(item.get("feedback_open"))})
    if not citation_safe:
        return spans
    safe = [candidate for span in spans if (candidate := citation_safe_span(span)) is not None]
    if diagnostics is not None:
        diagnostics['unavailable_citation_windows'] = len(spans) - len(safe)
    return safe


def citation_safe_span(span):
    """Trim only rejected edge tokens; retain one bounded original interval."""
    content = span['content']
    tokens = list(re.finditer(r'\S+', content))
    markers = sorted(span.get('list_markers', []) + span.get('field_leaders', []))
    first, last = 0, len(tokens)
    while first < last:
        start = 0 if first == 0 else tokens[first].start()
        end = len(content) if last == len(tokens) else tokens[last - 1].end()
        failures = []
        candidate = {**span,
            'span_id': (span['span_id'] if start == 0 and end == len(content) else
                        f"{span['span_id']}:safe:{start}:{end}"),
            'content': content[start:end], 'start': span['start'] + start, 'end': span['start'] + end,
            'list_markers': _slice_markers(span.get('list_markers', []), start, end),
            'field_leaders': _slice_markers(span.get('field_leaders', []), start, end),
            'quantity_tables': _slice_markers(span.get('quantity_tables', []), start, end),
            'date_context_before': date_context(span.get('date_context_before', '') + content[:start]),
            'boundary_before': (span.get('boundary_before', '') + content[:start])[-2:],
            'boundary_after': (content[end:] + span.get('boundary_after', ''))[:2],
            'value_boundary_before': (span.get('value_boundary_before', '') +
                _value_context(content[:start], _slice_markers(markers, 0, start)))[-16:],
            'value_boundary_after': (_value_context(content[end:], _slice_markers(markers, end, len(content))) +
                span.get('value_boundary_after', ''))[:2],
        }
        # Bind the exact proposed interval before validation. Searching a quote
        # inside the old window could instead find an earlier repeated occurrence.
        reference = validate_reference({
            'span_id': candidate['span_id'], 'evidence_id': candidate['evidence_id'],
            'document_id': candidate['document_id'], 'quote': candidate['content'],
        }, [{**candidate, 'feedback_open': False}], diagnostics=failures)
        if reference:
            return candidate
        if failures == ['boundary_start']:
            first += 1
        elif failures == ['boundary_end']:
            last -= 1
        else:
            return None
    return None


def _calendar_features(text: str, date_order: str, context: str = '') -> set[str]:
    features = set()
    for found in source_dates(text, date_order, context_before=context):
        if found.value and found.precision in {'month', 'day'}:
            features.add('calendar:' + found.value)
            # A day can supply a month observation, never the reverse.
            features.add('calendar:' + found.value[:7])
    return features


class EvidenceReservationError(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def select_spans(question: str, spans: list[dict], budget: int = 28000, *, serialized: bool = False, date_order: str = "mdy") -> list[dict]:
    """Bound synthesis context only; audits receive the complete eligible pack."""
    tokens = set(re.findall(r"[\w$%]+", question.lower()))
    # Retrieval metadata identifies explicitly requested documents even when
    # their short OCR has fewer shared words than a long unrelated notice.
    # This affects relevance only; reference validation still requires OCR.
    requested_ids = {int(value) for value in re.findall(
        r"\b(?:paperless\s+(?:document\s+)?|document\s+)(?:id\s*)?[:#]?\s*"
        r"([1-9]\d{0,18})\b", question, re.I)}
    quoted_titles = {normalize_quote(value).casefold() for value in re.findall(r'["“]([^"”\n]+)["”]', question)}
    costs = [len(json.dumps(span, ensure_ascii=False)) + 2 if serialized else len(span["content"]) for span in spans]
    calendar_tokens = [_calendar_features(span['content'], date_order, span.get('date_context_before', '')) for span in spans]
    span_tokens = [set(re.findall(r"[\w$%]+", span["content"].lower())) | dates for span, dates in zip(spans, calendar_tokens)]
    tokens |= _calendar_features(question, date_order)
    frequency = Counter(token for words in span_tokens for token in words)
    weights = {token: 1 + math.log((len(spans) + 1) / (count + 1)) for token, count in frequency.items()}
    def rank(pair, query_tokens=tokens):
        index, span = pair
        title = normalize_quote(str(span.get("title") or "")).casefold()
        requested = (2 if span.get("document_id") in requested_ids else
                     1 if title and title in quoted_titles else 0)
        overlap = math.fsum(weights[token] for token in query_tokens & span_tokens[index])
        return (-requested, -overlap, index)
    ranked = sorted(enumerate(spans), key=rank)
    # A long requested document must not crowd out another explicitly named
    # source before synthesis gets a chance to compare them.
    first_per_document, remaining, represented = [], [], set()
    for pair in ranked:
        doc_id = pair[1].get("document_id")
        if rank(pair)[0] < 0 and doc_id not in represented and costs[pair[0]] <= budget:
            first_per_document.append(pair)
            represented.add(doc_id)
        else:
            remaining.append(pair)
    priority_queues = []
    for scope in ("recent", "history"):
        priority, represented = [], set()
        for pair in sorted(enumerate(spans), key=lambda pair: (pair[1].get("chunk_index", 0), pair[1].get("start", 0), rank(pair) if scope == "history" else (pair[0],))):
            span = pair[1]
            if span.get(f"{scope}_reserved") and not span.get("feedback_open") and span["document_id"] not in represented:
                priority.append(pair)
                represented.add(span["document_id"])
        priority_queues.append(priority)
    history = [pair for row in zip_longest(*priority_queues) for pair in row if pair is not None]
    ranked = first_per_document + history + remaining
    explicit_indices = {pair[0] for pair in first_per_document}
    result, used, selected, contents = [], 0, set(), set()
    for index, span in ranked:
        if index in selected:
            continue
        if used + costs[index] > budget:
            continue
        # Repeated recent windows need not consume the historical context budget.
        # Preserve history reservations, canonical records and explicitly requested slots.
        if (span.get("recent_reserved") and not span.get("history_reserved")
                and span["content"] in contents and index not in explicit_indices):
            continue
        result.append(span)
        contents.add(span["content"])
        selected.add(index)
        used += costs[index]
    return result


def span_coverage(question: str, available: list[dict], selected: list[dict], *, reserve_history: bool = True) -> dict:
    requested = {int(value) for value in re.findall(
        r"\b(?:paperless\s+(?:document\s+)?|document\s+)(?:id\s*)?[:#]?\s*([1-9]\d{0,18})\b", question, re.I)}
    titles = {normalize_quote(value).casefold() for value in re.findall(r'["“]([^"”\n]+)["”]', question)}
    requested |= {s["document_id"] for s in available if normalize_quote(str(s.get("title") or "")).casefold() in titles}
    historical = {s["document_id"] for s in available if reserve_history and s.get("history_reserved") and not s.get("feedback_open")}
    delivered = {s["document_id"] for s in selected}
    recent = {s["document_id"] for s in available if reserve_history and s.get("recent_reserved") and not s.get("feedback_open")}
    priority = requested | historical | recent
    return {"requested_document_ids": sorted(requested), "reserved_document_ids": sorted(historical), "recent_document_ids": sorted(recent),
            "selected_document_ids": sorted(delivered), "omitted_priority_document_ids": sorted(priority - delivered),
            "limited": bool(priority - delivered)}


def _plain_field_labels(text: str, content_start: int = 0, content_end: int | None = None):
    """Ignore only balanced bold alphabetic field labels, retaining raw ranges."""
    pattern = r"(?<![\w*\\])\*\*([A-Za-z][A-Za-z \t]*:)\*\*(?=\s|$)"
    characters, ranges, cursor = [], [], 0
    for match in re.finditer(pattern, text):
        if match.start() and unicodedata.category(text[match.start() - 1]).startswith("M"):
            continue
        # Boundary context may guard a wrapper, but cannot authorize one whose
        # own opening/closing syntax lies outside the selected source content.
        if match.start() < content_start or match.end() > (len(text) if content_end is None else content_end):
            continue
        for index in range(cursor, match.start()):
            characters.append(text[index])
            ranges.append((index, index + 1))
        for index in range(match.start(1), match.end(1)):
            characters.append(text[index])
            ranges.append((match.start() if index == match.start(1) else index,
                           match.end() if index == match.end(1) - 1 else index + 1))
        cursor = match.end()
    for index in range(cursor, len(text)):
        characters.append(text[index])
        ranges.append((index, index + 1))
    return "".join(characters), ranges


def _quote_range(source: str, quote: str):
    normalized = normalize_quote(quote)
    if normalized not in normalize_quote(source):
        return None
    start = source.find(quote)
    if start >= 0:
        return start, start + len(quote)
    words = list(re.finditer(r"\S+", source))
    tokens = [unicodedata.normalize("NFC", w.group()) for w in words]
    wanted = normalized.split(" ")
    found = next((i for i in range(len(tokens) - len(wanted) + 1)
                  if tokens[i:i + len(wanted)] == wanted), None)
    return None if found is None else (words[found].start(), words[found + len(wanted) - 1].end())


def validate_reference(reference: Any, spans: list[dict], *, diagnostics: list | None = None) -> dict | None:
    def reject(reason):
        if diagnostics is not None:
            diagnostics.append(reason)
        return None
    if not isinstance(reference, dict):
        return reject('invalid_reference_shape')
    span = next((s for s in spans if s["span_id"] == reference.get("span_id")), None)
    if not span:
        return reject('unknown_or_unselected_span')
    if span["feedback_open"]:
        return reject('source_feedback_open')
    if set(reference) == {'span_id'}:
        reference = {**reference, 'evidence_id': span['evidence_id'],
                     'document_id': span['document_id'], 'quote': span['content']}
    if reference.get("evidence_id") != span["evidence_id"] or type(reference.get("document_id")) is not int:
        return reject('identity_mismatch')
    if reference["document_id"] != span["document_id"]:
        return reject('identity_mismatch')
    quote = reference.get("quote")
    if not isinstance(quote, str) or not normalize_quote(quote):
        return reject('empty_quote')
    source = span["content"]
    bounds = _quote_range(source, quote)
    if bounds is None:
        prefix, suffix = span.get("boundary_before", ""), span.get("boundary_after", "")
        visible, ranges = _plain_field_labels(
            prefix + source + suffix, len(prefix), len(prefix) + len(source))
        # Context controls the grammar, but a match must begin and end in
        # this span. An outside prefix occurrence must not hide a later match.
        eligible = [i for i, (start, end) in enumerate(ranges)
                    if len(prefix) <= start < end <= len(prefix) + len(source)]
        if not eligible:
            return reject('quote_not_in_source')
        first, last = eligible[0], eligible[-1] + 1
        visible, ranges = visible[first:last], ranges[first:last]
        plain_quote, _ = _plain_field_labels(quote)
        visible_bounds = _quote_range(visible, plain_quote)
        if visible_bounds is None:
            return reject('quote_not_in_source')
        first, last = visible_bounds
        bounds = ranges[first][0] - len(prefix), ranges[last - 1][1] - len(prefix)
        if not 0 <= bounds[0] < bounds[1] <= len(source):
            return reject('quote_not_in_source')
    start, end = bounds
    before = (span.get("boundary_before", "") + source[:start])[-1:]
    after = (source[end:] + span.get("boundary_after", ""))[:2]
    if before and source[start].isalnum() and (before.isalnum() or before == "_"):
        return reject('boundary_start')
    if after and source[end - 1].isalnum() and (after[0].isalnum() or after[0] == "_"):
        return reject('boundary_end')
    if source[start].isdigit() and before in {"+", "-", "−", ".", ","}:
        return reject('boundary_start')
    if source[end - 1].isdigit() and len(after) > 1 and after[0] in ".,/-" and after[1].isdigit():
        return reject('boundary_end')
    if source[end - 1] in ".," and end - start > 1 and source[end - 2].isdigit() and after[:1].isdigit():
        return reject('boundary_end')
    source_list_markers = _slice_markers(span.get("list_markers", []), start, end)
    source_field_leaders = _slice_markers(span.get("field_leaders", []), start, end)
    markers = sorted(span.get("list_markers", []) + span.get("field_leaders", []))
    visible = presentation_text(source[start:end], list_markers=source_list_markers, field_leaders=source_field_leaders)
    before_visible = (span.get("value_boundary_before", _value_context(span.get("boundary_before", "")))
                      + _value_context(source[:start], _slice_markers(markers, 0, start)))
    after_visible = (_value_context(source[end:], _slice_markers(markers, end, len(source)))
                     + span.get("value_boundary_after", _value_context(span.get("boundary_after", ""))))
    if visible:
        first, last = visible[0], visible[-1]
        before, after = before_visible[-1:], after_visible[:2]
        if first.isalnum() and before and (before.isalnum() or before == "_"):
            return reject('boundary_start')
        if last.isalnum() and after and (after[0].isalnum() or after[0] == "_"):
            return reject('boundary_end')
        numeric_start = re.match(r"(?:USD|EUR|GBP|CAD|AUD|JPY|[$€£])?\s*\d", visible)
        if numeric_start and (before in {"+", "-", "−", ".", ","}
                              or re.search(r"[+−-]\s*(?:USD|EUR|GBP|CAD|AUD|JPY|[$€£])?\s*$", before_visible)):
            return reject('boundary_start')
        if last.isdigit() and len(after) > 1 and after[0] in ".,/-" and after[1].isdigit():
            return reject('boundary_end')
        if last in ".," and len(visible) > 1 and visible[-2].isdigit() and after[:1].isdigit():
            return reject('boundary_end')
        # A truncated delimiter-only context cannot establish the token's edge.
        if numeric_start and "value_boundary_before" not in span and not before_visible and span["start"] > len(span.get("boundary_before", "")):
            return reject('boundary_start')
        if last.isdigit() and "value_boundary_after" not in span and span.get("boundary_after") and (
            not after_visible or after_visible in {".", ",", "/", "-"}
        ):
            return reject('boundary_end')
    return {"span_id": span["span_id"], "evidence_id": span["evidence_id"],
            "document_id": span["document_id"], "source_title": span["title"],
            "quote": source[start:end], "source_list_markers": source_list_markers,
            "source_field_leaders": source_field_leaders,
            "source_quantity_tables": _slice_markers(span.get("quantity_tables", []), start, end),
            **({'source_context': span['source_context']} if span.get('source_context') else {}),
            "start": span["start"] + start,
            "date_context_before": date_context(span.get("date_context_before", "") + source[:start]),
            "end": span["start"] + end, "content_digest": span["content_digest"]}


def presentation_text(text: str, *, list_markers: list | None = None, field_leaders: list | None = None) -> str:
    markers = (_list_marker_ranges(text) if list_markers is None else list_markers)
    leaders = (_field_leader_ranges(text) if field_leaders is None else field_leaders)
    text = _without_presentation_ranges(text, sorted(markers + leaders))
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
    # Whitespace around a displayed sign must not turn a negative quantity into
    # a positive one after its balanced formatting is peeled.
    text = re.sub(r"(?<![\w.,])([+-])\s+(?=(?:USD|EUR|GBP|CAD|AUD|JPY|[$€£])?\s*\d)", r"\1", text)
    return re.sub(r"(?<![\w.,])([+-])((?:USD|EUR|GBP|CAD|AUD|JPY|[$€£])\s*)(?=\d)", r"\2\1", text)


def value_mismatches(text: str, references: list[dict], *, date_order: str = "mdy") -> dict:
    # Supplement (never replace) semantic audit. Exact source values are needed
    # for generated precise numbers and named units. Computations need their own
    # explicit calculation evidence; the auditor cannot simply bless a new value.
    # Compare rendered prose without changing the audited revision or offsets.
    text = canonical_prose(text)
    text = re.sub(r"^\s*\d+\.(?:\s|$)", "", text, flags=re.MULTILINE)
    text = presentation_text(text)
    # A quote boundary is not source adjacency, even within the same document.
    sources = [r["quote"].replace("−", "-") for r in references]
    # Bare four-digit tokens remain scalars: identifiers and quantities may
    # look like years and still require the original Decimal comparison.
    dates = [found for found in source_dates(text, date_order) if not re.fullmatch(r"\d{4}", found.text)]
    source_occurrences = [source_dates(source, date_order, context_before=ref.get("date_context_before", ""))
                          for source, ref in zip(sources, references)]
    missing_dates = [found.text for found in dates
                     if not any(date_supported(found, actual) for occurrences in source_occurrences for actual in occurrences)]
    numeric_text = without_dates(text, dates)
    numeric_sources = [presentation_text(source, list_markers=ref.get("source_list_markers", []), field_leaders=ref.get("source_field_leaders", []))
                       for source, ref in zip(sources, references)]
    mismatches = {}
    if missing_dates:
        mismatches["dates"] = list(dict.fromkeys(missing_dates))[:30]
    number = r"[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?"
    def numbers(value):
        return {Decimal(n.replace(",", "")) for n in re.findall(r"(?<![\w.,])(" + number + r")(?!\w|[.,]\d)", value)}
    missing_numbers = numbers(numeric_text) - {value for source in numeric_sources for value in numbers(source)}
    if missing_numbers:
        mismatches["values"] = sorted(str(value) for value in missing_numbers)[:30]
    source_pairs = set()
    available_units = set()
    for source, reference in zip(numeric_sources, references):
        source_pairs.update(source_quantities.prose_quantities(source))
        source_pairs.update(source_quantities.table_quantities(
            reference['quote'], reference.get('source_quantity_tables', [])))
        available_units.update(source_quantities.unit_names(source))
    missing_units = source_quantities.unit_names(text) - source_quantities.source_currency_units(available_units)
    if missing_units:
        mismatches["units"] = sorted(missing_units)[:30]
    missing_quantities = source_quantities.prose_quantities(text) - source_quantities.source_currency_quantities(source_pairs)
    if missing_quantities:
        mismatches["quantities"] = sorted(f"{amount} {unit}" for amount, unit in missing_quantities)[:30]
    return mismatches


def values_match(text: str, references: list[dict], *, date_order: str = "mdy") -> bool:
    return not value_mismatches(text, references, date_order=date_order)


def date_occurs(value: str, source: str, date_order: str = "mdy", *, context_before: str = "") -> bool:
    return source_date_occurs(value, source, date_order, context_before=context_before)


def empty_ledger(candidate: str, observations: ObservationCandidate | None = None) -> dict:
    total = len(observations.units() if observations else answer_units(candidate))
    return {"claims": [], "summary": {"total": total, "supported": 0, "audit_coverage": 0},
            "complete": False, "spans": [], "available_span_count": 0,
            "unitization": observations.strategy if observations else 'prose_v1',
            "candidate_digest": hashlib.sha256(candidate.encode()).hexdigest()}


def temporal_acceptance(question, candidate, ledger, plan, evidence_pack, evaluated_at):
    # Keywords only identify claims that need a temporal assessment. An explicit
    # source-relative assessment can establish a documented comparison, never
    # the completeness of the archive or present real-world validity.
    current_words = r"\b(?:currently|current|active|today|now|still|latest)\b"
    claims = ledger["claims"]
    required = bool(plan.get("requires_current")) or bool(
        re.search(current_words, question + " " + candidate, re.I)
        or any(c["temporal_scope"] in {"current", "documented"} for c in claims))
    current = current_state({**plan, "requires_current": required}, evidence_pack, evaluated_at)
    if not required:
        return "supported", current
    source_relative = bool(claims) and all(
        c["temporal_scope"] == "documented" or (
            c["temporal_scope"] == "historical" and (
                c.get("temporal_assertion") == "source_observation" or not re.search(current_words, c["claim"], re.I)))
        for c in claims)
    if not source_relative:
        return "current_unresolved", current
    compared = sorted({doc for c in claims for doc in c.get("comparison_document_ids", [])})
    if compared:
        current.update(status="documented", comparison_scope="retrieved_documents",
                       comparison_document_ids=compared,
                       note="The comparison describes the retrieved documents; current real-world status and archive completeness are not established.")
    return "qualified", current


def audit_protocol_errors(raw: Any, units: list[dict]) -> list[str]:
    """Structural errors only; semantic rejection never justifies another vote."""
    if not raw:
        return ["unavailable"]
    if isinstance(raw, dict) and 'audit_protocol_error' in raw:
        reason = raw['audit_protocol_error']
        return ['semantic_protocol_' + reason] if isinstance(reason, str) and reason in PROTOCOL_ERRORS else ['invalid_assessments']
    if not isinstance(raw, dict) or not isinstance(raw.get("assessments"), list):
        return ["invalid_assessments"]
    expected = {unit["id"] for unit in units}
    assessments = raw["assessments"]
    if any(not isinstance(item, dict) for item in assessments):
        return ["invalid_assessment"]
    ids = [item.get("unit_id") for item in assessments]
    errors = []
    if any(not isinstance(value, str) or value not in expected for value in ids):
        errors.append("unexpected_unit_id")
    valid_ids = [value for value in ids if isinstance(value, str) and value in expected]
    if len(set(valid_ids)) != len(valid_ids):
        errors.append("duplicate_unit_id")
    if set(valid_ids) != expected:
        errors.append("missing_unit_id")
    if any(not isinstance(item.get("status"), str) or
           item["status"] not in {"supported", "unsupported", "missing", "conflicting"} for item in assessments):
        errors.append("invalid_status")
    if any(not isinstance(item.get("references"), list) for item in assessments):
        errors.append("invalid_references_list")
    return errors


def subset_source_reservations(candidate, ledger, revised_units, retained_ids):
    """Bind unchanged survivors in order, including duplicate observation text."""
    claims = ledger['claims']
    units = (ObservationCandidate.from_text(candidate).units() if ledger.get('unitization') == 'observations_v1'
             else answer_units(candidate))
    if (ledger.get('candidate_digest') != hashlib.sha256(candidate.encode()).hexdigest()
            or len(claims) != len(units)
            or any(any(c.get(k) != u[k] for k in ('id', 'start', 'end')) or c.get('claim') != u['text']
                   for c, u in zip(claims, units))
            or not isinstance(retained_ids, list) or len(set(retained_ids)) != len(retained_ids)):
        raise EvidenceReservationError('invalid_reserved_candidate')
    survivors = [c for c in claims if c['id'] in retained_ids]
    if ([c['id'] for c in survivors] != retained_ids or len(survivors) != len(revised_units)
            or any(c['claim'] != u['text'] or c['status'] != 'supported' or not c['references']
                   for c, u in zip(survivors, revised_units))):
        raise EvidenceReservationError('invalid_reserved_candidate')
    return {unit['id']: claim['references'] for claim, unit in zip(survivors, revised_units)}


class AnswerFinalizer:
    def __init__(self, auditor, repairer=None, *, timeout_seconds: float = 60, max_units: int = 80,
                 concurrency: int = 4, date_order: str = "mdy"):
        self.date_order = date_order
        self.auditor = auditor
        self.repairer = repairer
        self.timeout_seconds = timeout_seconds
        self.max_units = max_units
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or type(concurrency) is not int or concurrency < 1:
            raise ValueError("Audit timeout and concurrency must be positive")
        self.concurrency = concurrency

    def _audit_timeout(self, candidate, observations=None):
        units = observations.units() if observations else answer_units(candidate)
        batches = math.ceil(min(len(units), self.max_units) / 4)
        return self.timeout_seconds * max(1, math.ceil(batches / self.concurrency))

    async def _audit(self, question, answer, pack, plan, declarations=(), *, diagnostics=None, observations=None, source_reservations=None, progress=None):
        diagnostics = diagnostics if diagnostics is not None else []
        if observations and observations.text != answer:
            raise ValueError('Observation candidate text mismatch')
        units = observations.units() if observations else answer_units(answer)
        progress = progress if progress is not None else {}
        source_diagnostics = {}
        canonical = evidence_spans(pack, citation_safe=True, diagnostics=source_diagnostics)
        spans = [span for span in canonical if not span.get('feedback_open')]
        serialized = json.dumps(spans, ensure_ascii=False)
        admission = {
            'policy': 'complete_eligible_manifest_v1',
            'canonical_windows': len(canonical),
            'eligible_windows': len(spans), 'supplied_windows': len(spans),
            'excluded_windows': len(canonical) - len(spans),
            'canonical_documents': len({s['document_id'] for s in canonical}),
            'eligible_documents': len({s['document_id'] for s in spans}),
            'supplied_documents': len({s['document_id'] for s in spans}),
            'excluded_documents': len({s['document_id'] for s in canonical} - {s['document_id'] for s in spans}),
            'serialized_chars': len(serialized), 'serialized_bytes': len(serialized.encode('utf-8')),
        }
        progress.update(available_span_count=len(spans), source_diagnostics=source_diagnostics,
                        selection_coverage=[])
        manifest, claims, rejection_reasons = {}, [], []
        checked = 0
        results = []
        context = '' if observations else audit_context(answer)
        complete = bool(units) and len(units) <= self.max_units and len(context) <= self.max_units * 1200 and bool(spans)
        if complete:
            # Preserve surrounding dated/section context across batches. This
            # is bounded answer prose, not an additional source of evidence.
            audit_plan = {**plan, "source_date_order": self.date_order, "answer_context": context,
                          "unitization": observations.strategy if observations else 'prose_v1'}
            batches = [units[offset:offset + 4] for offset in range(0, len(units), 4)]
            for unit_id, references in (source_reservations or {}).items():
                if unit_id not in {u['id'] for u in units} or not references:
                    raise EvidenceReservationError('invalid_reserved_candidate')
                for ref in references:
                    if not isinstance(ref, dict) or not ref or validate_reference(ref, spans) != ref:
                        raise EvidenceReservationError('invalid_reserved_source')
            # Validate continuity before any calls. Every batch receives the same
            # complete source opportunities; no prior verdict is supplied.
            diagnostics.extend({"batch": index, "attempts": 0, "status": "pending", "initial_errors": [], "final_errors": []}
                               for index in range(len(batches)))
            coverage = progress['selection_coverage']
            coverage.extend({'batch': index, **span_coverage(question, spans, [], reserve_history=False),
                             **admission, 'supplied_windows': 0, 'supplied_documents': 0,
                             'dispatched': False}
                            for index in range(len(batches)))
            pending = iter(enumerate(batches))
            results = [None] * len(batches)
            async def worker():
                for index, batch in pending:
                    selected = spans
                    protocol = diagnostics[index]
                    async def invoke(audit_request_plan):
                        protocol.update(attempts=protocol["attempts"] + 1, status="running")
                        coverage[index].update(**span_coverage(question, spans, selected, reserve_history=False),
                                               supplied_windows=len(selected),
                                               supplied_documents=admission['eligible_documents'], dispatched=True)
                        # Preserve sent-source identity and admission even when an
                        # outer timeout prevents this coroutine from returning.
                        progress['spans'] = selected
                        try:
                            return await self.auditor.audit_answer_units(
                                question, batch, selected, {**audit_request_plan, 'evidence_selection': admission})
                        except asyncio.CancelledError:
                            protocol.update(status="cancelled", final_errors=["cancelled"])
                            raise
                        except TimeoutError:
                            protocol.update(status="failed", final_errors=["timeout"])
                            raise
                        except Exception:
                            protocol.update(status="failed", final_errors=["unavailable"])
                            raise
                    raw = await invoke(audit_plan)
                    errors = audit_protocol_errors(raw, batch)
                    protocol["initial_errors"] = errors
                    if errors and errors != ["unavailable"]:
                        correction_plan = {**audit_plan, "audit_protocol_recovery": {
                            "expected_unit_ids": [unit["id"] for unit in batch], "errors": errors}}
                        raw = await invoke(correction_plan)
                        errors = audit_protocol_errors(raw, batch)
                    protocol.update(status="invalid" if errors else "corrected" if protocol["attempts"] == 2 else "valid",
                                    final_errors=errors)
                    results[index] = (selected, raw, protocol)
            async with asyncio.TaskGroup() as group:
                for _ in range(min(self.concurrency, len(batches))):
                    group.create_task(worker())
            for batch, (selected, raw, protocol) in zip(batches, results):
                manifest.update({s["span_id"]: s for s in selected})
                assessments = raw["assessments"] if not protocol["final_errors"] else []
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
                    reference_diagnostics = []
                    reference_diagnostic_counts = {}
                    refs = []
                    for index, reference in enumerate(raw_refs):
                        failures = []
                        refs.append(validate_reference(reference, selected, diagnostics=failures))
                        for reason in failures:
                            reference_diagnostic_counts[reason] = reference_diagnostic_counts.get(reason, 0) + 1
                            if len(reference_diagnostics) < 8:
                                reference_diagnostics.append({'index': index, 'reason': reason})
                    if not raw_refs:
                        reference_diagnostics.append({'reason': 'no_references'})
                        reference_diagnostic_counts['no_references'] = 1
                    valid = bool(refs) and all(refs)
                    status = assessment.get("status", "unchecked")
                    if status not in {"supported", "unsupported", "conflicting", "missing"}:
                        status = "unchecked"
                    model_status = assessment.get("model_status", status)
                    if status != "unchecked":
                        checked += 1
                    semantic = assessment.get('semantic_decision')
                    claim_rejections = list(semantic['rejection_reasons']) if semantic else []
                    mismatch_details = {}
                    if not valid:
                        claim_rejections.append("missing_evidence" if not raw_refs else "invalid_reference")
                    else:
                        mismatch_details = value_mismatches(unit["text"], refs, date_order=self.date_order)
                        if mismatch_details:
                            claim_rejections.append("value_mismatch")
                    if status == "supported" and claim_rejections:
                        status = "unsupported"
                    claims.append({"id": unit["id"], "claim": unit["text"], "start": unit["start"],
                                   "end": unit["end"], "status": status, "references": [r for r in refs if r],
                                   "model_status": model_status, "reference_diagnostics": reference_diagnostics,
                                   "reference_diagnostic_counts": reference_diagnostic_counts,
                                   "reference_diagnostics_omitted": sum(reference_diagnostic_counts.values()) - len(reference_diagnostics),
                                   "rejection_reasons": claim_rejections, "value_mismatches": mismatch_details,
                                   "document_id": refs[0]["document_id"] if valid else None,
                                   "evidence_ids": [r["evidence_id"] for r in refs if r],
                                   "evidence_quote": refs[0]["quote"] if valid else "",
                                   "source_title": refs[0]["source_title"] if valid else ""})
                    if semantic:
                        claims[-1]['semantic_decision'] = semantic
                    assertion = assessment.get("temporal_assertion")
                    scope = assessment.get("temporal_scope", "unknown")
                    claims[-1]["temporal_scope"] = scope if isinstance(scope, str) and scope in {"historical", "documented", "current", "none", "unknown"} else "unknown"
                    if assertion is not None:
                        if isinstance(assertion, str) and (scope, assertion) in {
                                ("historical", "source_observation"), ("documented", "retrieved_comparison"),
                                ("current", "present_world"), ("none", "none")}:
                            claims[-1]["temporal_assertion"] = assertion
                        else:
                            claims[-1]["status"] = "unsupported"
                            claims[-1]["rejection_reasons"].append("invalid_temporal_assertion")
                    if scope == "documented":
                        compared = assessment.get("comparison_document_ids")
                        available = {span["document_id"] for span in selected if not span.get("feedback_open")}
                        if (assessment.get("comparison_scope") == "retrieved_documents"
                                and isinstance(compared, list) and compared
                                and all(type(doc) is int and doc in available for doc in compared)
                                and {r["document_id"] for r in refs if r}.issubset(set(compared))):
                            claims[-1].update(comparison_scope="retrieved_documents",
                                              comparison_document_ids=sorted(set(compared)))
                        else:
                            claims[-1]["temporal_scope"] = "unknown"
                            if claims[-1]['status'] != 'conflicting':
                                claims[-1]["status"] = "unsupported"
                            claims[-1]["rejection_reasons"].append("invalid_comparison_scope")
        for declaration in declarations:
            preceding = [claim for claim in claims if claim["start"] < declaration["offset"]]
            if not preceding:
                complete = False
                rejection_reasons.append("invalid_attribution")
                continue
            claim = preceding[-1]
            if declaration["document_id"] not in {r["document_id"] for r in claim["references"]}:
                claim["status"] = "unsupported"
                claim["rejection_reasons"].append("invalid_attribution")
        # Formatting and quantities can cross audit-unit boundaries. Recheck the
        # complete revision before certifying it; references remain separate quotes.
        if claims and all(claim["status"] == "supported" for claim in claims):
            references = [reference for claim in claims for reference in claim["references"]]
            if not values_match(answer, references, date_order=self.date_order):
                for claim in claims:
                    claim["status"] = "unsupported"
                    claim["rejection_reasons"].append("answer_value_mismatch")
        complete = complete and checked == len(units)
        summary = {status: sum(c["status"] == status for c in claims)
                   for status in ("supported", "unsupported", "conflicting", "missing", "unchecked")}
        summary.update(total=len(units), audited=checked, audit_coverage=checked / len(units) if units else 0,
                       support_ratio=summary["supported"] / len(units) if units else 0)
        return {"claims": claims, "summary": summary, "complete": complete,
                "unitization": observations.strategy if observations else 'prose_v1',
                "source_diagnostics": source_diagnostics,
                "rejection_reasons": rejection_reasons,
                "spans": list(manifest.values()), "available_span_count": len(spans),
                "candidate_digest": hashlib.sha256(answer.encode()).hexdigest(),
                "selection_coverage": progress["selection_coverage"],
                "audit_batches": diagnostics}

    async def finalize(self, question: str, answer: str | ObservationCandidate, evidence_pack: dict, *, plan: dict | None = None,
                       mode: str = "strict", evaluated_at: str | None = None) -> dict:
        plan = dict(plan or {})
        evaluated_at = evaluated_at or datetime.now(timezone.utc).date().isoformat()
        if not parse_date(evaluated_at) or parse_date(evaluated_at)[1] != "day":
            raise ValueError("evaluated_at must be a valid ISO calendar day")
        plan["evaluated_at"] = evaluated_at
        disposition, attempts, error = "incomplete", 0, None
        repair_diagnostic = None
        repair_in_progress = False
        observations = None
        if isinstance(answer, ObservationCandidate):
            # Dataclass construction itself does not validate its fields. Re-enter
            # the same contract used by model responses before making any call.
            if not isinstance(answer.observations, tuple):
                raise ObservationValidationError('invalid_observations')
            observations = ObservationCandidate.from_response({'observations': list(answer.observations)})
            candidate, declarations = canonical_candidate(observations.text, evidence_pack)
            if candidate != observations.text or declarations:
                raise ObservationValidationError('invalid_attribution')
        else:
            candidate, declarations = canonical_candidate(str(answer or ""), evidence_pack)
        ledger = empty_ledger(candidate, observations)
        if mode == "quick" and observations is None:
            disposition = "unaudited"
        else:
            try:
                for attempt in range(2 if self.repairer else 1):
                    attempts += 1
                    # A failed second audit must not attach the prior
                    # candidate's ledger to the replacement's digest.
                    ledger = empty_ledger(candidate, observations)
                    ledger["audit_batches"] = diagnostics = []
                    async with asyncio.timeout(self._audit_timeout(candidate, observations)):
                        ledger = await self._audit(question, candidate, evidence_pack, plan, declarations,
                                                   diagnostics=diagnostics, observations=observations, progress=ledger)
                    summary = ledger["summary"]
                    if attempt > 0 and summary['total'] > self.max_units:
                        repair_diagnostic = {'reason': 'audit_unit_limit',
                                             'unit_count': summary['total'], 'unit_limit': self.max_units}
                    if ledger["complete"] and summary["supported"] == summary["total"]:
                        disposition, _ = temporal_acceptance(
                            question, candidate, ledger, plan, evidence_pack, evaluated_at)
                        if disposition in {"supported", "qualified"}:
                            break
                    else:
                        disposition = "unsupported" if ledger["complete"] else "incomplete"
                    if not summary["total"] or summary["audited"] != summary["total"]:
                        # Failed audit execution stops here. A fully audited
                        # candidate with invalid attribution can still be repaired.
                        error = "The source audit did not complete."
                        break
                    if attempt == 0 and self.repairer and len(candidate) <= 96000:
                        repair_in_progress = True
                        async with asyncio.timeout(self.timeout_seconds):
                            repaired = await self.repairer.repair_answer(
                                question, candidate, json.dumps(ledger["spans"], ensure_ascii=False),
                                {"status": disposition, "claims": ledger["claims"],
                                 "rejection_reasons": ledger.get("rejection_reasons", [])})
                        repair_in_progress = False
                        revised_observations = (ObservationCandidate.from_response(repaired)
                                                if isinstance(repaired, dict) and 'observations' in repaired else None)
                        replacement = (revised_observations.text if revised_observations else
                                       repaired.get("answer") if isinstance(repaired, dict) else None)
                        if not isinstance(replacement, str) or not replacement.strip():
                            disposition, error = "audit_failed", "The answer repair returned no valid candidate."
                            repair_diagnostic = {'reason': 'transport_unavailable' if repaired is None else 'invalid_object'}
                            break
                        revised, revised_declarations = canonical_candidate(replacement, evidence_pack)
                        if revised_observations and (revised != replacement or revised_declarations):
                            raise ObservationValidationError('invalid_attribution')
                        if (revised, revised_declarations, revised_observations) == (candidate, declarations, observations):
                            break
                        candidate, declarations = revised, revised_declarations
                        observations = revised_observations
                    else:
                        break
            except ObservationValidationError as exc:
                disposition, error = 'audit_failed', 'The answer repair did not satisfy the observation format.'
                repair_diagnostic = exc.diagnostic
            except TimeoutError:
                disposition, error = "timeout", "The source audit exceeded its time budget."
                if repair_in_progress:
                    repair_diagnostic = {'reason': 'transport_unavailable'}
            except Exception:
                disposition, error = "audit_failed", "The source audit was unavailable or returned invalid data."
                if repair_in_progress:
                    repair_diagnostic = {'reason': 'transport_unavailable'}
        partial = None
        # Only the final completed, nonconflicting audit is eligible. Never reuse
        # an earlier ledger after a failed repair/audit, or slice within a unit.
        summary = ledger["summary"]
        if (disposition == "unsupported" and ledger["complete"]
                and summary.get("audited") == summary.get("total")
                and 0 < summary.get("supported", 0) < summary.get("total", 0)
                and not ledger.get("rejection_reasons")
                and all(c["status"] in {"supported", "unsupported", "missing"}
                        and not ({"invalid_attribution", "answer_value_mismatch", "invalid_comparison_scope", "invalid_temporal_assertion"}
                                 & set(c.get("rejection_reasons", []))) for c in ledger["claims"])):
            subset_observations = None
            try:
                if observations:
                    subset_observations, selection = observations.supported_subset(ledger['claims'])
                    subset = subset_observations.text if subset_observations else None
                else:
                    selection = supported_revision(candidate, ledger["claims"], answer_units)
                    subset = selection.pop("candidate")
            except (ValueError, TypeError, KeyError):
                subset, selection = None, {'reason': 'invalid_candidate_units'}
                disposition, error = 'audit_failed', 'The partial answer units did not match the audited candidate.'
            ledger["subset_selection"] = selection
            if subset:
                ledger["subset_audit_batches"] = subset_diagnostics = []
                ledger['subset_audit'] = subset_progress = empty_ledger(subset, subset_observations)
                subset_progress['audit_batches'] = subset_diagnostics
                try:
                    subset_units = subset_observations.units() if subset_observations else answer_units(subset)
                    reservations = subset_source_reservations(candidate, ledger, subset_units, selection['retained_ids'])
                    async with asyncio.timeout(self._audit_timeout(subset, subset_observations)):
                        subset_ledger = await self._audit(question, subset, evidence_pack, plan,
                            diagnostics=subset_diagnostics, observations=subset_observations, source_reservations=reservations,
                            progress=subset_progress)
                    subset_status, _ = temporal_acceptance(question, subset, subset_ledger, plan, evidence_pack, evaluated_at)
                    ledger['subset_audit'] = {
                        'candidate_digest': subset_ledger['candidate_digest'],
                        'unitization': subset_ledger['unitization'],
                        'complete': subset_ledger['complete'], 'summary': subset_ledger['summary'],
                        'rejection_reasons': subset_ledger.get('rejection_reasons', []),
                        'claims': subset_ledger['claims'], 'audit_batches': subset_diagnostics,
                        'spans': subset_ledger['spans'], 'available_span_count': subset_ledger['available_span_count'],
                        'selection_coverage': subset_ledger['selection_coverage'],
                        'source_diagnostics': subset_ledger['source_diagnostics'],
                        'temporal_disposition': subset_status,
                    }
                    if (subset_ledger["complete"] and subset_ledger["summary"]["supported"] == subset_ledger["summary"]["total"]
                            and subset_status in {"supported", "qualified"}):
                        omitted = selection["omitted_units"]
                        partial = {"original_candidate_digest": ledger["candidate_digest"],
                                   "original_total": summary["total"], "original_supported": summary["supported"],
                                   "omitted_count": len(omitted),
                                   "omitted_units": omitted}
                        candidate, ledger, disposition = subset, subset_ledger, "partial"
                    elif not subset_ledger['complete']:
                        disposition, error = 'incomplete', 'The partial answer source audit did not complete.'
                except EvidenceReservationError as exc:
                    ledger['subset_evidence_diagnostic'] = {'reason': exc.reason}
                    disposition = 'audit_failed'
                    error = 'The partial answer could not retain its required source evidence.'
                except TimeoutError:
                    disposition, error = "timeout", "The partial answer source audit exceeded its time budget."
                except Exception:
                    disposition, error = "audit_failed", "The partial answer source audit was unavailable or returned invalid data."
        temporal_disposition, current = temporal_acceptance(
            question, candidate, ledger, plan, evidence_pack, evaluated_at)
        if disposition in {"supported", "qualified", "current_unresolved"}:
            disposition = temporal_disposition
        complete = disposition in {"supported", "qualified"}
        supported = complete or disposition == "partial"
        public_answer = candidate if supported else ABSTENTION
        if disposition == "unaudited" and evidence_pack.get("items"):
            public_answer = "Unaudited quick answer — verify the source documents before relying on it.\n\n" + candidate
        refs = [r for claim in ledger["claims"] for r in claim["references"]] if supported else []
        doc_ids = list(dict.fromkeys(r["document_id"] for r in refs))
        if supported:
            public_answer = render_verified_answer(candidate, ledger["claims"],
                partial=disposition == "partial", qualified=temporal_disposition == "qualified", evaluated_at=evaluated_at)
        verification = {"status": "verified" if complete else disposition,
                        "supported_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "supported"],
                        "unsupported_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "unsupported"],
                        "stale_or_conflicting_claims": [c["claim"] for c in ledger["claims"] if c["status"] == "conflicting"],
                        "missing_evidence": [] if supported else [error or "Not every material claim has validated source support."],
                        "notes": ["Semantic support is model-assessed; source membership and audit coverage are checked independently."]}
        if partial:
            verification["partial"] = partial
            verification["missing_evidence"] = ["Some claims were omitted because they could not be verified."]
        revision = hashlib.sha256(candidate.encode()).hexdigest()
        # Public manifest records exactly what was sent without duplicating full OCR.
        for audit in (ledger, ledger.get('subset_audit', {})):
            for claim in audit.get('claims', []):
                if 'semantic_decision' in claim:
                    claim['semantic_decision'] = {key: value for key, value in claim['semantic_decision'].items()
                                                  if key not in {'source_basis', 'unresolved_assumptions'}}
            if 'spans' in audit:
                audit['spans'] = [{k: v for k, v in span.items() if k not in {"content", "boundary_before", "boundary_after", "date_context_before"}} for span in audit['spans']]
        result = {"answer": public_answer, "verification": verification, "claim_ledger": ledger,
                "current_state": current,
                "finalization": {"policy_version": POLICY_VERSION, "disposition": disposition,
                                 **({'repair_diagnostic': repair_diagnostic} if repair_diagnostic else {}),
                                 "answer_digest": hashlib.sha256(public_answer.encode()).hexdigest(),
                                 "candidate_digest": revision, "evaluated_at": evaluated_at,
                                 "documented_qualification": supported and temporal_disposition == "qualified",
                                 "attempts": attempts, "complete": complete, "answer_verified": supported, "cited_document_ids": doc_ids},
                "evidence": {"score": 0.65 if disposition in {"qualified", "partial"} else 0.8 if supported else 0.25 if disposition == "unaudited" else 0.0,
                             "level": "medium" if disposition in {"qualified", "partial"} else "high" if supported else "low", "audit_status": disposition,
                             "source_count": len(doc_ids), "claim_summary": ledger["summary"],
                             "coverage": {"answer_complete": complete,
                                          "selected_span_count": len(ledger["spans"]),
                                          "available_span_count": ledger["available_span_count"]},
                             "reasons": ["All answer units have validated source references."] if supported else [],
                             "penalties": [] if supported else verification["missing_evidence"],
                             "dimensions": {"claim_support": ledger["summary"].get("support_ratio", 0),
                                            "audit_coverage": ledger["summary"].get("audit_coverage", 0)}}}
        result["timeline_events"] = []
        if mode == "timeline":
            if supported:
                ledger["candidate_text"] = candidate
            events, receipt = project_timeline(public_answer, ledger, result["finalization"], self.date_order)
            result["timeline_events"] = events
            result["finalization"]["timeline"] = receipt
        return result



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
