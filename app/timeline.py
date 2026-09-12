"""Publish calendar mentions of the final accepted candidate, never new claims.

The input ledger is the finalizer's certified ledger, not a provider response.
Restoration checks its historical binding without re-certifying the live corpus.
"""
from copy import deepcopy
import hashlib
import json
import re

from app.answer_delivery import validate_verified_delivery
from app.source_dates import calendar_year_context, date_context, date_supported, source_dates

VERSION = 'verified-dates-v1'
_YEAR_RANGE = re.compile(r'(?<!\w)(\d{4})(?:[^\S\r\n]|[*_`])*'
                         r'(?:[-–—]|to|through|until)(?:[^\S\r\n]|[*_`])*'
                         r'([+−-]?(?:\d+(?:[.,]\d+)?|[.,]\d+))', re.I)


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _mentions(text, date_order, context_before=''):
    ranges = [match for match in _YEAR_RANGE.finditer(text)]
    for found in source_dates(text, date_order, context_before=context_before):
        if not found.value:
            continue
        if found.precision == 'year':
            start, end = found.start, found.end
            enclosing = next((match for match in ranges if found.start in {match.start(1), match.start(2)}), None)
            if enclosing:
                start, end = enclosing.start(), enclosing.end()
                if (not re.fullmatch(r'\d{4}', enclosing[2])
                        or (end < len(text) and (text[end].isalnum() or text[end] == '_'))):
                    continue  # A year-shaped prefix of a quantity is not a date.
            # Inspect the complete numeric range. A unit after its second
            # endpoint governs both values, not just the nearest endpoint.
            before, after = date_context(context_before + text[:start]), text[end:]
            before = re.sub(r'\b(?:is|was|are|were|from)\s+$', '', before, flags=re.I)
            # A bare year field can be a product attribute. Require stronger
            # calendar context rather than classifying product/domain names.
            before = re.sub(r'\byear\s*:?\s*$', '', before, flags=re.I)
            if not calendar_year_context(before, after):
                continue
        yield found


def project_timeline(answer, ledger, finalization, date_order='mdy'):
    """Return (cards, receipt) from one exact eligible final candidate.

    No-date is a successful projection. Malformed binding is local failure:
    callers retain the accepted prose but cannot cache a successful timeline.
    """
    def unavailable(reason):
        return [], {'version': VERSION, 'status': 'unavailable', 'reason': reason}

    try:
        candidate = validate_verified_delivery(answer, ledger, finalization)
    except (ValueError, TypeError) as exc:
        return unavailable(str(exc))
    try:
        claims = ledger['claims']
        events = []
        for claim in claims:
            seen = set()
            actual = [found for ref in claim['references'] for found in
                      _mentions(ref['quote'], date_order, ref.get('date_context_before', ''))]
            for found in _mentions(claim['claim'], date_order):
                if (found.value, found.precision) in seen:
                    continue
                if not any(date_supported(found, source) for source in actual):
                    # An accepted date that cannot retain its source authority
                    # makes the projection unavailable, never silently complete.
                    return unavailable('source_date_binding')
                seen.add((found.value, found.precision))
                events.append({'date': found.value, 'precision': found.precision,
                               'date_text': found.text, 'date_start': claim['start'] + found.start,
                               'date_end': claim['start'] + found.end,
                               'claim_id': claim['id'], 'claim_start': claim['start'], 'claim_end': claim['end'],
                               'claim': claim['claim'], 'candidate_digest': ledger['candidate_digest'],
                               'references': deepcopy(claim['references']), 'status': 'source_supported',
                               'presentation': 'observation' if ledger['unitization'] == 'observations_v1' else 'answer_context'})
        events.sort(key=lambda event: (event['date'], event['claim_start'], event['date_start']))
        binding = {key: ledger[key] for key in ('candidate_text', 'candidate_digest', 'claims', 'complete', 'summary', 'unitization')}
        receipt = {'version': VERSION, 'date_order': date_order,
                   'status': 'ready' if events else 'no_dates', 'event_count': len(events),
                   'candidate_digest': ledger['candidate_digest'],
                   'ledger_digest': _digest(json.dumps(binding, sort_keys=True, ensure_ascii=False))}
        return events, receipt
    except (KeyError, TypeError, ValueError):
        return unavailable('invalid_projection')


def restore_timeline(result):
    """Validate the persisted projection; never upgrade legacy event prose."""
    unavailable = ([], {'version': VERSION, 'status': 'unavailable', 'reason': 'stored_binding'})
    if not isinstance(result, dict):
        return unavailable
    finalization = result.get('finalization')
    receipt = finalization.get('timeline') if isinstance(finalization, dict) else None
    if not isinstance(receipt, dict) or receipt.get('version') != VERSION:
        return unavailable
    events, expected = project_timeline(result.get('answer'), result.get('claim_ledger'), finalization, receipt.get('date_order'))
    if expected['status'] not in {'ready', 'no_dates'} or expected != receipt or events != result.get('timeline_events'):
        return unavailable
    return events, expected
