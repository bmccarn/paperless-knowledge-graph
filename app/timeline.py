"""Publish calendar mentions of the final accepted candidate, never new claims.

The input ledger is the finalizer's certified ledger, not a provider response.
Restoration checks its historical binding without re-certifying the live corpus.
"""
from copy import deepcopy
import hashlib
import json
import re

from app.answer_delivery import render_verified_answer
from app.source_dates import calendar_year_context, date_context, date_supported, source_dates

VERSION = 'verified-dates-v1'
_YEAR_RANGE = re.compile(r'(?<!\w)(\d{4})[\s*_`]*(?:[-–—]|to|through|until)[\s*_`]*(\d{4})(?!\w)', re.I)


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

    if (not isinstance(finalization, dict) or not isinstance(ledger, dict)
            or finalization.get('answer_verified') is not True
            or not isinstance(finalization.get('disposition'), str)
            or finalization['disposition'] not in {'supported', 'qualified', 'partial'}
            or finalization.get('complete') is not (finalization['disposition'] != 'partial')):
        return unavailable('answer_not_verified')
    try:
        candidate = ledger['candidate_text']
        claims = ledger['claims']
        if (not isinstance(candidate, str) or not candidate or not isinstance(claims, list) or not claims
                or ledger.get('complete') is not True
                or not isinstance(ledger.get('unitization'), str)
                or ledger['unitization'] not in {'observations_v1', 'prose_v1'}
                or ledger['summary']['total'] != len(claims)
                or ledger['summary']['audited'] != len(claims)
                or ledger['summary']['supported'] != len(claims)
                or _digest(candidate) != ledger.get('candidate_digest')
                or _digest(candidate) != finalization.get('candidate_digest')
                or not isinstance(answer, str) or _digest(answer) != finalization.get('answer_digest')):
            return unavailable('candidate_binding')
        ids, end = set(), 0
        for claim in claims:
            if (not isinstance(claim, dict) or not isinstance(claim.get('id'), str)
                    or claim['id'] in ids or claim.get('status') != 'supported'
                    or type(claim.get('start')) is not int or type(claim.get('end')) is not int
                    or not end <= claim['start'] < claim['end'] <= len(candidate)
                    or candidate[claim['start']:claim['end']] != claim.get('claim')
                    or not isinstance(claim.get('references'), list) or not claim['references']):
                return unavailable('claim_binding')
            ids.add(claim['id'])
            end = claim['end']
            for ref in claim['references']:
                if (not isinstance(ref, dict) or type(ref.get('document_id')) is not int or ref['document_id'] < 1
                        or not isinstance(ref.get('quote'), str) or not ref['quote']
                        or type(ref.get('start')) is not int or type(ref.get('end')) is not int
                        or not 0 <= ref['start'] < ref['end']
                        or len(ref['quote']) != ref['end'] - ref['start']
                        or not all(isinstance(ref.get(key), str) and ref[key]
                                   for key in ('span_id', 'evidence_id', 'content_digest'))):
                    return unavailable('reference_binding')
        if answer != render_verified_answer(candidate, claims,
                partial=finalization['disposition'] == 'partial',
                qualified=finalization.get('documented_qualification') is True,
                evaluated_at=finalization.get('evaluated_at')):
            return unavailable('answer_binding')
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
