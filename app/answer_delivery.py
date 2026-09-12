"""Deterministic presentation and exact binding of an accepted candidate."""
import hashlib

def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()

def render_verified_answer(candidate, claims, *, partial=False, qualified=False, evaluated_at=None):
    answer = candidate
    for claim in reversed(claims):
        ids = dict.fromkeys(reference['document_id'] for reference in claim['references'])
        citations = ' ' + ' '.join(f'[Document {doc_id}](/documents/{doc_id})' for doc_id in ids)
        answer = answer[:claim['end']] + citations + answer[claim['end']:]
    if partial:
        answer += '\n\nPartial answer: some claims could not be verified and were omitted. This does not answer every part of your question.'
    if qualified:
        answer += f'\n\nThese are documented facts. Current status as of {evaluated_at} is not established by the retrieved evidence.'
    return answer


def validate_verified_delivery(answer, ledger, finalization, *, candidate=None):
    """Validate historical delivery binding without claiming a new source audit."""
    if (not isinstance(finalization, dict) or not isinstance(ledger, dict)
            or finalization.get('answer_verified') is not True
            or not isinstance(finalization.get('disposition'), str)
            or finalization['disposition'] not in {'supported', 'qualified', 'partial'}
            or finalization.get('complete') is not (finalization['disposition'] != 'partial')):
        raise ValueError('answer_not_verified')
    try:
        candidate = ledger['candidate_text'] if candidate is None else candidate
        claims = ledger['claims']
        if (not isinstance(candidate, str) or not candidate or not isinstance(claims, list) or not claims
                or ('candidate_text' in ledger and ledger['candidate_text'] != candidate)
                or ledger.get('complete') is not True
                or not isinstance(ledger.get('unitization'), str)
                or ledger['unitization'] not in {'observations_v1', 'prose_v1'}
                or ledger['summary']['total'] != len(claims)
                or ledger['summary']['audited'] != len(claims)
                or ledger['summary']['supported'] != len(claims)
                or _digest(candidate) != ledger.get('candidate_digest')
                or _digest(candidate) != finalization.get('candidate_digest')
                or not isinstance(answer, str) or _digest(answer) != finalization.get('answer_digest')):
            raise ValueError('candidate_binding')
        ids, end = set(), 0
        for claim in claims:
            if (not isinstance(claim, dict) or not isinstance(claim.get('id'), str)
                    or claim['id'] in ids or claim.get('status') != 'supported'
                    or type(claim.get('start')) is not int or type(claim.get('end')) is not int
                    or not end <= claim['start'] < claim['end'] <= len(candidate)
                    or candidate[claim['start']:claim['end']] != claim.get('claim')
                    or not isinstance(claim.get('references'), list) or not claim['references']):
                raise ValueError('claim_binding')
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
                    raise ValueError('reference_binding')
        if answer != render_verified_answer(candidate, claims,
                partial=finalization['disposition'] == 'partial',
                qualified=finalization.get('documented_qualification') is True,
                evaluated_at=finalization.get('evaluated_at')):
            raise ValueError('answer_binding')
        return candidate
    except (KeyError, TypeError):
        raise ValueError('invalid_projection') from None
