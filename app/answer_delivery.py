"""Deterministic presentation of an accepted candidate; adds no factual prose."""

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
