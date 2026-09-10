"""Model-free payload measurements; observed source bytes are not token limits."""
import json
from app.answer_finalization import evidence_spans
from app.source_reading import group_sources, READER_PROMPT, response_format


def measure_bundle(bundle, request, requirements):
    """Measure exact first document-local reader inputs and later-stage source floor.

    Reading output, candidate units and audit instructions are not available yet;
    their full payloads cannot be claimed measured from the source inventory alone.
    """
    pack = bundle.evidence_pack
    spans = evidence_spans(pack, citation_safe=True)
    documents = group_sources(spans) if spans else []
    base = {k: request[k] for k in ('question', 'resolved_question', 'evaluated_at', 'source_date_order')}
    base['requirements'] = requirements
    def size(value): return len(json.dumps(value, ensure_ascii=False).encode())
    readers = [size({'system_prompt': READER_PROMPT,
                     'prompt': json.dumps({**base, 'source_documents': [doc]}, ensure_ascii=False),
                     'response_format': response_format([doc])}) for doc in documents]
    return {'scope': 'document-local first reader input; source-only floor for later stages',
            'reader_documents': len(documents), 'reference_windows': len(spans),
            'largest_reader_serialized_bytes': max(readers, default=0),
            'total_reader_serialized_bytes': sum(readers),
            'whole_source_inventory_serialized_bytes': size(documents),
            'evidence_pack_serialized_bytes': size(pack),
            'full_composition_and_audit_payload_bytes': 'not_yet_established',
            'provider_context_capacity': 'not_established',
            'native_model_calls': 0, 'acquisition_complete': bundle.receipt['complete'],
            'transfer': bundle.receipt['measurements']}
