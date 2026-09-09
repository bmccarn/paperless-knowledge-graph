"""Synthetic native decisions for protocol tests; no model-quality claim."""


def decision(unit_id='u1', status='supported', references=None, **fields):
    return {
        'unit_id': unit_id,
        'source_basis': 'The original record establishes the described observation.',
        'checks': {'subject': 'supported', 'predicate': 'supported' if status == 'supported' else 'not_established',
                   'record_role': 'supported', 'conditions': 'not_applicable',
                   'temporal': 'supported', 'comparison': 'not_applicable'},
        'unresolved_assumptions': [],
        'references': references if references is not None else [{'span_id': 'source-1'}],
        'temporal_scope': 'none', 'temporal_assertion': 'none',
        'comparison_scope': None, 'comparison_document_ids': [],
        'status': status,
        **fields,
    }
