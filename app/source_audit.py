"""Native source-audit decision protocol, separate from factual source validation.

The model's explanation is transient editor input. Neither it nor these checks
certifies a fact; original references and the finalizer's other gates still apply.
"""
import json


FACETS = ('subject', 'predicate', 'record_role', 'conditions', 'temporal', 'comparison')
CORE_FACETS = frozenset(FACETS[:3])
CHECK_STATUSES = ('supported', 'not_established', 'contradicted', 'not_applicable')
VERDICTS = ('supported', 'unsupported', 'missing', 'conflicting')
SCOPES = ('historical', 'documented', 'current', 'none')
ASSERTIONS = ('source_observation', 'retrieved_comparison', 'present_world', 'none')
MAX_BASIS_CHARS = 1200
MAX_ASSUMPTIONS = 6
MAX_ASSUMPTION_CHARS = 240
PROTOCOL_ERRORS = frozenset({
    'invalid_json', 'duplicate_key', 'invalid_object', 'invalid_assessments',
    'invalid_assessment', 'invalid_unit_ids', 'invalid_source_basis',
    'invalid_checks', 'invalid_assumptions', 'invalid_references',
    'invalid_temporal_metadata', 'invalid_status', 'unknown_source_handle', 'inconsistent_scope_checks',
})


class SourceAuditProtocolError(ValueError):
    def __init__(self, reason):
        self.reason = reason if reason in PROTOCOL_ERRORS else 'invalid_assessment'
        super().__init__(self.reason)


def response_format(unit_ids):
    """A fresh schema belongs to one native audit request, including corrections."""
    properties = {
        'unit_id': {'type': 'string', 'enum': list(unit_ids)},
        'source_basis': {'type': 'string', 'minLength': 1, 'maxLength': MAX_BASIS_CHARS,
                         'description': 'Briefly state what the original sources establish about this assertion, including relevant field roles, selected options and action stage. Do not defend the candidate or supply new evidence.'},
        'checks': {'type': 'object', 'additionalProperties': False, 'required': list(FACETS),
                   'properties': {facet: {'type': 'string', 'enum': list(CHECK_STATUSES[:-1] if facet in CORE_FACETS else CHECK_STATUSES)}
                                  for facet in FACETS}},
        'unresolved_assumptions': {'type': 'array', 'maxItems': MAX_ASSUMPTIONS,
                                   'items': {'type': 'string', 'minLength': 1, 'maxLength': MAX_ASSUMPTION_CHARS},
                                   'description': 'Any inference required to make the assertion true that the source does not establish. Empty only if none.'},
        'references': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
                       'required': ['span_id'], 'properties': {'span_id': {'type': 'string'}}}},
        'temporal_scope': {'type': 'string', 'enum': list(SCOPES)},
        'temporal_assertion': {'type': 'string', 'enum': list(ASSERTIONS)},
        'comparison_scope': {'type': ['string', 'null'], 'enum': ['retrieved_documents', None]},
        'comparison_document_ids': {'type': 'array', 'items': {'type': 'integer'}},
        'status': {'type': 'string', 'enum': list(VERDICTS)},
    }
    return {'type': 'json_schema', 'json_schema': {
        'name': 'source_audit_decisions', 'strict': True,
        'schema': {'type': 'object', 'additionalProperties': False, 'required': ['assessments'],
                   'properties': {'assessments': {'type': 'array', 'minItems': len(unit_ids),
                                  'maxItems': len(unit_ids), 'items': {'type': 'object',
                                  'additionalProperties': False, 'required': list(properties),
                                  'properties': properties}}}},
    }}


def _require(condition, reason):
    if not condition:
        raise SourceAuditProtocolError(reason)


def _bounded_text(value, limit):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= limit


def parse_decisions(text, unit_ids, *, allowed_span_ids=None):
    """Reject malformed protocols; downgrade semantic inconsistency without retry."""
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, 'duplicate_key')
            result[key] = value
        return result

    try:
        raw = json.loads(text, object_pairs_hook=unique_object)
    except SourceAuditProtocolError:
        raise
    except (ValueError, TypeError, RecursionError):
        raise SourceAuditProtocolError('invalid_json') from None
    _require(isinstance(raw, dict) and set(raw) == {'assessments'}, 'invalid_object')
    rows = raw['assessments']
    _require(isinstance(rows, list), 'invalid_assessments')
    expected_keys = set(response_format(unit_ids)['json_schema']['schema']['properties']['assessments']['items']['required'])
    normalized, seen = [], []
    for row in rows:
        _require(isinstance(row, dict) and set(row) == expected_keys, 'invalid_assessment')
        unit_id = row['unit_id']
        _require(isinstance(unit_id, str) and unit_id in unit_ids and unit_id not in seen, 'invalid_unit_ids')
        seen.append(unit_id)
        _require(_bounded_text(row['source_basis'], MAX_BASIS_CHARS), 'invalid_source_basis')
        checks = row['checks']
        _require(isinstance(checks, dict) and set(checks) == set(FACETS), 'invalid_checks')
        _require(all(isinstance(value, str) and value in CHECK_STATUSES
                     and not (facet in CORE_FACETS and value == 'not_applicable')
                     for facet, value in checks.items()), 'invalid_checks')
        assumptions = row['unresolved_assumptions']
        _require(isinstance(assumptions, list) and len(assumptions) <= MAX_ASSUMPTIONS
                 and all(_bounded_text(value, MAX_ASSUMPTION_CHARS) for value in assumptions), 'invalid_assumptions')
        refs = row['references']
        _require(isinstance(refs, list) and all(isinstance(ref, dict) and set(ref) == {'span_id'}
                 and isinstance(ref['span_id'], str) and bool(ref['span_id']) for ref in refs), 'invalid_references')
        if allowed_span_ids is not None:
            _require(all(ref['span_id'] in allowed_span_ids for ref in refs), 'unknown_source_handle')
        _require(isinstance(row['temporal_scope'], str) and row['temporal_scope'] in SCOPES
                 and isinstance(row['temporal_assertion'], str) and row['temporal_assertion'] in ASSERTIONS
                 and row['comparison_scope'] in ('retrieved_documents', None)
                 and isinstance(row['comparison_document_ids'], list)
                 and all(type(doc) is int for doc in row['comparison_document_ids']), 'invalid_temporal_metadata')
        _require(isinstance(row['status'], str) and row['status'] in VERDICTS, 'invalid_status')
        if (checks['comparison'] == 'not_applicable' and row['comparison_scope'] is None
                and row['temporal_scope'] == 'historical'
                and row['temporal_assertion'] == 'source_observation'):
            # Inert metadata cannot turn a historical observation into a comparison.
            row['comparison_document_ids'] = []
        reasons = ['semantic_' + facet for facet in FACETS
                   if checks[facet] in ('not_established', 'contradicted')]
        if assumptions:
            reasons.append('semantic_assumptions')
        if (checks['temporal'] == 'not_applicable'
                and (row['temporal_scope'] != 'none' or row['temporal_assertion'] != 'none')):
            reasons.append('semantic_temporal')
        if (checks['comparison'] == 'not_applicable'
                and (row['temporal_scope'] == 'documented' or row['temporal_assertion'] == 'retrieved_comparison'
                     or row['comparison_scope'] is not None or row['comparison_document_ids'])):
            reasons.append('semantic_comparison')
        normalized.append({
            **{key: row[key] for key in ('unit_id', 'references', 'temporal_scope', 'temporal_assertion',
                                        'comparison_scope', 'comparison_document_ids')},
            'status': 'unsupported' if row['status'] == 'supported' and reasons else row['status'],
            'model_status': row['status'],
            'semantic_decision': {'checks': dict(checks), 'rejection_reasons': reasons,
                                  'source_basis': row['source_basis'], 'unresolved_assumptions': list(assumptions)},
        })
    _require(set(seen) == set(unit_ids), 'invalid_unit_ids')
    return {'assessments': normalized}


def validate_scope_consistency(parsed):
    """Allow bounded metadata correction only when no semantic rejection can reroll."""
    rows = parsed['assessments']
    for row in rows:
        semantic = row['semantic_decision']
        if (row['model_status'] != 'supported' or semantic['unresolved_assumptions']
                or any(value in {'not_established', 'contradicted'} for value in semantic['checks'].values())):
            return
    for row in rows:
        semantic = row['semantic_decision']
        if any(semantic['checks'][facet] == 'not_applicable'
               and 'semantic_' + facet in semantic['rejection_reasons']
               for facet in ('temporal', 'comparison')):
            raise SourceAuditProtocolError('inconsistent_scope_checks')
