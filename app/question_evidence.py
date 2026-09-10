"""Request-local original reading for the inactive question-to-answer pipeline.

The immutable serialized snapshot prevents one request, candidate or mutable caller
from changing another request's original evidence or prior reading interpretations.
"""
from dataclasses import dataclass
from datetime import date
import hashlib
import json

from app import source_reading
from app.answer_finalization import evidence_spans
from app.query_metrics import CURRENT_QUERY_METRICS

PIPELINE_VERSION = 'question-evidence-v7'
CONVERSATION_CONTEXT_MAX_CHARS = 12_000

class QuestionEvidenceError(ValueError):
    """Content-free planning/snapshot failures; source text is never an error."""


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def validate_requirements(value):
    if not isinstance(value, dict) or set(value) != {'resolved_question', 'requirements'}:
        raise QuestionEvidenceError('invalid_requirements')
    rows = value['requirements']
    if (not isinstance(value['resolved_question'], str) or not value['resolved_question'].strip()
            or not isinstance(rows, list) or not rows):
        raise QuestionEvidenceError('invalid_requirements')
    ids = set()
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {'id', 'aspect', 'temporal_scope', 'comparison_scope'}
                or not isinstance(row['id'], str) or not row['id'].strip() or row['id'] in ids
                or not isinstance(row['aspect'], str) or not row['aspect'].strip()
                or not isinstance(row['temporal_scope'], str)
                or row['temporal_scope'] not in {'none', 'historical', 'current', 'unknown'}
                or not isinstance(row['comparison_scope'], str)
                or row['comparison_scope'] not in {'none', 'retrieved_documents', 'unknown'}):
            raise QuestionEvidenceError('invalid_requirements')
        ids.add(row['id'])
    return json.loads(canonical_json(value))


@dataclass(frozen=True)
class QuestionEvidence:
    """An immutable question/requirements/originals/reading snapshot, never truth."""
    _snapshot: str
    _reading: str

    @classmethod
    async def prepare(cls, orchestrator, question, requirements, evidence_pack, *,
                      evaluated_at, source_date_order='mdy', conversation_context=''):
        if not isinstance(question, str) or not question.strip():
            raise QuestionEvidenceError('invalid_question')
        try:
            if date.fromisoformat(evaluated_at).isoformat() != evaluated_at:
                raise ValueError()
        except (TypeError, ValueError):
            raise QuestionEvidenceError('invalid_evaluated_at') from None
        if not isinstance(source_date_order, str) or source_date_order not in {'mdy', 'dmy', 'reject_ambiguous'}:
            raise QuestionEvidenceError('invalid_date_order')
        if not isinstance(conversation_context, str) or len(conversation_context) > CONVERSATION_CONTEXT_MAX_CHARS:
            raise QuestionEvidenceError('invalid_conversation_context')
        requested = validate_requirements(requirements)
        acquisition = evidence_pack.get('_acquisition')
        if acquisition is not None:
            from app.source_acquisition import validate_bundle
            validate_bundle(evidence_pack, acquisition['receipt'], acquisition['inventory_digest'], acquisition['request'])
            original_request = acquisition['request']
            if any(original_request[k] != v for k, v in {
                    'question': question, 'resolved_question': requested['resolved_question'],
                    'conversation_context': conversation_context, 'evaluated_at': evaluated_at,
                    'source_date_order': source_date_order}.items()):
                raise QuestionEvidenceError('acquisition_request_mismatch')
        spans = [s for s in evidence_spans(evidence_pack, citation_safe=True) if not s.get('feedback_open')]
        documents = source_reading.group_sources(spans)
        metrics = CURRENT_QUERY_METRICS.get()
        if metrics is not None:
            metrics.reader_documents = len(documents)
        snapshot = canonical_json({'pipeline_version': PIPELINE_VERSION,
                                   'question': question, **requested, 'evaluated_at': evaluated_at,
                                   'source_date_order': source_date_order, 'source_documents': documents,
                                   'conversation_context': conversation_context,
                                   **({'acquisition_inventory_digest': acquisition['inventory_digest']} if acquisition else {})})
        # The reader gets copies, before there is a candidate to anchor its reading.
        payload = json.loads(snapshot)
        reading = await orchestrator.read_question_sources(payload)
        validated = source_reading.parse_reading(canonical_json(reading), documents)
        return cls(snapshot, canonical_json(validated))

    @property
    def digest(self):
        return hashlib.sha256(self._snapshot.encode()).hexdigest()

    @property
    def composition_input(self):
        return {**json.loads(self._snapshot), 'source_reading': json.loads(self._reading)}

    def audit_payload(self, payload):
        """Only an exact source/question/date match can reuse the prior reading."""
        snapshot = json.loads(self._snapshot)
        documents = source_reading.group_sources(payload['source_spans'])
        if (any(payload.get(key) != snapshot[key] for key in ('question', 'evaluated_at', 'source_date_order'))
                or any(key in payload and payload[key] != snapshot[key]
                       for key in ('requirements', 'resolved_question', 'conversation_context'))
                or documents != snapshot['source_documents']):
            raise QuestionEvidenceError('evidence_snapshot_mismatch')
        return {**{k: v for k, v in payload.items() if k != 'source_spans'},
                'resolved_question': snapshot['resolved_question'],
                'requirements': snapshot['requirements'], 'source_documents': documents,
                'conversation_context': snapshot['conversation_context'],
                'source_reading': json.loads(self._reading)}

    def auditor(self, orchestrator):
        return PreparedQuestionAuditor(orchestrator, self)


@dataclass(frozen=True)
class PreparedQuestionAuditor:
    orchestrator: object
    evidence: QuestionEvidence

    async def audit_answer_units(self, question, units, spans, plan):
        return await self.orchestrator.audit_answer_units(question, units, spans, plan,
                                                        prepared_evidence=self.evidence)


def planning_response_format():
    string = {'type': 'string'}
    properties = {
        'intent': string, 'domain': string, 'requires_current': {'type': 'boolean'},
        'needs_timeline': {'type': 'boolean'}, 'must_answer_current_vs_historical': {'type': 'boolean'},
        'required_doc_types': {'type': 'array', 'items': string},
        'subqueries': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
            'required': ['role', 'query'], 'properties': {'role': string, 'query': string}}},
        'reasoning': string, 'resolved_question': string,
        'requirements': {'type': 'array', 'minItems': 1, 'items': {
            'type': 'object', 'additionalProperties': False,
            'required': ['id', 'aspect', 'temporal_scope', 'comparison_scope'],
            'properties': {'id': string, 'aspect': string,
                'temporal_scope': {'type': 'string', 'enum': ['none', 'historical', 'current', 'unknown']},
                'comparison_scope': {'type': 'string', 'enum': ['none', 'retrieved_documents', 'unknown']}}}},
    }
    return {'type': 'json_schema', 'json_schema': {'name': 'question_requirements_plan', 'strict': True,
        'schema': {'type': 'object', 'additionalProperties': False, 'required': list(properties),
                   'properties': properties}}}


def coarse_requirements(question):
    return {'resolved_question': question, 'requirements': [{
        'id': 'r1', 'aspect': question, 'temporal_scope': 'unknown', 'comparison_scope': 'unknown'}]}
