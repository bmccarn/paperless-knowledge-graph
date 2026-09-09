"""Content-free, request-scoped counts of native Strands invocations."""
from contextvars import ContextVar
from dataclasses import dataclass, field
from collections import Counter

CURRENT_QUERY_METRICS = ContextVar('question_pipeline_metrics', default=None)
STAGES = frozenset({'query_planner', 'source_reader', 'answer_composer', 'source_auditor',
                    'answer_editor', 'answer_coverage'})


@dataclass
class QueryMetrics:
    calls: Counter = field(default_factory=Counter)
    reader_documents: int = 0
    audit_batches: int = 0

    def report(self):
        # Optional stages absent from the run contribute zero. Readers and audit
        # batches each allow only their existing single protocol correction.
        ceiling = (int(self.calls['query_planner'] > 0) + 2 * self.reader_documents
                   + int(self.calls['answer_composer'] > 0) + 2 * self.audit_batches
                   + int(self.calls['answer_editor'] > 0) + int(self.calls['answer_coverage'] > 0))
        return {'native_stage_calls': dict(self.calls), 'native_call_count': sum(self.calls.values()),
                'native_call_ceiling': ceiling, 'reader_documents': self.reader_documents,
                'audit_batches': self.audit_batches,
                'scope': 'strands_pipeline_stages_only',
                'transport_attempts': 'not_established'}


def record_native_stage(name):
    metrics = CURRENT_QUERY_METRICS.get()
    if metrics is not None and name in STAGES:
        metrics.calls[name] += 1
