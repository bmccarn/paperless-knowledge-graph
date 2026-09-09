"""Inactive original-led composition, source audit and final coverage pipeline."""
import hashlib

from app.answer_finalization import AnswerFinalizer
from app.config import settings
from app.question_evidence import QuestionEvidence, PIPELINE_VERSION
from app.answer_coverage import unavailable_coverage
from app.answer_completion import recover_coverage, PriorSupportRevoked


async def finalize_question(orchestrator, question, evidence_pack, plan, mode):
    stage = 'source_reader'
    try:
        evidence = await QuestionEvidence.prepare(orchestrator, question,
            {key: plan[key] for key in ('resolved_question', 'requirements')}, evidence_pack,
            evaluated_at=plan['evaluated_at'], source_date_order=settings.source_date_order)
        stage = 'answer_composer'
        composition = await orchestrator.compose_question_answer(evidence)
        stage = 'source_audit'
        final = await AnswerFinalizer(evidence.auditor(orchestrator), orchestrator,
            timeout_seconds=settings.answer_audit_timeout_seconds,
            concurrency=settings.strands_max_concurrent_calls,
            date_order=settings.source_date_order).finalize(
                question, composition.candidate, evidence_pack, plan=plan,
                mode=mode, evaluated_at=plan['evaluated_at'])
        final['finalization']['request_identity_digest'] = plan['request_identity_digest']
        stage = 'answer_coverage'
        try:
            coverage = await orchestrator.assess_question_coverage(evidence, final,
                planning_status=plan['requirements_status'])
        except Exception:
            coverage = unavailable_coverage(evidence, final, planning_status=plan['requirements_status'])
        stage = 'answer_completion'
        final, coverage = await recover_coverage(orchestrator, evidence, composition, final,
                                                coverage, evidence_pack, plan, mode)
        final['finalization'].update(pipeline_version=PIPELINE_VERSION,
            evidence_snapshot_digest=evidence.digest, question_coverage=coverage)
        final['evidence']['coverage']['requested_aspects_complete'] = coverage['complete']
        return final
    except Exception as exc:
        # A failed new stage cannot publish the old draft or force another model
        # attempt. Cancellation (BaseException) remains owned by the caller.
        final = await AnswerFinalizer(None).finalize(question, '', evidence_pack,
            mode='timeline' if mode == 'timeline' else 'strict', evaluated_at=plan['evaluated_at'])
        messages = {'source_reader': 'The original-source reading could not complete. Please retry the query.',
                    'answer_composer': 'The source-based answer could not be composed. Please retry the query.',
                    'source_audit': 'The source audit could not complete. Please retry the query.',
                    'answer_coverage': 'The answer coverage check could not complete. Please retry the query.',
                    'answer_completion': 'The completion audit could not validate the earlier answer. Please review the source documents.'}
        final['answer'] = messages[stage]
        final['finalization'].update(pipeline_version=PIPELINE_VERSION, pipeline_failure=stage,
            answer_digest=hashlib.sha256(final['answer'].encode()).hexdigest())
        if isinstance(exc, PriorSupportRevoked):
            final['finalization']['coverage_recovery'] = exc.diagnostic
        final['verification']['missing_evidence'] = [messages[stage]]
        return final
