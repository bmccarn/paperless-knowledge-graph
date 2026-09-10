"""Inactive original-led composition, source audit and final coverage pipeline."""
import hashlib

from app.answer_finalization import AnswerFinalizer
from app.config import settings
from app.question_evidence import QuestionEvidence, PIPELINE_VERSION
from app.answer_coverage import unavailable_coverage, augment_fact_coverage
from app.answer_fact_selection import prepare_facts
from app.answer_completion import recover_coverage, PriorSupportRevoked


async def _finalize_question(orchestrator, question, evidence_pack, plan, mode):
    stage = 'source_reader'
    try:
        evidence = await QuestionEvidence.prepare(orchestrator, question,
            {key: plan[key] for key in ('resolved_question', 'requirements')}, evidence_pack,
            evaluated_at=plan['evaluated_at'], source_date_order=settings.source_date_order,
            conversation_context=plan.get('conversation_context', ''))
        stage = 'fact_inventory'
        composition = prepare_facts(evidence)
        inventory_digest = composition.inventory_digest
        candidate = composition.candidate
        if candidate is None:
            # A valid source reading can contain no observations. Never treat
            # an empty inventory as factual success or promote its limitations.
            final = await AnswerFinalizer(None).finalize(question, '', evidence_pack,
                plan=plan, mode=mode, evaluated_at=plan['evaluated_at'])
            final['answer'] = 'No source observations could be retained for a verified answer.'
            final['finalization'].update(pipeline_version=PIPELINE_VERSION,
                pipeline_failure='empty_fact_inventory', request_identity_digest=plan['request_identity_digest'],
                evidence_snapshot_digest=evidence.digest,
                reader_inventory_digest=inventory_digest,
                disposition='incomplete', complete=False, answer_verified=False,
                answer_digest=hashlib.sha256(final['answer'].encode()).hexdigest())
            # Empty Quick strings normally take the legacy unaudited path. This
            # explicit pipeline failure has no facts and must never inherit its score.
            final['verification']['status'] = 'incomplete'
            final['verification']['missing_evidence'] = [final['answer']]
            final['evidence'].update(score=0.0, level='low', audit_status='incomplete',
                                     penalties=[final['answer']])
            if mode == 'timeline':
                from app.timeline import project_timeline
                final['timeline_events'], final['finalization']['timeline'] = project_timeline(
                    final['answer'], final['claim_ledger'], final['finalization'], settings.source_date_order)
            conservation = composition.bind_final(evidence, final)
            final['finalization']['fact_conservation'] = conservation
            final['finalization']['question_coverage'] = augment_fact_coverage(
                unavailable_coverage(evidence, final, planning_status=plan['requirements_status']), conservation)
            final['evidence']['coverage']['requested_aspects_complete'] = False
            return final
        stage = 'source_audit'
        final = await AnswerFinalizer(evidence.auditor(orchestrator), orchestrator,
            timeout_seconds=settings.answer_audit_timeout_seconds,
            concurrency=settings.strands_max_concurrent_calls,
            date_order=settings.source_date_order).finalize(
                question, candidate, evidence_pack, plan=plan,
                mode=mode, evaluated_at=plan['evaluated_at'])
        final['finalization']['request_identity_digest'] = plan['request_identity_digest']
        final['finalization']['reader_inventory_digest'] = inventory_digest
        stage = 'answer_coverage'
        try:
            coverage = await orchestrator.assess_question_coverage(evidence, final,
                planning_status=plan['requirements_status'])
        except Exception:
            coverage = unavailable_coverage(evidence, final, planning_status=plan['requirements_status'])
        stage = 'answer_completion'
        final, coverage = await recover_coverage(orchestrator, evidence, composition, final,
                                                coverage, evidence_pack, plan, mode)
        # Completion/editor produce new final receipts; the original inventory
        # identity remains anchored independently of their surviving subset.
        final['finalization']['reader_inventory_digest'] = inventory_digest
        conservation = composition.bind_final(evidence, final)
        final['finalization']['fact_conservation'] = conservation
        coverage = augment_fact_coverage(coverage, conservation)
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
                    'fact_inventory': 'The source observations could not form a verified answer. Please review the source documents.',
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


async def finalize_question(orchestrator, question, evidence_pack, plan, mode):
    acquisition = evidence_pack.get('_acquisition')
    if plan.get('acquisition_required') and acquisition is None:
        raise ValueError('missing_required_acquisition')
    if acquisition is not None:
        from app.source_acquisition import validate_bundle, acquisition_digest
        validate_bundle(evidence_pack, acquisition['receipt'], acquisition['inventory_digest'], acquisition['request'])
        if (plan.get('acquisition_request_digest') != acquisition_digest(acquisition['request'])
                or acquisition['request']['mode'] != mode):
            raise ValueError('acquisition_plan_mismatch')
    final = await _finalize_question(orchestrator, question, evidence_pack, plan, mode)
    if acquisition is not None:
        from app.source_acquisition import augment_acquisition_coverage
        final['finalization']['acquisition_inventory_digest'] = acquisition['inventory_digest']
        final['finalization']['source_acquisition'] = acquisition['receipt']
        coverage = final['finalization'].get('question_coverage')
        if coverage is not None:
            coverage = augment_acquisition_coverage(coverage, acquisition['receipt'])
            final['finalization']['question_coverage'] = coverage
            final['evidence']['coverage']['requested_aspects_complete'] = coverage['complete']
    return final
