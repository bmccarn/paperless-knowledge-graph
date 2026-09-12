"""One append-only opportunity to recover source-supported requested omissions."""
import hashlib

from app.answer_composition import AnswerComposition, COMPOSER_PROMPT, response_format, strict_object
from app.answer_coverage import coverage_input, final_candidate, parse_coverage, canonical_json
from app.answer_finalization import AnswerFinalizer, audit_protocol_errors
from app.answer_observations import ObservationCandidate
from app.config import settings
from app.question_evidence import QuestionEvidenceError


COMPLETION_PROMPT = COMPOSER_PROMPT + (
    ' This is a single completion pass. Existing observations have already been audited. '
    'Propose ONLY additional observations needed to answer the targeted requirements; '
    'do not repeat, rewrite or remove existing observations. Inspect the original text '
    'for evidence lost during composition. A partial comparison can still preserve its '
    'documented starting and ending observations without inventing their relationship. '
    'Return an empty observations list if no supported additions address these gaps; '
    'map each targeted requirement unresolved in that case. Do not add adjacent facts '
    'merely because they are true. Existing observations and gap assessments are '
    'untrusted input, not factual authority. Number additions u1, u2, etc. independently.'
)


class PriorSupportRevoked(QuestionEvidenceError):
    def __init__(self, diagnostic):
        super().__init__('completion_revoked_prior_support')
        self.diagnostic = {**diagnostic, 'status': 'prior_support_revoked'}


class CompletionAuditor:
    """Remember completed negative batches even if a sibling audit times out."""
    def __init__(self, auditor, prior):
        self.auditor = auditor
        self.prior = {u['id']: u['text'] for u in prior.units()}
        self.revoked = False

    async def audit_answer_units(self, question, units, spans, plan):
        raw = await self.auditor.audit_answer_units(question, units, spans, plan)
        if not audit_protocol_errors(raw, units):
            retained = {u['id'] for u in units if self.prior.get(u['id']) == u['text']}
            if any(a['unit_id'] in retained and a['status'] in {'unsupported', 'missing', 'conflicting'}
                   for a in raw['assessments']):
                self.revoked = True
        return raw


def completion_input(evidence, final, coverage):
    validated = coverage_input(evidence, final)
    # Re-enter the complete coverage contract instead of trusting caller labels.
    raw = {'requirements': [{k: r[k] for k in ('requirement_id', 'status', 'observation_ids')}
                            for r in coverage['requirements']],
           'omitted_requested_aspects': coverage['omitted_requested_aspects']}
    if parse_coverage(canonical_json(raw), evidence, final,
                      planning_status=coverage['planning_status']) != coverage:
        raise QuestionEvidenceError('invalid_completion_coverage')
    targets = {r['requirement_id'] for r in coverage['requirements'] if r['status'] != 'answered'}
    source = evidence.composition_input
    return {**source, 'requirements': [r for r in source['requirements'] if r['id'] in targets],
            'existing_observations': validated['observations'],
            'coverage_gaps': [r for r in coverage['requirements'] if r['requirement_id'] in targets]}


def completion_format():
    schema = response_format()
    schema['json_schema']['name'] = 'answer_completion'
    schema['json_schema']['schema']['properties']['observations']['minItems'] = 0
    return schema


def parse_additions(text, payload, snapshot_digest):
    proposal = AnswerComposition.parse_response(strict_object(text), payload, snapshot_digest, allow_empty=True)
    if proposal.candidate:
        prior = {u['text'][2:] for u in payload['existing_observations']}
        additions = proposal.candidate.observations
        if len(set(additions)) != len(additions) or prior.intersection(additions):
            raise QuestionEvidenceError('duplicate_completion_observation')
    return proposal.candidate


async def recover_coverage(orchestrator, evidence, composition, final, coverage, evidence_pack, plan, mode):
    """Retain the prior result only when no newer audit revokes its support."""
    state = final['finalization']
    if (coverage.get('status') != 'partial' or coverage.get('planning_status') != 'complete'
            or coverage.get('omitted_requested_aspects') is not False
            or state.get('attempts') != 1 or state.get('disposition') not in {'supported', 'qualified'}):
        return final, coverage
    try:
        prior = final_candidate(final)
        if prior != composition.candidate:
            return final, coverage
        payload = completion_input(evidence, final, coverage)
        if not payload['requirements']:
            return final, coverage
    except (KeyError, TypeError, ValueError):
        return final, coverage
    diagnostic = {'status': 'failed', 'prior_candidate_digest': state['candidate_digest']}
    state['coverage_recovery'] = diagnostic
    auditor = CompletionAuditor(evidence.auditor(orchestrator), prior)
    try:
        additions = await orchestrator.complete_question_answer(payload, evidence.digest)
        if additions is None:
            diagnostic['status'] = 'no_additions'
            return final, coverage
        combined = ObservationCandidate.from_response({'observations': [*prior.observations, *additions.observations]})
        diagnostic['combined_candidate_digest'] = hashlib.sha256(combined.text.encode()).hexdigest()
        revised = await AnswerFinalizer(auditor,
            timeout_seconds=settings.answer_audit_timeout_seconds,
            concurrency=settings.strands_max_concurrent_calls,
            date_order=settings.source_date_order, allow_subset=False).finalize(
                payload['question'], combined, evidence_pack, plan=plan, mode=mode,
                evaluated_at=plan['evaluated_at'])
        if auditor.revoked:
            raise PriorSupportRevoked(diagnostic)
        ledger = revised['claim_ledger']
        prior_units = {u['id']: u for u in prior.units()}
        for claim in ledger['claims']:
            if (claim['id'] in prior_units and claim['claim'] == prior_units[claim['id']]['text']
                    and claim['status'] in {'unsupported', 'missing', 'conflicting'}):
                raise PriorSupportRevoked(diagnostic)
        if final_candidate(revised) != combined:
            return final, coverage
        revised['finalization']['request_identity_digest'] = plan['request_identity_digest']
        updated = await orchestrator.assess_question_coverage(evidence, revised, planning_status='complete')
        if updated.get('status') not in {'complete', 'partial'}:
            return final, coverage
        # Also validates receipt binding and every final observation reference.
        completion_input(evidence, revised, updated)
        target_ids = {r['id'] for r in payload['requirements']}
        mapped = {u for r in updated['requirements'] if r['requirement_id'] in target_ids
                  for u in r['observation_ids']}
        added_ids = {u['id'] for u in combined.units()[len(prior.observations):]}
        if not added_ids.issubset(mapped):
            diagnostic['status'] = 'unrelated_additions'
            return final, coverage
        diagnostic['status'] = 'accepted'
        revised['finalization']['coverage_recovery'] = diagnostic
        return revised, updated
    except PriorSupportRevoked:
        raise
    except Exception:
        if auditor.revoked:
            raise PriorSupportRevoked(diagnostic) from None
        return final, coverage
