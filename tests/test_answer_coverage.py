"""Coverage never substitutes for factual support or an exact final candidate."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_coverage import parse_coverage, coverage_input
from app.answer_finalization import AnswerFinalizer
from app.answer_observations import ObservationCandidate
from app.question_evidence import QuestionEvidence, QuestionEvidenceError
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.test_observation_delivery import HandleAuditor
from tests.test_question_evidence import REQUIREMENTS
from tests import test_source_reading as reading_fixtures


class AnswerCoverageTests(unittest.IsolatedAsyncioTestCase):
    setUp = reading_fixtures.SourceReadingTests.setUp

    async def prepared(self, *, partial=False):
        class Reader:
            async def read_question_sources(inner, payload): return self.reading
        evidence = await QuestionEvidence.prepare(Reader(), 'What do the records establish?',
            REQUIREMENTS, self.pack, evaluated_at='2026-09-09')
        candidate = ObservationCandidate(tuple((['REJECT.'] if partial else []) + ['Vendor completed a refund of $125.']))
        final = await AnswerFinalizer(HandleAuditor()).finalize(
            'What do the records establish?', candidate, self.pack, evaluated_at='2026-09-09')
        self.assertTrue(final['finalization']['answer_verified'])
        return evidence, final

    def response(self):
        return {'requirements': [{'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']}],
                'omitted_requested_aspects': False}

    async def test_coverage_binds_final_subset_ids_and_checks_omitted_question_aspects(self):
        evidence, final = await self.prepared(partial=True)
        self.assertEqual(final['finalization']['disposition'], 'partial')
        payload = coverage_input(evidence, final)
        self.assertEqual([u['id'] for u in payload['observations']], ['u1'])
        self.assertNotIn('REJECT', json.dumps(payload))
        for omitted, planning, complete in ((False, 'complete', True), (True, 'complete', False),
                                            (False, 'coarse', False), (False, 'unavailable', False)):
            raw = self.response(); raw['omitted_requested_aspects'] = omitted
            receipt = parse_coverage(json.dumps(raw), evidence, final, planning_status=planning)
            self.assertEqual(receipt['complete'], complete)
            self.assertEqual(receipt['binding']['candidate_digest'], final['finalization']['candidate_digest'])
            self.assertEqual(receipt['binding']['answer_digest'], final['finalization']['answer_digest'])
            self.assertEqual(receipt['binding']['snapshot_digest'], evidence.digest)

    async def test_unknown_ids_missing_aspects_and_freeform_facts_are_rejected(self):
        evidence, final = await self.prepared()
        for change in (lambda x: x['requirements'].clear(),
                       lambda x: x['requirements'][0].update(observation_ids=['u2']),
                       lambda x: x['requirements'][0].update(requirement_id='r2'),
                       lambda x: x['requirements'][0].update(status='unresolved'),
                       lambda x: x['requirements'][0].update(explanation='A new factual assertion.'),
                       lambda x: x.update(omitted_requested_aspects='false')):
            raw = self.response(); change(raw)
            with self.assertRaises(QuestionEvidenceError): parse_coverage(json.dumps(raw), evidence, final)

    async def test_tampered_candidate_never_reaches_coverage_model(self):
        evidence, final = await self.prepared()
        for change in (lambda x: x['claim_ledger']['claims'][0].update(id='u2'),
                       lambda x: x['claim_ledger']['claims'][0].update(claim='- A changed fact.'),
                       lambda x: x['finalization'].update(candidate_digest='wrong'),
                       lambda x: x['claim_ledger']['spans'][0].update(content_digest='wrong'),
                       lambda x: x['finalization'].update(evaluated_at='2026-09-10'),
                       lambda x: x.update(answer='Changed delivered answer'),
                       lambda x: x['finalization'].update(answer_verified=False)):
            changed = copy.deepcopy(final); change(changed)
            with self.assertRaises(QuestionEvidenceError): coverage_input(evidence, changed)

    async def test_failure_and_disabled_configuration_preserve_facts_without_false_complete(self):
        evidence, final = await self.prepared()
        before = copy.deepcopy(final)
        for enabled, failure in ((False, None), (True, TimeoutError()), (True, RuntimeError('private provider text')),
                                 (True, 'malformed')):
            auditor = StrandsQueryOrchestrator(); auditor.enabled = enabled
            calls = []
            async def model(**kwargs):
                calls.append(kwargs)
                if isinstance(failure, Exception): raise failure
                return failure
            with patch.object(auditor, '_text_agent', side_effect=model):
                receipt = await auditor.assess_question_coverage(evidence, final)
            self.assertEqual(len(calls), int(enabled))
            self.assertEqual(receipt['status'], 'unavailable')
            self.assertFalse(receipt['complete'])
            self.assertNotIn('private provider text', json.dumps(receipt))
            self.assertEqual(final, before)

    async def test_cancellation_is_not_downgraded_to_coverage_failure(self):
        evidence, final = await self.prepared()
        auditor = StrandsQueryOrchestrator(); auditor.enabled = True
        started = asyncio.Event()
        async def model(**kwargs): started.set(); await asyncio.Event().wait()
        with patch.object(auditor, '_text_agent', side_effect=model):
            task = asyncio.create_task(auditor.assess_question_coverage(evidence, final))
            await asyncio.wait_for(started.wait(), 1); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
