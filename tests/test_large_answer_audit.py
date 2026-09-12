"""Whole-candidate audit scheduling must survive large reader inventories."""
import asyncio
import json
import unittest
from unittest.mock import patch

from tests.test_observation_delivery import HandleAuditor, ObservationRepairer, pack
from tests import test_question_pipeline as pipeline_controls
from app.answer_finalization import AnswerFinalizer, canonical_candidate
from app.answer_observations import ObservationCandidate


TEXT = 'Cedar records a monthly charge of $20.'


def uncited_text(final):
    # Citation removal leaves a space where each generated link used to be.
    text = canonical_candidate(final['answer'], pack(TEXT))[0]
    return '\n'.join(line.rstrip() for line in text.splitlines())


class LargeAnswerAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_modes_audit_every_observation_above_former_ceiling(self):
        for count in (81, 155):
            for mode in ('quick', 'deep', 'timeline', 'strict'):
                with self.subTest(count=count, mode=mode):
                    candidate = ObservationCandidate((TEXT,) * count)
                    auditor = HandleAuditor()
                    final = await AnswerFinalizer(auditor).finalize(
                        'What charges are recorded?', candidate, pack(TEXT), mode=mode)
                    self.assertTrue(final['finalization']['answer_verified'])
                    self.assertEqual(uncited_text(final), candidate.text)
                    self.assertEqual(final['claim_ledger']['summary']['audited'], count)
                    self.assertEqual([unit for call in auditor.calls for unit in call[0]], candidate.units())
                    self.assertTrue(all(len(call[0]) <= 4 for call in auditor.calls))

    async def test_large_editor_candidate_is_fully_reaudited(self):
        candidate = ObservationCandidate((TEXT,) * 155)
        auditor = HandleAuditor()
        final = await AnswerFinalizer(auditor, ObservationRepairer(
            {'observations': list(candidate.observations)})).finalize(
                'What charges are recorded?', 'Cedar REJECT.', pack(TEXT))
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertEqual(uncited_text(final), candidate.text)
        self.assertEqual(final['claim_ledger']['summary']['audited'], 155)
        self.assertEqual(len(auditor.calls), 40)

    async def test_legacy_prose_preserves_whole_context(self):
        answer = '\n\n'.join([TEXT] * 81)
        auditor = HandleAuditor()
        final = await AnswerFinalizer(auditor).finalize('What charges are recorded?', answer, pack(TEXT))
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertEqual(final['claim_ledger']['summary']['audited'], 81)
        self.assertTrue(all(call[2]['answer_context'] == answer for call in auditor.calls))

    async def test_long_valid_candidate_keeps_its_single_repair_opportunity(self):
        text = 'Cedar records ' + 'documented charge ' * 60 + 'REJECT.'
        original = ObservationCandidate((text,) * 100)
        self.assertGreater(len(original.text), 96000)
        auditor = HandleAuditor()
        repairer = ObservationRepairer({'observations': [TEXT]})
        final = await AnswerFinalizer(auditor, repairer, allow_subset=False).finalize(
            'What charges are recorded?', original, pack(TEXT))
        self.assertEqual(repairer.calls, 1)
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertEqual(len(auditor.calls), 26)

    async def test_negative_after_old_ceiling_cannot_be_hidden(self):
        candidate = ObservationCandidate((TEXT,) * 80 + ('Cedar REJECT.',))
        auditor = HandleAuditor()
        final = await AnswerFinalizer(auditor, allow_subset=False).finalize(
            'What charges are recorded?', candidate, pack(TEXT))
        self.assertFalse(final['finalization']['answer_verified'])
        self.assertEqual(final['claim_ledger']['summary']['audited'], 81)
        self.assertEqual(final['claim_ledger']['summary']['unsupported'], 1)

    async def test_late_batch_failure_does_not_publish_a_passing_prefix(self):
        class FailingAuditor(HandleAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                result = await super().audit_answer_units(question, units, spans, plan)
                if any(unit['id'] == 'u81' for unit in units):
                    raise RuntimeError('controlled late failure')
                return result
        auditor = FailingAuditor()
        final = await AnswerFinalizer(auditor).finalize(
            'What charges are recorded?', ObservationCandidate((TEXT,) * 81), pack(TEXT))
        self.assertEqual(len(auditor.calls), 21)
        self.assertFalse(final['finalization']['answer_verified'])
        self.assertFalse(final['claim_ledger']['complete'])

    async def test_large_candidate_keeps_worker_bound(self):
        class ConcurrentAuditor(HandleAuditor):
            active = peak = 0
            async def audit_answer_units(self, *args):
                self.active += 1
                self.peak = max(self.peak, self.active)
                try:
                    await asyncio.sleep(0)
                    return await super().audit_answer_units(*args)
                finally:
                    self.active -= 1
        auditor = ConcurrentAuditor()
        final = await AnswerFinalizer(auditor, concurrency=3).finalize(
            'What charges are recorded?', ObservationCandidate((TEXT,) * 155), pack(TEXT))
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertEqual(auditor.peak, 3)
        self.assertEqual(auditor.active, 0)

    async def test_outer_cancellation_drains_large_audit_workers(self):
        entered = asyncio.Event()
        class WaitingAuditor(HandleAuditor):
            active = 0
            async def audit_answer_units(self, *args):
                self.active += 1
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    self.active -= 1
        auditor = WaitingAuditor()
        task = asyncio.create_task(AnswerFinalizer(auditor).finalize(
            'What charges are recorded?', ObservationCandidate((TEXT,) * 155), pack(TEXT)))
        try:
            await asyncio.wait_for(entered.wait(), .5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(auditor.active, 0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def test_deadline_scales_past_former_ceiling(self):
        finalizer = AnswerFinalizer(None, timeout_seconds=10, concurrency=4)
        candidate = ObservationCandidate((TEXT,) * 155)
        self.assertEqual(finalizer._audit_timeout(candidate.text, candidate), 100)


class LargeInventoryPipelineTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = pipeline_controls.QuestionPipelineTests.asyncSetUp
    asyncTearDown = pipeline_controls.QuestionPipelineTests.asyncTearDown
    model = pipeline_controls.QuestionPipelineTests.model

    async def test_reader_inventory_survives_large_audit_and_restored_coverage(self):
        original = self.model
        async def many_observations(**kwargs):
            result = await original(**kwargs)
            if kwargs['name'] == 'source_reader':
                data = json.loads(result)
                data['documents'][0]['observations'] *= 155
                return json.dumps(data)
            return result
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode), patch.object(self.orchestrator, '_text_agent', side_effect=many_observations):
                self.calls.clear()
                final = await self.engine.query('What monthly premium is recorded?', mode=mode)
                self.assertTrue(final['finalization']['answer_verified'])
                conservation = final['finalization']['fact_conservation']
                self.assertTrue(conservation['complete'])
                self.assertEqual(len(conservation['inventory']), 155)
                self.assertEqual(final['claim_ledger']['summary']['audited'], 155)
                self.assertEqual(self.calls.count('source_auditor'), 39)
