"""Recovery diagnostic contracts using synthetic originals and real SDK mock transport."""
import asyncio
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import evidence_spans
from app.source_reading import group_sources
from app.strands_orchestrator import StrandsQueryOrchestrator
from scripts import run_source_recovery as runner
from scripts.source_recovery_preflight import mock_sdk, synthetic_response


def payload_from(body):
    content = body['messages'][-1]['content']
    return json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))


class RecoveryRunnerTests(unittest.IsolatedAsyncioTestCase):
    def pair(self, count=1):
        text = 'Vendor requested a refund; completion is not recorded.'
        pack = {'items': [{'id': 'request', 'document_id': 1, 'chunk_index': 0,
            'title': 'Request', 'source_kind': 'ocr', 'content': text, 'source_content': text}]}
        documents = group_sources(evidence_spans(pack, citation_safe=True))
        request = {'question': 'What does the refund record establish?', 'evaluated_at': '2026-09-10',
                   'source_date_order': 'mdy', 'source_documents': documents}
        from app.question_evidence import coarse_requirements
        request.update(coarse_requirements(request['question']))
        primary = {'documents': [{'document_id': 1, 'observations': [{
            'text': 'Vendor completed a refund.', 'references': [{'span_id': documents[0]['windows'][0]['span']['span_id']}]}]
            * count, 'limitations': []}]}
        return {'request': request, 'primary': primary, 'evidence_pack': pack}

    async def execute(self, directory, pair=None, order=None, responder=synthetic_response, max_calls=32):
        pair = self.pair() if pair is None else pair
        order = order or ['baseline', 'recovery', 'combined']
        async with mock_sdk('synthetic', responder):
            native = StrandsQueryOrchestrator(); native.enabled = True
            capture = runner.RecoveryCapture(directory, max_calls=max_calls, seconds=10)
            try:
                with runner.native_capture(native, capture, directory) as stages:
                    result = await runner.owned_call(runner.execute_pair(pair, order, native, directory, capture), deadline=capture.deadline)
                capture.require_complete()
                return result, capture, stages
            finally:
                await native.close(); capture.close_pending()

    async def test_concurrent_batches_capture_actual_wire_and_preserve_matched_primary_context(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            result, capture, stages = await self.execute(root, self.pair(9))
            self.assertTrue(all(r['status'] == 'completed' for r in result.values()))
            self.assertEqual(len(capture.attempts), 7)
            self.assertEqual(len(capture.wires), 7)
            before = [json.loads(s['prompt']) for s in stages['attempts'] if s['name'] == 'source_auditor']
            for left, right in zip(before[:3], before[3:]):
                self.assertEqual(left, right)  # Empty valid additions do not change either context.
            self.assertEqual(len(list(root.glob('baseline-audit-*.json'))), 1)
            self.assertEqual(len(list(root.glob('combined-audit-*.json'))), 1)

    async def test_false_primary_is_retained_before_subset_and_original_ledger_is_captured(self):
        def responder(body):
            payload = payload_from(body)
            if 'expected_unit_ids' not in payload:
                handle = payload['source_documents'][0]['windows'][0]['span']['span_id']
                return {'documents': [{'document_id': 1, 'observations': [{
                    'text': 'Vendor requested a refund; completion is not recorded.',
                    'references': [{'span_id': handle}]}], 'limitations': []}]}
            answer = synthetic_response(body)
            for unit, assessment in zip(payload['units'], answer['assessments']):
                if 'requested a refund' in unit['text']:
                    assessment['status'] = 'supported'
                    assessment['checks'].update(subject='supported', predicate='supported', record_role='supported')
            return answer
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            result, capture, stages = await self.execute(root, responder=responder)
            self.assertEqual(result['combined']['disposition'], 'partial')
            original = json.loads((root/'combined-audit-00.json').read_bytes())
            self.assertEqual([c['status'] for c in original['claims']], ['unsupported', 'supported'])
            final = json.loads((root/'combined-final.json').read_bytes())
            self.assertEqual(len(final['claim_ledger']['claims']), 1)
            self.assertEqual(len(list(root.glob('combined-audit-*.json'))), 2)
            audits = [json.loads(s['prompt']) for s in stages['attempts'] if s['name'] == 'source_auditor']
            self.assertTrue(all(a['source_reading'] == self.pair()['primary'] for a in audits))

    async def test_invalid_first_arm_stops_pair_and_marks_remaining_stages_not_run(self):
        for order in (['baseline', 'recovery', 'combined'], ['recovery', 'combined', 'baseline']):
            with tempfile.TemporaryDirectory() as name:
                root = Path(name)
                with self.assertRaises(runner.PairFailure):
                    await self.execute(root, order=order, responder=lambda body: '{}')
                state = json.loads((root/'stages.json').read_bytes())
                self.assertEqual(state[order[0]]['status'], 'failed')
                self.assertTrue(all(state[s]['status'] == 'not_run' for s in order[1:]))
                self.assertEqual(len(list(root.glob('model-*-input.json'))), 2)

    async def test_attempt_budget_never_truncates_candidate_or_starts_recovery(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            with self.assertRaises(runner.PairFailure):
                await self.execute(root, self.pair(9), max_calls=1)
            self.assertEqual(len(json.loads((root/'baseline-candidate.json').read_bytes())['units']), 9)
            self.assertEqual(len(list(root.glob('model-*-input.json'))), 1)
            state = json.loads((root/'stages.json').read_bytes())
            self.assertEqual(state['recovery']['status'], 'not_run')

    async def test_wire_failure_is_shared_integrity_even_when_auditor_swallows_exception(self):
        with tempfile.TemporaryDirectory() as name:
            with patch.object(runner.RecoveryCapture, 'write', side_effect=OSError('synthetic capture failure')):
                with self.assertRaises(runner.IntegrityFailure):
                    await self.execute(Path(name))

    async def test_source_request_mismatch_prevents_any_dispatch(self):
        pair = self.pair(); pair['evidence_pack']['items'][0]['source_content'] = 'Different original.'
        with tempfile.TemporaryDirectory() as name:
            with self.assertRaises(runner.IntegrityFailure):
                await self.execute(Path(name), pair)
            self.assertFalse(list(Path(name).glob('model-*-input.json')))

    async def test_repeated_owner_cancellation_joins_pending_operation(self):
        entered, release = asyncio.Event(), asyncio.Event()
        done = []
        async def operation():
            try: await asyncio.Event().wait()
            finally:
                entered.set(); await release.wait(); done.append(True)
        task = asyncio.create_task(runner.owned_call(operation()))
        await asyncio.sleep(0); task.cancel(); await entered.wait(); task.cancel()
        await asyncio.sleep(0); release.set()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(done, [True])

    def test_schedule_preserves_twelve_pairs_and_reverses_second_repetition(self):
        rows = runner.schedule()
        self.assertEqual(len(rows), 12)
        self.assertEqual([r['case'] for r in rows[:6]], list(reversed([r['case'] for r in rows[6:]])))
        self.assertTrue(all(r['order'] == ['baseline', 'recovery', 'combined'] for r in rows[:6]))
        self.assertTrue(all(r['order'] == ['recovery', 'combined', 'baseline'] for r in rows[6:]))
