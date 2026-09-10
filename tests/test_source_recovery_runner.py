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

    async def test_pre_subset_ledger_write_failure_is_shared_integrity(self):
        native_write = runner.ModelCapture.write
        def failing(capture, name, payload):
            if name == 'baseline-audit-00.json': raise OSError('synthetic ledger write failure')
            return native_write(capture, name, payload)
        with tempfile.TemporaryDirectory() as name:
            with patch.object(runner.ModelCapture, 'write', failing):
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

    async def package(self, root):
        from scripts.source_recovery_preflight import prepare
        from scripts.eval_source_audit import write_private
        def write(name, value):
            write_private(root/name, value)
            return name
        pair = self.pair(); names = runner.CASE_ORDER
        gold = write('gold.json', {})
        requests = {c: write(c+'-request.json', pair['request']) for c in names}
        originals = {c: write(c+'-original.json', {'id': 1, 'content': 'Synthetic original.'}) for c in names}
        b2 = {'gold': gold, 'originals': originals, 'payloads': {c: {'F': requests[c]} for c in names}}
        b2['sha256'] = {p.name: runner.digest(p) for p in root.iterdir()}
        b2_manifest = write('b2-manifest.json', b2)
        rows, old, inventory = [], [], {}
        for index, row in enumerate(runner.schedule()):
            primary = write(f'primary-{index:02d}.json', pair['primary'])
            rows.append({**row, 'input': write(f'pair-{index:02d}.json', pair), 'primary': primary, 'b2_index': index})
            old.append({'case': row['case'], 'repetition': row['repetition'], 'arm': 'F', 'index': index, 'status': 'completed'})
            inventory[f'invocation-{index:02d}/reading.json'] = runner.digest(root/primary)
        b2_run = write('b2-run.json', {'rows': old})
        b2_inventory = write('b2-inventory.json', inventory)
        protocol = write('protocol.json', {'synthetic': True})
        grade = write('grade.json', {'synthetic': True})
        manifest = {'kind': 'source-recovery-v1', 'status': 'admitted', 'limits': runner.LIMITS,
            'schedule': runner.schedule(), 'pairs': rows, 'requests': requests, 'originals': originals,
            'failure_policy': 'stop_pair_continue_controls_stop_shared', 'provider_capacity': 'unknown',
            'b2_manifest': b2_manifest, 'b2_run': b2_run, 'b2_inventory': b2_inventory,
            'b2_grade_spec': grade, 'b2_grade_standards': grade, 'b2_adjudication': grade,
            'gold': gold, 'protocol': protocol, 'runtime': None, 'code_sha256': runner.code_identity()}
        write('manifest-prepared.json', manifest)
        await prepare(root, root/'preflight', 'synthetic')
        manifest['runtime'] = runner.runtime_identity()
        manifest['preflight'] = 'preflight/preflight.json'
        manifest['sha256'] = {str(p.relative_to(root)): runner.digest(p) for p in root.rglob('*') if p.is_file()}
        manifest['review_receipts'] = [{'reviewer': name, 'status': 'approved',
            'subject_sha256': runner.admission_subject(manifest)} for name in ('synthetic-a', 'synthetic-b')]
        write('manifest.json', manifest)
        return manifest

    async def test_full_runner_continues_individual_failure_but_stops_shared_failure(self):
        from app import strands_orchestrator as native
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            with patch.object(native.settings, 'strands_enabled', True), patch.object(native, 'STRANDS_AVAILABLE', True), \
                    patch.object(native.settings, 'strands_model', 'synthetic'):
                manifest = await self.package(root)
                calls = [0]
                def failing_first(body):
                    calls[0] += 1
                    return TimeoutError('synthetic transport') if calls[0] == 1 else synthetic_response(body)
                async with mock_sdk('synthetic', failing_first):
                    await runner.run(root/'manifest.json', root/'individual')
                result = json.loads((root/'individual/run.json').read_bytes())
                self.assertEqual(result['rows'][0]['status'], 'failed')
                self.assertTrue(all(r['status'] == 'completed' for r in result['rows'][1:]))
                self.assertEqual(result['native_attempts'], 34)
                native_write = runner.ModelCapture.write
                def broken_ledger(capture, name, payload):
                    if name == 'baseline-audit-00.json': raise OSError('synthetic ledger write failure')
                    return native_write(capture, name, payload)
                async with mock_sdk('synthetic'):
                    with patch.object(runner.ModelCapture, 'write', broken_ledger):
                        with self.assertRaises(runner.IntegrityFailure):
                            await runner.run(root/'manifest.json', root/'shared')
                result = json.loads((root/'shared/run.json').read_bytes())
                self.assertEqual(result['native_attempts'], 1)
                self.assertEqual(result['rows'][0]['status'], 'failed')
                self.assertTrue(all(r['status'] == 'not_run' for r in result['rows'][1:]))
                native_read = Path.read_bytes
                def broken_read(path):
                    if path.name == 'model-000-output.json' and path.parent.parent == root/'shared-read':
                        raise OSError('synthetic capture read failure')
                    return native_read(path)
                async with mock_sdk('synthetic'):
                    with patch.object(Path, 'read_bytes', broken_read):
                        with self.assertRaises(runner.IntegrityFailure):
                            await runner.run(root/'manifest.json', root/'shared-read')
                result = json.loads((root/'shared-read/run.json').read_bytes())
                self.assertEqual(result['native_attempts'], 1)
                self.assertTrue(all(r['status'] == 'not_run' for r in result['rows'][1:]))
                # Validating the hash and consuming the same bytes prevents an input swap.
                (root/manifest['pairs'][0]['input']).write_text('{}')
                with self.assertRaises(runner.IntegrityFailure):
                    await runner.run(root/'manifest.json', root/'changed')
                self.assertFalse((root/'changed').exists())

    def test_schedule_preserves_twelve_pairs_and_reverses_second_repetition(self):
        rows = runner.schedule()
        self.assertEqual(len(rows), 12)
        self.assertEqual([r['case'] for r in rows[:6]], list(reversed([r['case'] for r in rows[6:]])))
        self.assertTrue(all(r['order'] == ['baseline', 'recovery', 'combined'] for r in rows[:6]))
        self.assertTrue(all(r['order'] == ['recovery', 'combined', 'baseline'] for r in rows[6:]))
