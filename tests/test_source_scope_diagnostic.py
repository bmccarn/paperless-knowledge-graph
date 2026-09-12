"""Matched scope profiles use the shared runner and actual SDK capture ownership."""
import json
from pathlib import Path
import tempfile
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import evidence_spans
from app.question_evidence import canonical_json, coarse_requirements
from app.source_reading import group_sources
from app.strands_orchestrator import StrandsQueryOrchestrator
from scripts import source_scope_diagnostic as profile
from scripts import run_model_comparison as runner
from scripts.source_scope_preflight import responder
from scripts.source_recovery_preflight import mock_sdk
from tests.test_source_scopes import digest
from tests.test_source_recovery_runner import payload_from


class ScopeDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    def execution(self, scoped, partial):
        prefix = 'Vendor requested a refund. '
        hidden = 'WITHHELD_FINAL_ACTION was completed.'
        original = dict(id=1, title='Record', content=prefix + hidden)
        text = prefix if partial else original['content']
        pack = {'items': [dict(id='record', document_id=1, chunk_index=0,
                              source_kind='ocr', title='Record', content=text)]}
        documents = group_sources(evidence_spans(pack, citation_safe=True))
        question = 'What does the record establish?'
        request = dict(question=question, evaluated_at='2026-09-10', source_date_order='mdy',
                       source_documents=documents, **coarse_requirements(question))
        primary = {'documents': [dict(document_id=1, observations=[dict(text='Vendor completed a refund.',
            references=[{'span_id': documents[0]['windows'][0]['span']['span_id']}])], limitations=[])]}
        binding = dict(case='replacement', trial=1, route=runner.ROUTES[0],
                       profile='scoped' if scoped else 'control', order=['baseline', 'reader', 'fresh'])
        return profile.PreparedExecution(canonical_json(dict(index=0, binding=binding, original=original,
            chunks=[dict(document_id=1, content_digest=digest(prefix), start=0, end=len(prefix))] if partial else [],
            pair=dict(request=request, primary=primary, evidence_pack=pack), challenge_ids=[])))

    async def execute(self, execution, root, expected=None):
        native = StrandsQueryOrchestrator(); native.enabled = True
        capture = runner.ComparisonCapture(root, max_calls=32, seconds=20)
        capture.expected_wires = expected
        try:
            with runner.recovery.native_capture(native, capture, root, allowed_stages=runner.STAGES) as stages:
                result = await runner.owned_call(profile.execute(execution, native, capture, root), deadline=capture.deadline)
            capture.require_complete()
            return result, capture, stages
        finally:
            await native.close(); capture.close_pending()

    async def test_both_profiles_capture_corrections_subset_and_source_isolation(self):
        for scoped in (False, True):
            for partial in (False, True):
                with self.subTest(scoped=scoped, partial=partial), tempfile.TemporaryDirectory() as temp:
                    async with mock_sdk(runner.ROUTES[0], responder) as bodies:
                        result, capture, stages = await self.execute(self.execution(scoped, partial), Path(temp))
                    self.assertEqual(result['fresh']['disposition'], 'partial')
                    self.assertEqual(len(result['fresh']['audit_ledgers']), 2)
                    self.assertEqual(len(bodies), 6)
                    readers, audits = [], []
                    for body in bodies:
                        payload = payload_from(body)
                        (audits if 'units' in payload else readers).append(payload)
                        if partial:
                            self.assertNotIn('WITHHELD_FINAL_ACTION', json.dumps(payload['source_documents']))
                        for doc in payload['source_documents']:
                            self.assertEqual('source_scope' in doc, scoped)
                            if scoped:
                                self.assertEqual(doc['source_scope']['coverage'], 'partial_original' if partial else 'complete_original')
                    self.assertEqual(len(readers), 2)
                    self.assertIn('reading_protocol_correction', readers[1])
                    self.assertTrue(any(p.get('protocol_correction') for p in audits))
                    self.assertTrue(all(not d['observations'] for p in audits for d in p['source_reading']['documents']))
                    self.assertEqual('reading-scope-receipt.json' in capture.hashes, scoped)
                    self.assertEqual(len(capture.wires), len(capture.attempts))

    async def test_changed_known_wire_stops_before_transport(self):
        with tempfile.TemporaryDirectory() as temp:
            async with mock_sdk(runner.ROUTES[0], responder) as bodies:
                with self.assertRaises(runner.IntegrityFailure):
                    await self.execute(self.execution(True, True), Path(temp), expected=[])
            self.assertEqual(bodies, [])
            state = json.loads((Path(temp)/'stages.json').read_bytes())
            self.assertEqual(state['baseline']['status'], 'failed')
            self.assertEqual(state['reader']['status'], 'not_run')

    def test_schedule_has_exact_matched_inputs_and_counterbalanced_profiles(self):
        rows = profile.schedule()
        self.assertEqual(len(rows), 96)
        for i in range(0, len(rows), 2):
            left, right = rows[i:i+2]
            self.assertEqual({k:v for k,v in left.items() if k != 'profile'},
                             {k:v for k,v in right.items() if k != 'profile'})
            self.assertEqual({left['profile'], right['profile']}, {'control', 'scoped'})
        self.assertEqual({rows[i]['profile'] for i in range(0, len(rows), 2)}, {'control', 'scoped'})

    def test_partial_derivation_rejects_hidden_context_and_forged_scope(self):
        import copy
        for variant, (case, end, extent) in profile.PARTIALS.items():
            prefix = 'Visible authorization. '.ljust(end)
            hidden = 'WITHHELD_COMPLETION'.ljust(extent-end)
            original = dict(id=1, title='Record', content=prefix + hidden)
            question = 'What does the record establish?'
            item = dict(document_id=1, chunk_index=0, title='Record', content=prefix,
                        source_kind='ocr', feedback_open=False)
            item['id'] = profile.digest(dict(case_id=case, document_id=1,
                original_content_sha256=digest(original['content']), start=0, end=end))[:24]
            pack = dict(question=question, mode='strict', items=[item], coverage={
                'retrieval_is_exhaustive': False, 'source_document_count': 1, 'available_chunk_count': 1})
            spans = evidence_spans(pack, citation_safe=True)
            request = dict(question=question, evaluated_at='2026-09-10', source_date_order='mdy',
                source_documents=group_sources(spans), **coarse_requirements(question))
            fixture = dict(id=variant, case_id=case, evidence_pack=pack, request=request,
                canonical_spans_sha256=profile.digest(spans), chunk_bindings=[dict(document_id=1,
                    content_digest=digest(prefix), start=0, end=end)], expected_scope={
                    'coverage': 'partial_original', 'original_identity': dict(document_id=1,
                        content_digest=digest(original['content']), extent=extent),
                    'supplied_intervals': [dict(start=0, end=end)], 'missing_intervals': [[end, extent]],
                    'complete_original_reference': None})
            pair, chunks = profile.prefix_pair(dict(request=request), original, fixture)
            self.assertEqual(pair['request'], request)
            self.assertNotIn('WITHHELD_COMPLETION', json.dumps(pair))
            for mutation in (
                lambda f: f['evidence_pack']['items'][0].update(_source_document_content=original['content']),
                lambda f: f['request']['source_documents'][0]['windows'][0]['span'].update(boundary_after='WI'),
                lambda f: f['chunk_bindings'][0].update(start=False),
                lambda f: f['expected_scope'].update(coverage='complete_original'),
                lambda f: f.update(canonical_spans_sha256='0' * 64),
            ):
                forged = copy.deepcopy(fixture); mutation(forged)
                with self.assertRaises(runner.IntegrityFailure):
                    profile.prefix_pair(dict(request=request), original, forged)

    async def test_preflight_rejects_missing_receipts_paths_and_changed_preparation(self):
        import copy
        from scripts.eval_source_audit import write_private
        execution = self.execution(True, True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); directory = root/'preflight/execution-00'; directory.mkdir(parents=True)
            async with mock_sdk(runner.ROUTES[0], responder):
                result, capture, stages = await self.execute(execution, directory)
            binding = execution.record['binding']
            wires = [capture.read(n) for n in capture.hashes if n.startswith('wire-')]
            row = dict(index=0, **binding, prepared_digest=execution.digest, requests=len(wires),
                model_sha256=dict(capture.hashes), stage_sha256=dict(stages['hashes']),
                total_request_bytes=sum(w['bytes'] for w in wires), largest_request_bytes=max(w['bytes'] for w in wires))
            (root/'manifest-prepared.json').write_text('{}')
            code = runner.recovery.code_identity()
            runtime = {'synthetic': True}
            report = dict(scope='source scope SDK localhost MockTransport only', native_model_calls=0,
                code_sha256=code, input_manifest_sha256=digest('{}'), runtimes={binding['route']: runtime},
                rows=[row] + [{} for _ in profile.schedule()[1:]])
            write_private(root/'preflight/report.json', report)
            manifest = dict(preflight='preflight/report.json', code_sha256=code,
                runtimes={binding['route']: runtime}, sha256={str(p.relative_to(root)): runner.recovery.digest(p)
                    for p in root.rglob('*') if p.is_file()})
            frozen = runner.freeze(manifest, root)
            self.assertEqual(len(profile.known_wires(manifest, frozen, execution)), 3)
            for mutation in (
                lambda r: r['rows'][0].update(prepared_digest='0' * 64),
                lambda r: r['rows'][0]['model_sha256'].pop('fresh-audit-01.json'),
                lambda r: r['rows'][0]['model_sha256'].pop('reading-scope-receipt.json'),
                lambda r: r.update(native_model_calls=1),
                lambda r: r.update(input_manifest_sha256='0' * 64),
            ):
                changed = copy.deepcopy(report); mutation(changed)
                altered = {**frozen, manifest['preflight']: canonical_json(changed).encode()}
                with self.assertRaises(runner.IntegrityFailure): profile.known_wires(manifest, altered, execution)

            # Corrupt dynamic wire units while preserving internally consistent
            # SDK/model/stage byte hashes. Path labels alone must not earn coverage.
            for corruption in ('empty', 'expected_ids', 'correction_ranges', 'subset_predicate'):
                altered, changed_manifest = dict(frozen), copy.deepcopy(manifest)
                changed_report = copy.deepcopy(report); changed_row = changed_report['rows'][0]
                def replace_artifact(name, value):
                    raw = canonical_json(value).encode()
                    altered['preflight/execution-00/' + name] = raw
                    checksum = digest(raw.decode())
                    changed_manifest['sha256']['preflight/execution-00/' + name] = checksum
                    for inventory in ('model_sha256', 'stage_sha256'):
                        if name in changed_row[inventory]: changed_row[inventory][name] = checksum
                for name in list(capture.hashes):
                    if not name.startswith('wire-'): continue
                    wire = json.loads(altered['preflight/execution-00/' + name])
                    if wire['operation'] != 'fresh': continue
                    body = json.loads(wire['body']); payload = payload_from(body)
                    if corruption == 'empty':
                        payload['units'] = []; payload['expected_unit_ids'] = []
                    elif corruption == 'expected_ids': payload['expected_unit_ids'] = ['foreign']
                    elif corruption == 'correction_ranges' and payload.get('protocol_correction'):
                        payload['units'][0]['start'] += 1
                    elif corruption == 'subset_predicate' and wire['audit_generation'] == 1:
                        payload['units'][0]['text'] = payload['units'][0]['text'].replace('alpha', 'beta')
                    else: continue
                    prompt = json.dumps(payload, ensure_ascii=False)
                    content = body['messages'][-1]['content']
                    if isinstance(content, str): body['messages'][-1]['content'] = prompt
                    else: content[0]['text'] = prompt
                    model_name = f"model-{wire['model_index']:03d}-input.json"
                    model = json.loads(altered['preflight/execution-00/' + model_name])
                    model['request']['messages'] = body['messages']; replace_artifact(model_name, model)
                    stage_name = f"stage-{wire['stage_index']:03d}-input.json"
                    stage = json.loads(altered['preflight/execution-00/' + stage_name])
                    stage['prompt'] = prompt; replace_artifact(stage_name, stage)
                    wire['body'] = canonical_json(body)
                    wire['bytes'] = len(wire['body'].encode()); wire['sha256'] = digest(wire['body'])
                    replace_artifact(name, wire)
                modified_wires = [json.loads(altered['preflight/execution-00/' + n])
                                  for n in capture.hashes if n.startswith('wire-')]
                changed_row['total_request_bytes'] = sum(w['bytes'] for w in modified_wires)
                changed_row['largest_request_bytes'] = max(w['bytes'] for w in modified_wires)
                altered[manifest['preflight']] = canonical_json(changed_report).encode()
                changed_manifest['sha256'][manifest['preflight']] = digest(canonical_json(changed_report))
                with self.subTest(corruption=corruption), self.assertRaises(runner.IntegrityFailure):
                    profile.known_wires(changed_manifest, altered, execution)
