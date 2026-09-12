"""Both routes through actual pinned SDK localhost transport; zero provider calls."""
import hashlib
import json
from pathlib import Path
from scripts import run_model_comparison as runner
from scripts.eval_source_audit import write_private
from scripts.source_recovery_preflight import mock_sdk, synthetic_response


def responder(body):
    content = body['messages'][-1]['content']
    payload = json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))
    if 'expected_unit_ids' in payload: return synthetic_response(body)
    if 'reading_protocol_correction' not in payload: return {}
    return {'documents': [{'document_id': d['document_id'], 'observations': [{
        'text': 'Synthetic serialization observation, not a factual evaluation.',
        'references': [{'span_id': d['windows'][0]['span']['span_id']}]}], 'limitations': []}
        for d in payload['source_documents']]}


async def prepare(manifest_path, output):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    manifest_path, output = Path(manifest_path), Path(output)
    raw = manifest_path.read_bytes(); manifest = json.loads(raw)
    frozen = runner.freeze(manifest, manifest_path.parent)
    runner.validate_sources(manifest, frozen)
    code = runner.recovery.code_identity()
    output.mkdir(mode=0o700, exist_ok=False)
    rows, runtimes = [], {}
    for index, binding in enumerate(manifest['executions']):
        pair, original = runner.execution_inputs(manifest, frozen, binding)
        directory = output/f'execution-{index:02d}'; directory.mkdir(mode=0o700)
        with runner.route_profile(binding['route']):
            async with mock_sdk(binding['route'], responder):
                runtimes[binding['route']] = runner.recovery.runtime_identity()
                native = StrandsQueryOrchestrator(); native.enabled = True
                # Local-only instrumentation ceilings, not native experiment admission.
                capture = runner.ComparisonCapture(directory, max_calls=10000, seconds=300)
                try:
                    with runner.recovery.native_capture(native, capture, directory, allowed_stages=runner.STAGES) as stages:
                        await runner.owned_call(runner.execute_pair(pair, original, binding['order'], native,
                            directory, capture), deadline=capture.deadline)
                    capture.require_complete()
                    artifacts = [(name, capture.read(name)) for name in capture.hashes if name.startswith('wire-')]
                    known = [(n, w) for n, w in artifacts if w['operation'] == 'reader' or
                             (w['operation'] == 'baseline' and w['audit_generation'] == 0)]
                    rows.append({'index': index, **binding, 'requests': len(artifacts),
                        'reader_requests': sum(w['operation'] == 'reader' for _, w in known),
                        'baseline_requests': sum(w['operation'] == 'baseline' for _, w in known),
                        'known_wires': [f'preflight/execution-{index:02d}/{n}' for n, _ in known],
                        'largest_request_bytes': max(w['bytes'] for _, w in artifacts),
                        'total_request_bytes': sum(w['bytes'] for _, w in artifacts),
                        'model_sha256': dict(capture.hashes), 'stage_sha256': dict(stages['hashes'])})
                finally:
                    await runner.owned_call(native.close()); capture.close_pending()
    if code != runner.recovery.code_identity(): raise runner.IntegrityFailure('Preflight code changed')
    report = {'scope': 'model comparison SDK localhost MockTransport only', 'native_model_calls': 0,
        'runtimes': runtimes, 'code_sha256': code, 'input_manifest_sha256': hashlib.sha256(raw).hexdigest(),
        'rows': rows, 'requests': sum(r['requests'] for r in rows),
        'largest_request_bytes': max(r['largest_request_bytes'] for r in rows),
        'total_request_bytes': sum(r['total_request_bytes'] for r in rows),
        'dynamic_audit_sizes': 'Synthetic one-observation reading; native fresh/subset audit demand remains unknown'}
    write_private(output/'report.json', report)
    write_private(output/'inventory.json', {str(p.relative_to(output)): runner.recovery.digest(p)
        for p in sorted(output.rglob('*')) if p.is_file()})
    return {k: report[k] for k in ('requests', 'largest_request_bytes', 'total_request_bytes', 'native_model_calls')}
