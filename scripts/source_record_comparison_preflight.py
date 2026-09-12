"""Complete comparison serialization with actual pinned SDK and local synthetic replies."""
import hashlib
import json
from pathlib import Path

from app.source_record_reader import requests
from scripts.eval_source_audit import write_private
from scripts.source_recovery_preflight import mock_sdk, synthetic_response
from scripts import run_source_records as runner


def responder(body):
    content = body['messages'][-1]['content']
    payload = json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))
    if 'focus_block_ids' not in payload: return synthetic_response(body)
    # Exercise audit serialization too, without evaluating any source meaning.
    return {'blocks': [{'block_id': identity, 'status': 'interpreted', 'observations': [{
        'text': 'Synthetic serialization observation; not factual evaluation.',
        'references': [{'block_id': identity}]}], 'reason': None} for identity in payload['focus_block_ids']]}


async def prepare(manifest_path, output, model):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    manifest_path, output = Path(manifest_path), Path(output)
    manifest_bytes = manifest_path.read_bytes(); manifest = json.loads(manifest_bytes)
    frozen = runner.freeze(manifest, manifest_path.parent)
    runner.validate_sources(manifest, frozen)
    code = runner.recovery.code_identity()
    output.mkdir(mode=0o700, exist_ok=False)
    rows = []
    async with mock_sdk(model, responder):
        runtime = runner.recovery.runtime_identity()
        for index, binding in enumerate(manifest['pairs']):
            pair = runner.bound(frozen, binding['input'])
            original = runner.bound(frozen, manifest['originals'][binding['case']])
            *_, inventory, question, anchor = runner.prepared_inputs(pair, original)
            directory = output/f'pair-{index:02d}'; directory.mkdir(mode=0o700)
            native = StrandsQueryOrchestrator(); native.enabled = True
            capture = runner.RecordCapture(directory, max_calls=runner.LIMITS['pair_attempts'], seconds=300)
            try:
                with runner.recovery.native_capture(native, capture, directory, allowed_stages=runner.STAGES) as stages:
                    await runner.owned_call(runner.execute_pair(pair, original, binding['order'], native,
                        directory, capture), deadline=capture.deadline)
                capture.require_complete()
                artifacts = [(name, capture.read(name)) for name in capture.hashes if name.startswith('wire-')]
                known = [(name, data) for name, data in artifacts if data['operation'] == 'reader'
                         or (data['operation'] == 'baseline' and data['audit_generation'] == 0)]
                reader_count = sum(data['operation'] == 'reader' for _, data in known)
                if reader_count != len(requests(inventory, question)):
                    raise runner.IntegrityFailure('Incomplete reader wire preflight')
                rows.append({'index': index, 'case': binding['case'], 'repetition': binding['repetition'],
                    'requests': len(artifacts), 'reader_requests': reader_count,
                    'baseline_requests': sum(data['operation'] == 'baseline' for _, data in known),
                    'known_wires': [f'preflight/pair-{index:02d}/{name}' for name, _ in known],
                    'largest_request_bytes': max(data['bytes'] for _, data in artifacts),
                    'total_request_bytes': sum(data['bytes'] for _, data in artifacts),
                    'model_sha256': dict(capture.hashes), 'stage_sha256': dict(stages['hashes']),
                    'inventory_digest': inventory.digest, 'documents_digest': anchor})
            finally:
                await runner.owned_call(native.close()); capture.close_pending()
    if code != runner.recovery.code_identity(): raise runner.IntegrityFailure('Preflight code changed')
    report = {'scope': 'source-record comparison SDK localhost MockTransport only',
        'native_model_calls': 0, 'runtime': runtime, 'model': model, 'code_sha256': code,
        'input_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(), 'rows': rows,
        'requests': sum(r['requests'] for r in rows),
        'largest_request_bytes': max(r['largest_request_bytes'] for r in rows),
        'total_request_bytes': sum(r['total_request_bytes'] for r in rows),
        'dynamic_audit_sizes': 'Synthetic per-block observations only; actual reader output and subset audits unknown'}
    write_private(output/'report.json', report)
    write_private(output/'inventory.json', {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(output.rglob('*')) if p.is_file()})
    return {k: report[k] for k in ('requests', 'largest_request_bytes', 'total_request_bytes', 'native_model_calls')}
