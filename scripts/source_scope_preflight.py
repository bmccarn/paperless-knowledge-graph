"""Actual pinned-SDK serialization for both scope profiles; localhost only."""
import hashlib
import json
from pathlib import Path

from scripts import source_scope_diagnostic as profile
from scripts import run_model_comparison as runner
from scripts.eval_source_audit import write_private
from scripts.source_recovery_preflight import mock_sdk

MARKER = 'Synthetic scope serialization observation '


def responder(body):
    content = body['messages'][-1]['content']
    payload = json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))
    doc = payload['source_documents'][0]
    scoped = 'source_scope' in doc
    reference = (doc['source_scope']['complete_original_reference'] or
                 {'kind': 'passage', 'handle': doc['windows'][0]['span']['span_id']}) if scoped else {
                     'span_id': doc['windows'][0]['span']['span_id']}
    if 'expected_unit_ids' not in payload:
        if 'reading_protocol_correction' not in payload: return {}
        return {'documents': [dict(document_id=doc['document_id'], observations=[
            dict(text=MARKER + suffix + '.', references=[reference]) for suffix in ('alpha', 'beta')], limitations=[])]}
    fresh = all(u['text'].removeprefix('- ').startswith(MARKER) for u in payload['units'])
    invalid_scope = fresh and len(payload['units']) == 2 and not payload.get('protocol_correction')
    rows = []
    for index, unit in enumerate(payload['units']):
        supported = fresh and (invalid_scope or index == 0)
        rows.append(dict(unit_id=unit['id'], status='supported' if supported else 'unsupported',
            source_basis='Synthetic serialization response; not a factual evaluation.',
            checks=dict(subject='supported', predicate='supported' if supported else 'not_established',
                        record_role='supported', conditions='not_applicable', temporal='supported', comparison='not_applicable'),
            unresolved_assumptions=[], references=[reference], temporal_scope='historical' if invalid_scope else 'none',
            temporal_assertion='none', comparison_scope=None, comparison_document_ids=[]))
    return {'assessments': rows}


async def prepare(manifest_path, output):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    manifest_path, output = Path(manifest_path), Path(output)
    raw = manifest_path.read_bytes(); manifest = json.loads(raw)
    frozen = runner.freeze(manifest, manifest_path.parent)
    prepared = profile.prepare(manifest, frozen)
    code = runner.recovery.code_identity()
    profile.require(manifest['code_sha256'] == code)
    output.mkdir(mode=0o700, exist_ok=False)
    rows, runtimes = [], {}
    for execution in prepared:
        data = execution.record; index, binding = data['index'], data['binding']
        directory = output/f'execution-{index:02d}'; directory.mkdir(mode=0o700)
        with runner.route_profile(binding['route']):
            async with mock_sdk(binding['route'], responder):
                runtimes[binding['route']] = runner.recovery.runtime_identity()
                native = StrandsQueryOrchestrator(); native.enabled = True
                capture = runner.ComparisonCapture(directory, max_calls=10000, seconds=300)
                try:
                    with runner.recovery.native_capture(native, capture, directory, allowed_stages=runner.STAGES) as stages:
                        await runner.owned_call(profile.execute(execution, native, capture, directory), deadline=capture.deadline)
                    capture.require_complete()
                    wires = [capture.read(n) for n in capture.hashes if n.startswith('wire-')]
                    rows.append(dict(index=index, **binding, prepared_digest=execution.digest,
                        requests=len(wires), largest_request_bytes=max(w['bytes'] for w in wires),
                        total_request_bytes=sum(w['bytes'] for w in wires), model_sha256=dict(capture.hashes),
                        stage_sha256=dict(stages['hashes'])))
                finally:
                    await runner.owned_call(native.close()); capture.close_pending()
    profile.require(code == runner.recovery.code_identity())
    report = dict(scope='source scope SDK localhost MockTransport only', native_model_calls=0,
        code_sha256=code, runtimes=runtimes, input_manifest_sha256=hashlib.sha256(raw).hexdigest(), rows=rows,
        requests=sum(r['requests'] for r in rows), largest_request_bytes=max(r['largest_request_bytes'] for r in rows),
        total_request_bytes=sum(r['total_request_bytes'] for r in rows),
        dynamic_audit_sizes='Synthetic two-observation readings; native fresh/subset demand remains unknown')
    write_private(output/'report.json', report)
    write_private(output/'inventory.json', {str(p.relative_to(output)): runner.recovery.digest(p)
        for p in sorted(output.rglob('*')) if p.is_file()})
    return {k: report[k] for k in ('requests', 'largest_request_bytes', 'total_request_bytes', 'native_model_calls')}
