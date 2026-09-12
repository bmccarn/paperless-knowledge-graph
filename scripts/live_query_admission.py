"""Result-bound continuation for six live requests; no application clients open here."""
import base64
import json
import math
import re
from pathlib import Path

from scripts.conservative_query_admission import strict_json
from scripts.live_query_control import _identity
from scripts.live_query_evaluation import reviewed_snapshot, sha256
from scripts.live_query_manifest import parse_requests, prepare_manifest


def artifact_bytes(directory):
    """Retain the whole attempt, excluding only its separately bound review receipts."""
    captured = {}
    for path in sorted(directory.rglob('*')):
        if path.is_symlink():
            raise ValueError('Symlinked live evidence is not admitted')
        if path.is_file():
            name = path.relative_to(directory).as_posix()
            if name not in {'result.json', 'review.json', 'grade-spec.json', 'grade-standards.json'}:
                captured[name] = path.read_bytes()
    return captured


def validate_runtime(runtime, browser, process, request):
    """Require actual successful delivery and matching browser transport, not replay."""
    if (runtime.get('error') is not None or runtime.get('exception_types') != []
            or runtime.get('denied_graph_operations') != [] or runtime.get('denied_vector_operations') != []
            or not isinstance(runtime.get('final'), dict)):
        raise ValueError('Live runtime failed or crossed a read-only boundary')
    if (process.get('remote_cleanup_confirmed') is not True
            or process.get('exception_type') is not None or process.get('cleanup_exception_type') is not None
            or process.get('forced_local_exit') is not False
            or type(process.get('returncode')) is not int or process['returncode'] != 0):
        raise ValueError('Remote/local lifetime did not close successfully')
    identity = _identity(process.get('identity'))
    ready = process.get('ready', {})
    if (set(ready) != {'event', *identity, 'port'} or ready.get('event') != 'ready'
            or any(ready.get(k) != v for k, v in identity.items())
            or type(ready.get('port')) is not int or not 0 < ready['port'] <= 65535
            or process.get('closed') != {'event': 'closed', **identity, 'ready': True,
                'reason': 'stop', 'exception_type': None, 'cleanup_exception_type': None}):
        raise ValueError('Remote acknowledgment does not match readiness and admission')
    delivery = runtime.get('delivery', {})
    if delivery.get('statuses') != [200] or delivery.get('completed') != [True]:
        raise ValueError('Exactly one complete live stream required')
    bodies = delivery.get('bodies_base64')
    if not isinstance(bodies, list) or len(bodies) != 1:
        raise ValueError('Single raw stream body required')
    body = base64.b64decode(bodies[0], validate=True)
    if (browser.get('completed') is not True or browser.get('request') != request
            or browser.get('page_errors') != [] or type(browser.get('submission_count')) is not int
            or browser['submission_count'] != 1
            or browser.get('sse_sha256') != sha256(body)):
        raise ValueError('Built browser submission or received stream differs')
    return body


def expected_request_identity(manifest, request):
    # Match the frozen QueryEngine._query identity without importing its clients.
    from app.answer_finalization import POLICY_VERSION
    from app.question_evidence import PIPELINE_VERSION
    config = manifest['configuration']
    identity = {'policy': f'{POLICY_VERSION}:bounded-context-v1:{PIPELINE_VERSION}',
                'mode': request['mode'], 'question': request['question'],
                'history': request['history'], 'model': request['model'],
                'strands_model': config['strands_model'] or config['gemini_model'],
                'generation': manifest['corpus_snapshot']['generation'],
                'evaluated_at': manifest['evaluated_at'],
                'source_date_order': config['source_date_order']}
    return sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode())


def validate_request_identity(final, manifest, request):
    plan = final.get('query_plan', {})
    digest = expected_request_identity(manifest, request)
    if (final.get('question') != request['question'] or final.get('mode') != request['mode']
            or plan.get('original_question') != request['question']
            or plan.get('request_identity_digest') != digest
            or final.get('finalization', {}).get('request_identity_digest') != digest):
        raise ValueError('Actual engine request differs from scheduled request identity')


def capture_result(directory, manifest, request, index):
    """Package retained outputs; this does not grant an original-source passing grade."""
    captured = artifact_bytes(directory)
    runtime, browser, process = [strict_json(captured[name])
                                for name in ('runtime.json', 'browser.json', 'process.json')]
    body = validate_runtime(runtime, browser, process, request)
    validate_request_identity(runtime['engine_final'], manifest, request)
    from scripts.live_query_delivery import DeliveredEvents
    delivery = DeliveredEvents(None)
    delivery.statuses, delivery.completed, delivery.bodies = [200], [True], [body]
    public = delivery.conserved_final(runtime['engine_final'], manifest['configuration']['paperless_url'])
    if public != runtime['final']:
        raise ValueError('Retained final differs from actual engine and SSE delivery')
    manifest_hash = sha256(json.dumps(manifest, sort_keys=True, allow_nan=False).encode())
    if process['identity']['manifest_sha256'] != manifest_hash:
        raise ValueError('Process belongs to a different live admission')
    if runtime['corpus_before'] != manifest['corpus_snapshot'] or runtime['corpus_after'] != manifest['corpus_snapshot']:
        raise ValueError('Live corpus differs from admitted identity')
    for name in ('model_sha256', 'stage_sha256', 'originals_sha256'):
        hashes = runtime.get(name)
        if not isinstance(hashes, dict) or not hashes:
            raise ValueError('Complete model, stage and original captures required')
        prefix = 'originals/' if name == 'originals_sha256' else ''
        for path, digest in hashes.items():
            path = prefix + path
            if path not in captured or sha256(captured[path]) != digest:
                raise ValueError('Runtime evidence bytes changed')
    if {name for name in captured if name.startswith('originals/')} != {
            'originals/' + name for name in runtime['originals_sha256']}:
        raise ValueError('Missing or extra captured originals')
    # Sidecar hashes alone do not establish a complete native attempt. Check both
    # halves, exact call identity and the provider's terminal signal again.
    from scripts.live_query_capture import stream_completion_error
    count = runtime.get('native_call_count')
    if type(count) is not int or not 0 < count <= 300:
        raise ValueError('Invalid captured call count')
    model_names = {f'model-{i:03d}-{side}.json' for i in range(count) for side in ('input', 'output')}
    if set(runtime['model_sha256']) != model_names or {n for n in captured if n.startswith('model-')} != model_names:
        raise ValueError('Missing or extra native attempt')
    for i in range(count):
        input = strict_json(captured[f'model-{i:03d}-input.json'])
        output = strict_json(captured[f'model-{i:03d}-output.json'])
        if (input.get('index') != i or not isinstance(input.get('kind'), str)
                or any(output.get(k) != input.get(k) for k in ('index', 'kind', 'request'))
                or output.get('status') != 'completed' or output.get('exception_type') is not None):
            raise ValueError('Native call did not complete with its captured identity')
        if not re.fullmatch(r'(?:query|retrieval|stage-[0-9]{3,}):(?:chat|embedding)', input['kind']):
            raise ValueError('Unknown native capture kind')
        if input['kind'].endswith(':chat'):
            chunks = output.get('chunks') if input['request'].get('stream') else [output.get('response')]
            if not isinstance(chunks, list) or stream_completion_error(input, chunks):
                raise ValueError('Provider termination is missing or truncated')
        else:
            response = output.get('response')
            data = response.get('data') if isinstance(response, dict) else None
            source = input['request'].get('input')
            expected_count = len(source) if isinstance(source, list) and source and isinstance(source[0], (str, list)) else 1
            if (input['request'].get('stream') or not isinstance(data, list) or len(data) != expected_count
                    or any(not isinstance(row, dict) or type(row.get('index')) is not int or row['index'] != j
                           or not isinstance(row.get('embedding'), list) or not row['embedding']
                           or any(type(v) not in (int, float) or not math.isfinite(v) for v in row['embedding'])
                           for j, row in enumerate(data))):
                raise ValueError('Incomplete native embedding response')
    stage_names = set(runtime['stage_sha256'])
    stage_count = len(stage_names) // 2
    expected_stages = {f'stage-{i:03d}-{side}.json' for i in range(stage_count) for side in ('input', 'output')}
    if stage_names != expected_stages or {n for n in captured if n.startswith('stage-')} != expected_stages:
        raise ValueError('Incomplete native stage capture')
    for i in range(stage_count):
        input = strict_json(captured[f'stage-{i:03d}-input.json'])
        output = strict_json(captured[f'stage-{i:03d}-output.json'])
        if (input.get('index') != i or not isinstance(input.get('name'), str)
                or input['name'] in {'fact_selector', 'fact_exclusion'}
                or any(output.get(k) != input.get(k) for k in ('index', 'name', 'prompt', 'system_prompt', 'response_format'))
                or output.get('exception_type') is not None or not isinstance(output.get('response'), str)):
            raise ValueError('Invalid or retired native stage')
    from app.answer_coverage import restore_question_coverage
    coverage = restore_question_coverage(runtime['final'])
    if (coverage is None or coverage.get('status') not in {'complete', 'partial'}
            or runtime['final'].get('query_plan', {}).get('requirements_status') != 'complete'):
        raise ValueError('Live final planning or coverage cannot be restored')
    screenshots = browser.get('screenshots_sha256')
    if not isinstance(screenshots, dict) or not screenshots:
        raise ValueError('Actual built-browser screenshots required')
    for path, digest in screenshots.items():
        if (path not in captured or not path.endswith('.png')
                or not captured[path].startswith(b'\x89PNG\r\n\x1a\n')
                or sha256(captured[path]) != digest):
            raise ValueError('Browser evidence bytes changed')
    calls, seconds = runtime.get('native_call_count'), runtime.get('elapsed_seconds')
    if (type(calls) is not int or not 0 < calls <= 300 or type(seconds) not in (int, float)
            or not math.isfinite(seconds) or not 0 < seconds <= 3600):
        raise ValueError('Invalid live attempt usage')
    return {'version': 1, 'manifest_sha256': manifest_hash, 'index': index, 'request': request,
            'native_call_count': calls, 'elapsed_seconds': seconds, 'error': None,
            'artifacts_sha256': {name: sha256(payload) for name, payload in captured.items()},
            'sse_sha256': sha256(body)}


def admit_case(manifest, *, index, output, inputs, **preparation):
    """Recompute prerequisites and every earlier reviewed result before new resources."""
    if type(index) is not int or not 0 <= index < 6:
        raise ValueError('One scheduled live case required')
    expected = prepare_manifest(inputs=inputs, **preparation)
    if manifest != expected:
        raise ValueError('Live prerequisites, code or private inputs changed')
    request_payload = (Path(inputs) / 'requests.json').read_bytes()
    if sha256(request_payload) != manifest['private_inputs_sha256']['requests.json']:
        raise ValueError('Request bytes changed after manifest validation')
    requests = parse_requests(request_payload)
    output = Path(output)
    if (output / f'case-{index:02d}').exists():
        raise ValueError('Existing live attempt cannot be resumed or overwritten')
    actual = {path.name for path in output.glob('case-*')}
    if actual != {f'case-{i:02d}' for i in range(index)}:
        raise ValueError('Live attempts differ from the frozen sequence')
    calls, seconds = 0, 0.0
    for prior in range(index):
        directory = output / f'case-{prior:02d}'
        result_payload = (directory / 'result.json').read_bytes()
        result = strict_json(result_payload)
        expected_result = capture_result(directory, manifest, requests[prior], prior)
        if result != expected_result:
            raise ValueError('Prior live result or captured attempt changed')
        reviewed_snapshot(directory, {'result.json': result_payload,
                                      'review.json': (directory / 'review.json').read_bytes()})
        calls += result['native_call_count']; seconds += result['elapsed_seconds']
    if calls >= manifest['max_model_calls'] or seconds >= manifest['active_seconds']:
        raise ValueError('Aggregate live experiment budget exhausted')
    return {'request': requests[index], 'max_calls': manifest['max_model_calls'] - calls,
            'seconds': manifest['active_seconds'] - seconds}
