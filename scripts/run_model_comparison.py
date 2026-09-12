"""Frozen two-route diagnostic; no production configuration or query activation."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import time

from app.question_evidence import QuestionEvidence, canonical_json
from app.source_reading import parse_reading
from scripts.eval_source_audit import write_private
from scripts.run_reader_retention import admission_subject, CASE_ORDER, owned_call
from scripts import run_source_recovery as recovery
from scripts.run_source_records import freeze, bound, prepared_inputs as original_inputs

ROUTES = ('gemini-3.8-flash', 'paperless-brain-deep-review')
STAGES = frozenset({'source_reader', 'source_auditor'})
IntegrityFailure = recovery.IntegrityFailure
PairFailure = recovery.PairFailure


def schedule():
    rows = []
    for trial in (1, 2, 3):
        cases = list(CASE_ORDER)
        if trial == 2: cases.reverse()
        if trial == 3: cases = cases[3:] + cases[:3]
        for index, case in enumerate(cases):
            routes = ROUTES[::-1] if trial == 2 or (trial == 3 and index % 2) else ROUTES
            for route in routes:
                rows.append({'case': case, 'trial': trial, 'route': route,
                    'order': ['reader', 'fresh', 'baseline'] if trial == 2 else ['baseline', 'reader', 'fresh']})
    return rows


@contextmanager
def route_profile(route):
    from app.config import settings
    if route not in ROUTES: raise IntegrityFailure('Unreviewed model route')
    changes = {'strands_model': route, 'strands_call_timeout_seconds': 180.0,
               'answer_audit_timeout_seconds': 1800.0}
    previous = {k: getattr(settings, k) for k in changes}
    try:
        for key, value in changes.items(): setattr(settings, key, value)
        yield
    finally:
        for key, value in previous.items(): setattr(settings, key, value)


def execution_inputs(manifest, frozen, binding):
    case = binding['case']
    pair = bound(frozen, manifest['cases'][case]['input'])
    original = bound(frozen, manifest['originals'][case])
    original_inputs(pair, original)  # Complete source coordinate and evidence identity check.
    return pair, original


def validate_sources(manifest, frozen):
    from scripts import run_source_records as records
    legacy = bound(frozen, manifest['source_manifest'])
    records.validate_sources(legacy, frozen)
    if set(manifest.get('cases', {})) != set(CASE_ORDER): raise IntegrityFailure('Incomplete cases')
    if manifest.get('executions') != schedule(): raise IntegrityFailure('Execution schedule changed')
    appendix = bound(frozen, manifest['appendix'])
    if set(appendix) != set(CASE_ORDER): raise IntegrityFailure('Incomplete appendix inventory')
    for case in CASE_ORDER:
        old = [r for r in legacy['pairs'] if r['case'] == case]
        variants = [bound(frozen, r['input']) for r in sorted(old, key=lambda r: r['repetition'])]
        pair, _ = execution_inputs(manifest, frozen, {'case': case})
        expected = json.loads(canonical_json(variants[0]))
        documents = expected['primary']['documents']
        if len(documents) != 1: raise IntegrityFailure('Diagnostic requires one full original per case')
        documents[0]['observations'] += variants[1]['primary']['documents'][0]['observations']
        documents[0]['observations'] += appendix[case]
        if pair != expected: raise IntegrityFailure('Baseline occurrences or source context changed')
        parse_reading(canonical_json(pair['primary']), pair['request']['source_documents'])
        origins = bound(frozen, manifest['cases'][case]['origins'])
        expected_origins = []
        for row, variant in zip(sorted(old, key=lambda r: r['repetition']), variants):
            expected_origins += [{'kind': 'b2', 'b2_index': row['b2_index'], 'ordinal': i,
                                  'observation': observation} for i, observation in enumerate(
                                      variant['primary']['documents'][0]['observations'])]
        expected_origins += [{'kind': 'appendix', 'ordinal': i, 'observation': observation}
                             for i, observation in enumerate(appendix[case])]
        for i, origin in enumerate(expected_origins): origin['unit_id'] = f'u{i+1}'
        if origins != expected_origins: raise IntegrityFailure('Occurrence origin sidecar changed')
    labels = bound(frozen, manifest['appendix_labels'])
    for case in CASE_ORDER:
        entries = labels.get(case, [])
        if len(entries) != len(appendix[case]) or any(e.get('observation') != o or
                e.get('status') not in {'supported', 'unsupported', 'uncertain', 'calendar_policy'}
                for e, o in zip(entries, appendix[case])):
            raise IntegrityFailure('Appendix labels do not bind exact occurrences')


class ComparisonCapture(recovery.RecoveryCapture):
    def is_preflight_known(self, payload):
        return self.operation == 'reader' or (self.operation == 'baseline'
            and self.audit_generation == 0 and payload.get('protocol_correction') is None)


async def execute_pair(pair, original, order, orchestrator, directory, capture):
    request, primary, evidence, plan, *_ = original_inputs(pair, original)
    return await execute_stages(pair, request, primary, evidence, plan, order, orchestrator, directory, capture)


async def execute_stages(pair, request, primary, evidence, plan, order, orchestrator, directory, capture, *, source_scope=None):
    result = {arm: {'status': 'not_run'} for arm in order}
    fresh = None
    try:
        for number, arm in enumerate(order):
            capture.write(f'stage-{number}-pending.json', {'stage': arm, 'status': 'pending'})
            result[arm] = {'status': 'running'}
            capture.operation, capture.audit_generation = arm, None
            if arm == 'reader':
                if source_scope is None:
                    fresh = await orchestrator.read_question_sources(request)
                else:
                    from app.source_scopes import ScopedReading
                    result_reading = await orchestrator.read_question_sources(request, source_scope=source_scope)
                    if not isinstance(result_reading, ScopedReading):
                        raise IntegrityFailure('Missing scoped reading receipt')
                    result_reading.validate(source_scope, request['source_documents'])
                    capture.write('reading-scope-receipt.json', result_reading.receipt)
                    fresh = result_reading.reading
                fresh = parse_reading(canonical_json(fresh), request['source_documents'])
                capture.write('reading.json', fresh)
                details = {}
            else:
                from app.config import settings
                stage_deadline = min(capture.deadline, time.monotonic() + settings.answer_audit_timeout_seconds)
                details = await owned_call(recovery.audit(primary if arm == 'baseline' else fresh, pair,
                    evidence, plan, orchestrator, directory, arm, capture), deadline=stage_deadline)
            result[arm] = {'status': 'completed', **details}
    except BaseException as exc:
        for arm in result:
            if result[arm]['status'] == 'running': result[arm] = {'status': 'failed', 'exception_type': type(exc).__name__}
        raise
    finally:
        capture.write('stages.json', result)
    return result


def preflight_artifacts(manifest, frozen, row, base):
    artifacts = {}
    for field in ('model_sha256', 'stage_sha256'):
        if not isinstance(row.get(field), dict) or not row[field]:
            raise IntegrityFailure('Missing preflight capture inventory')
        for name, expected in row[field].items():
            if Path(name).name != name or manifest['sha256'].get(base+name) != expected:
                raise IntegrityFailure('Unbound preflight capture artifact')
            artifacts[name] = bound(frozen, base+name)
    return artifacts


def preflight_wire(wire, artifact, route):
    raw = wire['body'].encode()
    if len(raw) != wire['bytes'] or hashlib.sha256(raw).hexdigest() != wire['sha256']:
        raise IntegrityFailure('Preflight body changed')
    body = json.loads(raw)
    if body.get('model') != route: raise IntegrityFailure('Preflight model route changed')
    model_index, stage_index = wire['model_index'], wire['stage_index']
    model = artifact(f'model-{model_index:03d}-input.json')
    expected = dict(model['request']); expected.update(expected.pop('extra_body', {}))
    if body != expected or model.get('kind') != f'stage-{stage_index:03d}:chat':
        raise IntegrityFailure('Preflight SDK ownership changed')
    stage = artifact(f'stage-{stage_index:03d}-input.json')
    if (artifact(f'model-{model_index:03d}-output.json').get('status') != 'completed'
            or artifact(f'stage-{stage_index:03d}-output.json').get('native_result', {}).get('stop_reason') != 'end_turn'):
        raise IntegrityFailure('Incomplete preflight native result')
    content = body['messages'][-1]['content']
    payload = json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))
    if payload != json.loads(stage['prompt']): raise IntegrityFailure('Preflight stage payload changed')
    return body, payload, stage


def known_wires(manifest, frozen, index):
    """Admit complete known and synthetic dynamic stages before creating clients."""
    from app.answer_observations import ObservationCandidate
    report = bound(frozen, manifest['preflight'])
    if (report.get('scope') != 'model comparison SDK localhost MockTransport only'
            or type(report.get('native_model_calls')) is not int or report['native_model_calls'] != 0
            or len(report.get('rows', [])) != len(schedule()) or report.get('code_sha256') != manifest['code_sha256']
            or report.get('runtimes', {}).get(manifest['executions'][index]['route'], {}).get('packages') != manifest['runtimes'][manifest['executions'][index]['route']]['packages']
            or report.get('input_manifest_sha256') != manifest['sha256'].get('manifest-prepared.json')):
        raise IntegrityFailure('Preflight identity changed')
    binding = manifest['executions'][index]; row = report['rows'][index]
    if (row.get('index') != index or any(row.get(k) != binding[k] for k in ('case', 'trial', 'route'))):
        raise IntegrityFailure('Preflight schedule changed')
    pair, original = execution_inputs(manifest, frozen, binding)
    request, primary, _, _, _, question, _ = original_inputs(pair, original)
    base = f'preflight/execution-{index:02d}/'
    artifacts = preflight_artifacts(manifest, frozen, row, base)
    def artifact(name):
        if name not in artifacts: raise IntegrityFailure('Missing preflight capture artifact')
        return artifacts[name]
    stages = artifact('stages.json')
    if set(stages) != set(binding['order']) or any(r.get('status') != 'completed' for r in stages.values()):
        raise IntegrityFailure('Incomplete preflight stages')
    fresh = artifact('reading.json')
    parse_reading(canonical_json(fresh), request['source_documents'])
    def units(reading):
        return ObservationCandidate.from_response({'observations': [o['text'] for d in reading['documents']
                                                    for o in d['observations']]}).units()
    expected_units = {'baseline': units(primary), 'fresh': units(fresh)}
    expected_batches = {arm: [values[i:i+4] for i in range(0, len(values), 4)] for arm, values in expected_units.items()}
    seen_readers, seen_batches, known, bodies = [], {'baseline': [], 'fresh': []}, [], []
    wires = sorted((v for k, v in artifacts.items() if k.startswith('wire-')), key=lambda w: w['model_index'])
    if (not wires or len(wires) != row.get('requests')
            or [w['model_index'] for w in wires] != list(range(len(wires)))):
        raise IntegrityFailure('Incomplete preflight native schedule')
    for wire in wires:
        body, payload, stage = preflight_wire(wire, artifact, binding['route'])
        model_index = wire['model_index']
        operation = wire['operation']
        if operation == 'reader':
            if stage['name'] != 'source_reader': raise IntegrityFailure('Wrong preflight reader stage')
            seen_readers.append(payload); known.append(base+f'wire-{model_index:03d}-input.json'); bodies.append(body)
        elif operation in expected_batches:
            if (stage['name'] != 'source_auditor' or wire['audit_generation'] != 0
                    or payload.get('protocol_correction') is not None
                    or payload.get('source_documents') != request['source_documents']
                    or any(payload.get(k) != question[k] for k in question)
                    or payload.get('source_reading') != {'documents': [{'document_id': d['document_id'],
                        'observations': [], 'limitations': []} for d in request['source_documents']]}):
                raise IntegrityFailure('Preflight audit context changed')
            seen_batches[operation].append(payload['units'])
            if operation == 'baseline':
                known.append(base+f'wire-{model_index:03d}-input.json'); bodies.append(body)
        else: raise IntegrityFailure('Unexpected preflight operation')
    first = {**question, 'source_documents': request['source_documents']}
    corrected = {**first, 'reading_protocol_correction': {
        'error': 'invalid_source_reading',
        'instruction': 'Re-read these unchanged originals and return the required structure '
                       'using only this document ID and its exact supplied span_id references. '
                       'This corrects the response protocol, not a requested factual verdict.'}}
    if (seen_readers != [first, corrected] or row.get('reader_requests') != len(seen_readers)
            or row.get('baseline_requests') != len(seen_batches['baseline'])
            or set(row.get('known_wires', [])) != set(known) or len(row['known_wires']) != len(known)):
        raise IntegrityFailure('Incomplete known preflight bodies')
    for arm, batches in expected_batches.items():
        if sorted(map(canonical_json, seen_batches[arm])) != sorted(map(canonical_json, batches)):
            raise IntegrityFailure('Incomplete preflight audit batches')
        artifact(f'{arm}-audit-00.json')
    return bodies


def validate_admission(manifest, frozen, *, kind, artifacts):
    if (manifest.get('kind') != kind or manifest.get('status') != 'admitted'
            or manifest.get('failure_policy') != 'stop_pair_continue_controls_stop_shared'
            or manifest.get('code_sha256') != recovery.code_identity()):
        raise IntegrityFailure('Unreviewed model comparison contract')
    limits = manifest.get('limits', {})
    if set(limits) != {'pair_seconds', 'total_seconds', 'pair_attempts', 'native_attempts'} or any(
            type(v) is not int or v <= 0 for v in limits.values()):
        raise IntegrityFailure('Explicit positive budgets required')
    receipts = manifest.get('review_receipts', [])
    if (len(receipts) != 2 or len({r.get('reviewer') for r in receipts}) != 2
            or any(not isinstance(r.get('reviewer'), str) or not r['reviewer'].strip()
                or r.get('status') != 'approved' or r.get('subject_sha256') != admission_subject(manifest) for r in receipts)):
        raise IntegrityFailure('Two exact independent admission receipts required')
    for key in artifacts:
        bound(frozen, manifest[key]) if key != 'protocol' else frozen[manifest[key]]
    destinations = bound(frozen, manifest['destinations'])
    if set(destinations) != set(ROUTES) or any(not d.get('provider') or not d.get('model')
            or not d.get('destination') for d in destinations.values()):
        raise IntegrityFailure('Exact route destinations required')
    for route in ROUTES:
        with route_profile(route):
            if recovery.runtime_identity() != manifest['runtimes'][route]: raise IntegrityFailure('Runtime changed')


def validate_manifest(manifest, frozen):
    validate_admission(manifest, frozen, kind='reader-verifier-model-v1', artifacts=(
        'protocol', 'source_manifest', 'appendix', 'appendix_labels', 'preflight', 'destinations'))
    validate_sources(manifest, frozen)
    for index in range(len(schedule())): known_wires(manifest, frozen, index)


async def run(manifest_path, output):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app.answer_observations import ObservationValidationError
    from app.source_reading import SourceReadingError
    import httpx
    from openai import OpenAIError
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    frozen = freeze(manifest, manifest_path.parent)
    scoped = None
    prepared = None
    if manifest.get('kind') == 'source-scope-diagnostic-v1':
        from scripts import source_scope_diagnostic as scoped
        prepared = scoped.prepare(manifest, frozen)
        scoped.validate_admission(manifest, frozen, prepared)
    else:
        validate_manifest(manifest, frozen)
    output = Path(output); output.mkdir(mode=0o700, exist_ok=False)
    write_private(output/'manifest.json', manifest)
    started = time.monotonic(); limits = manifest['limits']; deadline = started + limits['total_seconds']
    rows, calls, stopped = [], 0, None
    try:
        for index, binding in enumerate(manifest['executions']):
            if time.monotonic() >= deadline or calls >= limits['native_attempts']:
                stopped = 'aggregate_budget_exhausted'; break
            if recovery.code_identity() != manifest['code_sha256']:
                raise IntegrityFailure('Code changed')
            if scoped is None:
                pair, original = execution_inputs(manifest, frozen, binding)
                expected_wires = known_wires(manifest, frozen, index)
            else:
                expected_wires = scoped.known_wires(manifest, frozen, prepared[index])
            admitted_at = time.monotonic()
            if admitted_at >= deadline:
                stopped = 'aggregate_budget_exhausted'; break
            pair_deadline = min(deadline, admitted_at + limits['pair_seconds'])
            directory = output/f'execution-{index:02d}'; directory.mkdir(mode=0o700)
            row = {'index': index, **binding, 'status': 'running'}; rows.append(row)
            write_private(directory/'pending.json', row)
            capture = ComparisonCapture(directory, max_calls=min(limits['pair_attempts'], limits['native_attempts']-calls),
                seconds=pair_deadline-admitted_at)
            capture.deadline = pair_deadline
            capture.expected_wires = expected_wires
            profile = route_profile(binding['route'])
            profile.__enter__()
            native = None; stages = None
            try:
                if recovery.runtime_identity() != manifest['runtimes'][binding['route']]:
                    raise IntegrityFailure('Runtime changed')
                native = StrandsQueryOrchestrator()
                with recovery.native_capture(native, capture, directory, allowed_stages=STAGES) as stages:
                    execution = (execute_pair(pair, original, binding['order'], native, directory, capture)
                                 if scoped is None else scoped.execute(prepared[index], native, capture, directory))
                    row['stages'] = await owned_call(execution, deadline=capture.deadline)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
                capture.require_complete(); row['status'] = 'completed'
            except BaseException as exc:
                row.update(status='failed', exception_type=type(exc).__name__)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error) from exc
                if not isinstance(exc, (PairFailure, TimeoutError, recovery.ModelCallBudgetExceeded,
                                        ObservationValidationError, SourceReadingError, httpx.HTTPError, OpenAIError)):
                    raise
            finally:
                calls += len(capture.attempts); row['native_attempts'] = len(capture.attempts)
                try:
                    if native is not None: await owned_call(native.close())
                    capture.close_pending()
                except BaseException:
                    row.update(status='failed', cleanup_failed=True); raise
                finally:
                    profile.__exit__(None, None, None)
                    row.update(model_sha256=dict(capture.hashes), wire_sha256=dict(capture.wires),
                               stage_sha256=dict(stages['hashes']) if stages else {})
                    write_private(directory/'outcome.json', row)
    except BaseException as exc:
        stopped = type(exc).__name__
        for row in rows:
            if row['status'] == 'running': row.update(status='failed', exception_type=type(exc).__name__)
        raise
    finally:
        remaining = [dict(index=i, **b, status='not_run') for i, b in enumerate(manifest['executions']) if i >= len(rows)]
        write_private(output/'run.json', {'status': 'stopped' if stopped else 'completed', 'stop_reason': stopped,
            'rows': rows+remaining, 'native_attempts': calls, 'elapsed_seconds': time.monotonic()-started,
            'semantic_grade': 'pending'})
