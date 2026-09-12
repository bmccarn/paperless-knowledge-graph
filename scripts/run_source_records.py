"""Admitted original-record comparison. No retrieval, production routing or retries."""
import asyncio
import hashlib
import json
from pathlib import Path
import time

from app.question_evidence import QuestionEvidence, canonical_json
from app.source_records import SourceRecordInventory
from app.source_record_reader import read_records, project_reading, requests
from scripts.eval_source_audit import write_private
from scripts.run_reader_retention import admission_subject, CASE_ORDER, owned_call
from scripts import run_source_recovery as recovery

LIMITS = {'pair_seconds': 3600, 'total_seconds': 14400, 'pair_attempts': 256, 'native_attempts': 1536}
STAGES = frozenset({'source_record_reader', 'source_auditor'})
IntegrityFailure = recovery.IntegrityFailure
PairFailure = recovery.PairFailure


def schedule():
    return [{**row, 'order': ['baseline', 'reader', 'records'] if row['repetition'] == 1
             else ['reader', 'records', 'baseline']} for row in recovery.schedule()]


def freeze(manifest, root):
    """Verify and retain the same bytes subsequently consumed, before awaited work."""
    frozen = {}
    for name, expected in manifest['sha256'].items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts:
            raise IntegrityFailure('Unbound input path')
        raw = (Path(root)/path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise IntegrityFailure('Input identity changed')
        frozen[name] = raw
    return frozen


def bound(frozen, name):
    if name not in frozen: raise IntegrityFailure('Unbound diagnostic input')
    return json.loads(frozen[name])


def prepared_inputs(pair, original):
    request, primary, previous, plan = recovery.prepared_inputs(pair)
    empty = {'documents': [{'document_id': d['document_id'], 'observations': [], 'limitations': []}
                           for d in request['source_documents']]}
    evidence = QuestionEvidence(previous._snapshot, canonical_json(empty))
    inventory = SourceRecordInventory.build([{'document_id': original['id'], 'content': original['content'],
        'content_digest': hashlib.sha256(original['content'].encode()).hexdigest()}])
    question = {k: request[k] for k in ('question', 'resolved_question', 'requirements', 'evaluated_at', 'source_date_order')}
    # Binding an all-unresolved inventory checks complete source projection before dispatch.
    unresolved = {'blocks': [{'block_id': b['block_id'], 'status': 'unresolved', 'observations': [],
        'reason': 'Admission coverage check only'} for d in inventory.record['documents'] for b in d['blocks']]}
    anchor = hashlib.sha256(canonical_json(request['source_documents']).encode()).hexdigest()
    project_reading(inventory.bind_reading(canonical_json(unresolved)), request['source_documents'], documents_digest=anchor)
    requests(inventory, question)
    return request, primary, evidence, plan, inventory, question, anchor


def validate_sources(manifest, frozen):
    if len(manifest.get('pairs', [])) != 12:
        raise IntegrityFailure('Incomplete comparison schedule')
    for key in ('originals', 'requests'):
        if set(manifest.get(key, {})) != set(CASE_ORDER): raise IntegrityFailure('Incomplete source inventory')
    b2 = bound(frozen, manifest['b2_manifest'])
    previous = bound(frozen, manifest['b2_run'])
    inventory = bound(frozen, manifest['b2_inventory'])
    if manifest['sha256'].get(manifest['gold']) != b2['sha256'][b2['gold']]:
        raise IntegrityFailure('Gold changed from frozen B2')
    for row, expected in zip(manifest['pairs'], schedule()):
        if any(row.get(k) != v for k, v in expected.items()): raise IntegrityFailure('Pair schedule changed')
        case = row['case']
        for key, old in [('requests', b2['payloads'][case]['F']), ('originals', b2['originals'][case])]:
            if manifest['sha256'].get(manifest[key][case]) != b2['sha256'][old]:
                raise IntegrityFailure('Frozen B2 source changed')
        matches = [r for r in previous['rows'] if r['case'] == case and r['repetition'] == row['repetition'] and r['arm'] == 'F']
        if (len(matches) != 1 or matches[0]['status'] != 'completed' or matches[0]['index'] != row['b2_index']
                or manifest['sha256'].get(row['primary']) != inventory.get(f"invocation-{row['b2_index']:02d}/reading.json")):
            raise IntegrityFailure('Frozen B2 primary changed')
        pair = bound(frozen, row['input'])
        if (set(pair) != {'request', 'primary', 'evidence_pack'}
                or pair['request'] != bound(frozen, manifest['requests'][case])
                or pair['primary'] != bound(frozen, row['primary'])):
            raise IntegrityFailure('Pair differs from frozen input')
        prepared_inputs(pair, bound(frozen, manifest['originals'][case]))


def known_wires(manifest, frozen, index):
    """Admit complete known and synthetic dynamic stages before creating clients."""
    from app.answer_observations import ObservationCandidate
    report = bound(frozen, manifest['preflight'])
    if (report.get('scope') != 'source-record comparison SDK localhost MockTransport only'
            or type(report.get('native_model_calls')) is not int or report['native_model_calls'] != 0
            or len(report.get('rows', [])) != 12 or report.get('code_sha256') != manifest['code_sha256']
            or report.get('runtime', {}).get('packages') != manifest['runtime']['packages']
            or report.get('model') != manifest['runtime']['model']
            or report.get('input_manifest_sha256') != manifest['sha256'].get('manifest-prepared.json')):
        raise IntegrityFailure('Preflight identity changed')
    binding = manifest['pairs'][index]; row = report['rows'][index]
    if (row.get('index') != index or any(row.get(k) != binding[k] for k in ('case', 'repetition'))):
        raise IntegrityFailure('Preflight schedule changed')
    pair = bound(frozen, binding['input'])
    request, primary, _, _, inventory, question, anchor = prepared_inputs(
        pair, bound(frozen, manifest['originals'][binding['case']]))
    if row.get('inventory_digest') != inventory.digest or row.get('documents_digest') != anchor:
        raise IntegrityFailure('Preflight source identity changed')
    base = f'preflight/pair-{index:02d}/'
    artifacts = {}
    for field in ('model_sha256', 'stage_sha256'):
        if not isinstance(row.get(field), dict) or not row[field]:
            raise IntegrityFailure('Missing preflight capture inventory')
        for name, expected in row[field].items():
            if Path(name).name != name or manifest['sha256'].get(base+name) != expected:
                raise IntegrityFailure('Unbound preflight capture artifact')
            artifacts[name] = bound(frozen, base+name)
    def artifact(name):
        if name not in artifacts: raise IntegrityFailure('Missing preflight capture artifact')
        return artifacts[name]
    stages = artifact('stages.json')
    if set(stages) != set(binding['order']) or any(r.get('status') != 'completed' for r in stages.values()):
        raise IntegrityFailure('Incomplete preflight stages')
    records = artifact('source-records.json')
    if records.get('status') != 'completed' or records.get('pending_block_ids'):
        raise IntegrityFailure('Incomplete preflight source records')
    parsed = inventory.bind_reading(canonical_json({'blocks': records['rows']}))
    projection = project_reading(parsed, request['source_documents'], documents_digest=anchor)
    if artifact('source-projection.json') != projection.record:
        raise IntegrityFailure('Preflight projection changed')
    def units(reading):
        return ObservationCandidate.from_response({'observations': [o['text'] for d in reading['documents']
                                                    for o in d['observations']]}).units()
    expected_units = {'baseline': units(primary), 'records': units(projection.reading)}
    expected_batches = {arm: [values[i:i+4] for i in range(0, len(values), 4)] for arm, values in expected_units.items()}
    seen_readers, seen_batches, known, bodies = [], {'baseline': [], 'records': []}, [], []
    wires = sorted((v for k, v in artifacts.items() if k.startswith('wire-')), key=lambda w: w['model_index'])
    if (not wires or len(wires) != row.get('requests')
            or [w['model_index'] for w in wires] != list(range(len(wires)))):
        raise IntegrityFailure('Incomplete preflight native schedule')
    for wire in wires:
        raw = wire['body'].encode()
        if len(raw) != wire['bytes'] or hashlib.sha256(raw).hexdigest() != wire['sha256']:
            raise IntegrityFailure('Preflight body changed')
        body = json.loads(raw); model_index, stage_index = wire['model_index'], wire['stage_index']
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
        operation = wire['operation']
        if operation == 'reader':
            if stage['name'] != 'source_record_reader': raise IntegrityFailure('Wrong preflight reader stage')
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
    if (seen_readers != requests(inventory, question) or row.get('reader_requests') != len(seen_readers)
            or row.get('baseline_requests') != len(seen_batches['baseline'])
            or set(row.get('known_wires', [])) != set(known) or len(row['known_wires']) != len(known)):
        raise IntegrityFailure('Incomplete known preflight bodies')
    for arm, batches in expected_batches.items():
        if sorted(map(canonical_json, seen_batches[arm])) != sorted(map(canonical_json, batches)):
            raise IntegrityFailure('Incomplete preflight audit batches')
        artifact(f'{arm}-audit-00.json')
    return bodies


def validate_manifest(manifest, frozen):
    if (manifest.get('kind') != 'source-record-comparison-v1' or manifest.get('status') != 'admitted'
            or manifest.get('limits') != LIMITS or manifest.get('schedule') != schedule()
            or manifest.get('failure_policy') != 'stop_pair_continue_controls_stop_shared'
            or manifest.get('provider_capacity') != 'unknown'):
        raise IntegrityFailure('Unreviewed diagnostic contract')
    receipts = manifest.get('review_receipts', [])
    if (len(receipts) != 2 or len({r.get('reviewer') for r in receipts}) != 2
            or any(not isinstance(r.get('reviewer'), str) or not r['reviewer'].strip()
                or r.get('status') != 'approved' or r.get('subject_sha256') != admission_subject(manifest) for r in receipts)):
        raise IntegrityFailure('Two exact independent admission receipts required')
    for key in ('protocol', 'gold', 'b2_manifest', 'b2_run', 'b2_inventory', 'b2_grade_spec',
                'b2_grade_standards', 'b2_adjudication', 'preflight'):
        if manifest.get(key) not in frozen: raise IntegrityFailure('Unbound diagnostic provenance')
    if manifest.get('runtime') != recovery.runtime_identity() or manifest.get('code_sha256') != recovery.code_identity():
        raise IntegrityFailure('Runtime or code changed')
    validate_sources(manifest, frozen)
    for index in range(12): known_wires(manifest, frozen, index)


class RecordCapture(recovery.RecoveryCapture):
    def is_preflight_known(self, payload):
        return self.operation == 'reader' or (self.operation == 'baseline'
            and self.audit_generation == 0 and payload.get('protocol_correction') is None)


async def execute_pair(pair, original, order, orchestrator, directory, capture):
    request, primary, evidence, plan, inventory, question, anchor = prepared_inputs(pair, original)
    result = {arm: {'status': 'not_run'} for arm in order}
    projection = None
    try:
        for number, arm in enumerate(order):
            capture.write(f'stage-{number}-pending.json', {'stage': arm, 'status': 'pending'})
            result[arm] = {'status': 'running'}
            capture.operation, capture.audit_generation = arm, None
            if arm == 'reader':
                reading = await read_records(inventory, question, orchestrator, deadline=capture.deadline)
                capture.write('source-records.json', reading.record)
                if reading.record['status'] != 'completed': raise PairFailure('Source record reading incomplete')
                projection = project_reading(reading.bound_reading, request['source_documents'], documents_digest=anchor)
                capture.write('source-projection.json', projection.record)
                details = {'receipt': reading.bound_reading.record['receipt']}
            else:
                selected = primary if arm == 'baseline' else projection.reading
                details = await recovery.audit(selected, pair, evidence, plan, orchestrator, directory, arm, capture)
            result[arm] = {'status': 'completed', **details}
    except BaseException as exc:
        for arm in result:
            if result[arm]['status'] == 'running': result[arm] = {'status': 'failed', 'exception_type': type(exc).__name__}
        raise
    finally:
        capture.write('stages.json', result)
    return result


async def run(manifest_path, output):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app.answer_observations import ObservationValidationError
    import httpx
    from openai import OpenAIError
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    frozen = freeze(manifest, manifest_path.parent)
    validate_manifest(manifest, frozen)
    output = Path(output); output.mkdir(mode=0o700, exist_ok=False)
    write_private(output/'manifest.json', manifest)
    started = time.monotonic(); deadline = started + LIMITS['total_seconds']
    rows, calls, stopped = [], 0, None
    try:
        for index, binding in enumerate(manifest['pairs']):
            if time.monotonic() >= deadline or calls >= LIMITS['native_attempts']:
                stopped = 'aggregate_budget_exhausted'; break
            if recovery.runtime_identity() != manifest['runtime'] or recovery.code_identity() != manifest['code_sha256']:
                raise IntegrityFailure('Runtime or code changed')
            pair = bound(frozen, binding['input'])
            original = bound(frozen, manifest['originals'][binding['case']])
            expected_wires = known_wires(manifest, frozen, index)
            admitted_at = time.monotonic()
            if admitted_at >= deadline:
                stopped = 'aggregate_budget_exhausted'; break
            pair_deadline = min(deadline, admitted_at + LIMITS['pair_seconds'])
            directory = output/f'pair-{index:02d}'; directory.mkdir(mode=0o700)
            row = {'index': index, **binding, 'status': 'running'}; rows.append(row)
            write_private(directory/'pending.json', row)
            capture = RecordCapture(directory, max_calls=min(LIMITS['pair_attempts'], LIMITS['native_attempts']-calls),
                seconds=pair_deadline-admitted_at)
            capture.deadline = pair_deadline
            capture.expected_wires = expected_wires
            native = StrandsQueryOrchestrator(); stages = None
            try:
                with recovery.native_capture(native, capture, directory, allowed_stages=STAGES) as stages:
                    row['stages'] = await owned_call(execute_pair(pair, original, binding['order'], native,
                        directory, capture), deadline=capture.deadline)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
                capture.require_complete(); row['status'] = 'completed'
            except BaseException as exc:
                row.update(status='failed', exception_type=type(exc).__name__)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error) from exc
                if not isinstance(exc, (PairFailure, TimeoutError, recovery.ModelCallBudgetExceeded,
                                        ObservationValidationError, httpx.HTTPError, OpenAIError)):
                    raise
            finally:
                calls += len(capture.attempts); row['native_attempts'] = len(capture.attempts)
                try:
                    await owned_call(native.close()); capture.close_pending()
                except BaseException:
                    row.update(status='failed', cleanup_failed=True); raise
                finally:
                    row.update(model_sha256=dict(capture.hashes), wire_sha256=dict(capture.wires),
                               stage_sha256=dict(stages['hashes']) if stages else {})
                    write_private(directory/'outcome.json', row)
    except BaseException as exc:
        stopped = type(exc).__name__
        for row in rows:
            if row['status'] == 'running': row.update(status='failed', exception_type=type(exc).__name__)
        raise
    finally:
        remaining = [dict(index=i, **b, status='not_run') for i, b in enumerate(manifest['pairs']) if i >= len(rows)]
        write_private(output/'run.json', {'status': 'stopped' if stopped else 'completed', 'stop_reason': stopped,
            'rows': rows+remaining, 'native_attempts': calls, 'elapsed_seconds': time.monotonic()-started,
            'semantic_grade': 'pending'})
