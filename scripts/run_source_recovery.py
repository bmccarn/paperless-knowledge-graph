"""Frozen document-local recovery comparison; no retrieval or live activation."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
import copy
import hashlib
import json
from pathlib import Path
import time

from scripts.eval_source_audit import CURRENT_ATTEMPT, write_private
from scripts.live_query_capture import ModelCapture, ModelCallBudgetExceeded
from scripts.live_query_stages import capture_stages
from scripts.run_reader_retention import CASE_ORDER, admission_subject, digest, owned_call, runtime_identity as reader_runtime_identity

LIMITS = {'pair_seconds': 300, 'total_seconds': 3600, 'pair_attempts': 32, 'native_attempts': 384}


def runtime_identity():
    from app.config import settings
    return {**reader_runtime_identity(), 'audit_timeout_seconds': settings.answer_audit_timeout_seconds,
            'audit_batch_size': 4, 'audit_mode': 'strict', 'repairer': None, 'allow_subset': True}


class IntegrityFailure(RuntimeError): pass
class PairFailure(RuntimeError): pass


def schedule():
    return [{'repetition': rep, 'case': case, 'order': list(order)}
            for rep, cases, order in ((1, CASE_ORDER, ('baseline', 'recovery', 'combined')),
                                     (2, reversed(CASE_ORDER), ('recovery', 'combined', 'baseline')))
            for case in cases]


def code_identity():
    root = Path(__file__).resolve().parents[1]
    return {str(p.relative_to(root)): digest(p) for p in sorted([
        *root.glob('app/*.py'), *root.glob('scripts/*.py'), root/'requirements.lock'])}


def read_bound(root, name, hashes):
    path = Path(name)
    if path.is_absolute() or '..' in path.parts or name not in hashes:
        raise IntegrityFailure('Unbound input path')
    raw = (root/path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != hashes[name]:
        raise IntegrityFailure('Input identity changed')
    return json.loads(raw)


def validate_manifest(manifest, root):
    if (manifest.get('kind') != 'source-recovery-v1' or manifest.get('limits') != LIMITS
            or manifest.get('schedule') != schedule() or manifest.get('status') != 'admitted'
            or manifest.get('failure_policy') != 'stop_pair_continue_controls_stop_shared'
            or manifest.get('provider_capacity') not in {'unknown', 'verified'}
            or len(manifest.get('pairs', [])) != 12):
        raise IntegrityFailure('Unreviewed diagnostic contract')
    receipts = manifest.get('review_receipts', [])
    if (len(receipts) != 2 or len({r.get('reviewer') for r in receipts}) != 2
            or any(not isinstance(r.get('reviewer'), str) or not r['reviewer'].strip()
                   or r.get('status') != 'approved' or r.get('subject_sha256') != admission_subject(manifest)
                   for r in receipts)):
        raise IntegrityFailure('Independent admission receipts required')
    hashes = manifest['sha256']
    for key in ('protocol', 'gold', 'b2_manifest', 'b2_run', 'b2_inventory',
                'b2_grade_spec', 'b2_grade_standards', 'b2_adjudication', 'preflight'):
        name = manifest.get(key)
        if name not in hashes: raise IntegrityFailure('Unbound diagnostic provenance')
    for name in hashes:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or digest(root/path) != hashes[name]:
            raise IntegrityFailure('Input inventory changed')
    for row, expected in zip(manifest['pairs'], schedule()):
        if any(row.get(k) != v for k, v in expected.items()) or row.get('input') not in hashes:
            raise IntegrityFailure('Pair input or schedule mismatch')
    from scripts.run_reader_retention import CASE_ORDER
    if set(manifest.get('requests', {})) != set(CASE_ORDER) or set(manifest.get('originals', {})) != set(CASE_ORDER):
        raise IntegrityFailure('Incomplete original request inventory')
    b2 = read_bound(root, manifest['b2_manifest'], hashes)
    previous = read_bound(root, manifest['b2_run'], hashes)
    inventory = read_bound(root, manifest['b2_inventory'], hashes)
    if hashes[manifest['gold']] != b2['sha256'][b2['gold']]:
        raise IntegrityFailure('Gold differs from frozen B2')
    for row in manifest['pairs']:
        case = row['case']
        request_name, original_name = manifest['requests'][case], manifest['originals'][case]
        if (hashes.get(request_name) != b2['sha256'][b2['payloads'][case]['F']]
                or hashes.get(original_name) != b2['sha256'][b2['originals'][case]]):
            raise IntegrityFailure('B2 original or request changed')
        matches = [r for r in previous['rows'] if r['case'] == case and r['repetition'] == row['repetition'] and r['arm'] == 'F']
        if (len(matches) != 1 or matches[0]['index'] != row.get('b2_index') or matches[0]['status'] != 'completed'
                or hashes.get(row.get('primary')) != inventory.get(f"invocation-{row['b2_index']:02d}/reading.json")):
            raise IntegrityFailure('B2 primary identity changed')
        pair = read_bound(root, row['input'], hashes)
        if (set(pair) != {'request', 'primary', 'evidence_pack'}
                or pair['request'] != read_bound(root, request_name, hashes)
                or pair['primary'] != read_bound(root, row['primary'], hashes)):
            raise IntegrityFailure('Pair reconstructed frozen input incorrectly')
        prepared_inputs(pair)
    if manifest.get('runtime') != runtime_identity() or manifest.get('code_sha256') != code_identity():
        raise IntegrityFailure('Runtime or code changed')
    for index in range(12): preflight_wires(manifest, root, index)
    return manifest


def preflight_wires(manifest, root, index):
    preflight = read_bound(root, manifest['preflight'], manifest['sha256'])
    if (preflight.get('scope') != 'SDK localhost MockTransport serialization only'
            or type(preflight.get('native_model_calls')) is not int or preflight['native_model_calls'] != 0
            or len(preflight.get('rows', [])) != 12
            or preflight.get('code_sha256') != manifest['code_sha256']
            or preflight.get('effective_model') != manifest['runtime']['model']
            or preflight.get('serialization_runtime', {}).get('packages') != manifest['runtime']['packages']):
        raise IntegrityFailure('Incomplete wire preflight')
    row = preflight['rows'][index]
    if row.get('index') != index: raise IntegrityFailure('Preflight pair mismatch')
    base = Path(manifest['preflight']).parent/f'pair-{index:02d}'
    bodies, recovery_correction, baseline = [], False, False
    for folder, fields in ((base, ('model_sha256', 'stage_sha256')),
                           (base/'correction', ('correction_model_sha256', 'correction_stage_sha256'))):
        for field in fields:
            for name, expected in row[field].items():
                relative = str(folder/name)
                if manifest['sha256'].get(relative) != expected:
                    raise IntegrityFailure('Unbound preflight artifact')
                artifact = read_bound(root, relative, manifest['sha256'])
                if name.startswith('wire-'):
                    raw = artifact['body'].encode()
                    if len(raw) != artifact['bytes'] or hashlib.sha256(raw).hexdigest() != artifact['sha256']:
                        raise IntegrityFailure('Preflight wire body changed')
                    body = json.loads(raw); bodies.append(body)
                    recovery_correction |= folder.name == 'correction' and artifact['model_index'] == 1
                    baseline |= artifact.get('operation') == 'baseline' and artifact.get('audit_generation') == 0
    if not recovery_correction or not baseline or len(bodies) != row['requests']:
        raise IntegrityFailure('Required preflight stages missing')
    return bodies


def prepared_inputs(pair):
    from app.answer_finalization import evidence_spans
    from app.question_evidence import QuestionEvidence, PIPELINE_VERSION, canonical_json
    from app.source_interpretation import _request
    from app.source_reading import group_sources, parse_reading
    request = _request(pair['request'])
    if not {'resolved_question', 'requirements'} <= set(request):
        raise IntegrityFailure('Frozen planned request fields required')
    spans = evidence_spans(pair['evidence_pack'], citation_safe=True)
    if group_sources(spans) != request['source_documents'] or any(s.get('feedback_open') for s in spans):
        raise IntegrityFailure('Original evidence pack differs from frozen reader input')
    primary = parse_reading(canonical_json(pair['primary']), request['source_documents'])
    # Direct immutable construction is deliberate: prepare() would spend a new reader call.
    evidence = QuestionEvidence(canonical_json({**request, 'pipeline_version': PIPELINE_VERSION,
                                               'conversation_context': ''}), canonical_json(primary))
    plan = {key: request[key] for key in ('evaluated_at', 'source_date_order', 'resolved_question', 'requirements')
            if key in request}
    return request, primary, evidence, plan


class RecoveryCapture(ModelCapture):
    """Capture errors stop the run; configured attempt exhaustion stops its pair."""
    integrity_error = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wires = {}
        self.operation, self.audit_generation = None, None
        self.expected_wires = None

    def write(self, name, payload):
        try: super().write(name, payload)
        except Exception as exc:
            self.integrity_error = 'capture_write_failed'
            raise IntegrityFailure(self.integrity_error) from exc

    def read(self, name):
        try:
            raw = (self.directory/name).read_bytes()
            if hashlib.sha256(raw).hexdigest() != self.hashes.get(name):
                raise ValueError('Capture artifact changed')
            return json.loads(raw)
        except Exception as exc:
            self.integrity_error = 'capture_read_failed'
            raise IntegrityFailure(self.integrity_error) from exc

    def start(self, kind, kwargs):
        if self.integrity_error: raise IntegrityFailure(self.integrity_error)
        if not kind.endswith(':chat'):
            self.integrity_error = 'unexpected_model_operation'
            raise IntegrityFailure(self.integrity_error)
        try: return super().start(kind, kwargs)
        except ModelCallBudgetExceeded: raise
        except Exception as exc:
            self.integrity_error = 'capture_admission_failed'
            raise IntegrityFailure(self.integrity_error) from exc

    async def wire(self, request):
        try:
            stage = CURRENT_ATTEMPT.get()
            matches = [a for a in self.attempts if stage is not None
                       and a['kind'] == f"stage-{stage['index']:03d}:chat"]
            if len(matches) != 1: raise ValueError('Unowned HTTP request')
            attempt = matches[0]; index = attempt['index']
            if index in self.wires: raise ValueError('Unexpected SDK retry')
            raw = request.content
            body = json.loads(raw)
            expected = copy.deepcopy(attempt['request'])
            extra = expected.pop('extra_body', {})
            if set(extra) & set(expected): raise ValueError('Ambiguous native body')
            expected.update(extra)
            if body != expected: raise ValueError('SDK body differs from captured call')
            payload = json.loads(stage['prompt'])
            known = self.operation == 'recovery' or (self.operation == 'baseline'
                and self.audit_generation == 0 and payload.get('protocol_correction') is None)
            if known and self.expected_wires is not None and body not in self.expected_wires:
                raise ValueError('Known SDK body differs from preflight')
            artifact = {'model_index': index, 'stage_index': stage['index'],
                        'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                        'body': raw.decode('utf-8'), 'operation': self.operation,
                        'audit_generation': self.audit_generation}
            self.write(f'wire-{index:03d}-input.json', artifact)
            self.wires[index] = artifact['sha256']
        except Exception as exc:
            self.integrity_error = 'native_wire_integrity'
            raise IntegrityFailure(self.integrity_error) from exc


def validate_call(capture, stages, stage_index):
    """Per-stage validation works while sibling audit batches are in flight."""
    if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
    if capture.exhausted: raise PairFailure('Pair model budget exhausted')
    stage = stages['attempts'][stage_index]
    matches = [a for a in capture.attempts if a['kind'] == f'stage-{stage_index:03d}:chat']
    if len(matches) != 1: raise PairFailure('Stage did not make one native attempt')
    index = matches[0]['index']
    if (index in capture.pending or index in capture.failures or index not in capture.wires
            or stage.get('native_result', {}).get('stop_reason') != 'end_turn'):
        raise PairFailure('Incomplete native execution')
    raw = capture.read(f'model-{index:03d}-output.json')
    for chunk in raw.get('chunks', [raw.get('response', {})]):
        for choice in chunk.get('choices', []):
            body = choice.get('delta', choice.get('message', {}))
            if (body.get('refusal') or body.get('tool_calls') or body.get('function_call')
                    or choice.get('finish_reason') not in (None, 'stop')):
                raise PairFailure('Non-text or nonterminal native execution')


@contextmanager
def native_capture(orchestrator, capture, directory):
    with capture_stages(orchestrator, capture, directory, reader_inventory=True) as stages:
        captured_model, captured_text = orchestrator._model, orchestrator._text_agent
        def model(**kwargs):
            instance = captured_model(**kwargs)
            client_factory = instance._get_client
            @asynccontextmanager
            async def client():
                async with client_factory() as proxy:
                    hooks = proxy.client._client.event_hooks['request']
                    hooks.append(capture.wire)
                    try: yield proxy
                    finally: hooks.remove(capture.wire)
            instance._get_client = client
            return instance
        async def text(name, *args, **kwargs):
            if name not in {'source_omission_review', 'source_auditor'}:
                capture.integrity_error = 'unexpected_model_stage'
                raise IntegrityFailure(capture.integrity_error)
            if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
            index = len(stages['attempts'])
            # capture_stages appends synchronously before its first await.
            try:
                result = await captured_text(name, *args, **kwargs)
            except Exception as exc:
                import httpx
                from openai import OpenAIError
                if not isinstance(exc, (TimeoutError, PairFailure, ModelCallBudgetExceeded, httpx.HTTPError, OpenAIError)):
                    capture.integrity_error = capture.integrity_error or 'stage_capture_failure'
                    raise IntegrityFailure(capture.integrity_error) from exc
                raise
            validate_call(capture, stages, index)
            return result
        orchestrator._model, orchestrator._text_agent = model, text
        try: yield stages
        finally: orchestrator._model, orchestrator._text_agent = captured_model, captured_text


async def audit(reading, pair, evidence, plan, orchestrator, directory, arm, capture):
    from app.answer_finalization import AnswerFinalizer
    from app.answer_observations import ObservationCandidate
    from app.config import settings
    observations = [o['text'] for d in reading['documents'] for o in d['observations']]
    if not observations: raise PairFailure('Empty candidate cannot be audited')
    candidate = ObservationCandidate.from_response({'observations': observations})
    write_private(directory/f'{arm}-candidate.json', {'reading': reading, 'units': candidate.units()})
    finalizer = AnswerFinalizer(evidence.auditor(orchestrator), repairer=None, allow_subset=True,
        timeout_seconds=settings.answer_audit_timeout_seconds,
        concurrency=settings.strands_max_concurrent_calls, date_order=pair['request']['source_date_order'])
    native_audit = finalizer._audit
    receipts = []
    async def observed_audit(*args, **kwargs):
        capture.audit_generation = len(receipts)
        ledger = await native_audit(*args, **kwargs)
        name = f'{arm}-audit-{len(receipts):02d}.json'
        capture.write(name, ledger); receipts.append(name)
        return ledger
    finalizer._audit = observed_audit
    result = await finalizer.finalize(pair['request']['question'], candidate,
        copy.deepcopy(pair['evidence_pack']), plan=copy.deepcopy(plan), mode='strict',
        evaluated_at=pair['request']['evaluated_at'])
    write_private(directory/f'{arm}-final.json', result)
    if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
    if (capture.exhausted or capture.failures or capture.pending
            or result['finalization']['disposition'] in {'audit_failed', 'incomplete', 'timeout', 'unaudited'}):
        raise PairFailure('Audit execution did not complete')
    return {'audit_ledgers': receipts, 'disposition': result['finalization']['disposition']}


async def execute_pair(pair, order, orchestrator, directory, capture):
    from app.source_interpretation import recover
    request, primary, evidence, plan = prepared_inputs(pair)
    result = {arm: {'status': 'not_run'} for arm in order}
    recovered = None
    try:
        for number, arm in enumerate(order):
            write_private(directory/f'stage-{number}-pending.json', {'stage': arm, 'status': 'pending'})
            result[arm] = {'status': 'running'}
            capture.operation, capture.audit_generation = arm, None
            if arm == 'recovery':
                recovered = await recover(request, primary, orchestrator)
                write_private(directory/'recovery.json', recovered.record)
                if not recovered.record['receipt']['execution_complete']:
                    raise PairFailure('Recovery execution incomplete')
                details = {'receipt': recovered.record['receipt']}
            else:
                reading = primary if arm == 'baseline' else recovered.reading
                details = await audit(reading, pair, evidence, plan, orchestrator, directory, arm, capture)
            result[arm] = {'status': 'completed', **details}
    except BaseException as exc:
        for arm in result:
            if result[arm]['status'] == 'running':
                result[arm] = {'status': 'failed', 'exception_type': type(exc).__name__}
        raise
    finally:
        write_private(directory/'stages.json', result)
    return result


async def run(manifest_path, output):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app.source_reading import SourceReadingError
    from app.answer_observations import ObservationValidationError
    import httpx
    from openai import OpenAIError
    root = Path(manifest_path).parent
    manifest = validate_manifest(json.loads(Path(manifest_path).read_bytes()), root)
    output = Path(output); output.mkdir(mode=0o700, exist_ok=False)
    write_private(output/'manifest.json', manifest)
    started = time.monotonic(); deadline = started + LIMITS['total_seconds']
    rows, calls, stopped = [], 0, None
    try:
        for index, binding in enumerate(manifest['pairs']):
            if time.monotonic() >= deadline or calls >= LIMITS['native_attempts']:
                stopped = 'aggregate_budget_exhausted'; break
            validate_manifest(manifest, root)
            pair = read_bound(root, binding['input'], manifest['sha256'])
            prepared_inputs(pair)  # Reject source/request drift before any model call.
            directory = output/f'pair-{index:02d}'; directory.mkdir(mode=0o700)
            row = {'index': index, **binding, 'status': 'running'}; rows.append(row)
            write_private(directory/'pending.json', row)
            capture = RecoveryCapture(directory, max_calls=min(LIMITS['pair_attempts'], LIMITS['native_attempts'] - calls),
                seconds=min(LIMITS['pair_seconds'], deadline-time.monotonic()))
            capture.expected_wires = preflight_wires(manifest, root, index)
            orchestrator = StrandsQueryOrchestrator(); stages = None
            try:
                with native_capture(orchestrator, capture, directory) as stages:
                    row['stages'] = await owned_call(execute_pair(pair, binding['order'], orchestrator,
                        directory, capture), deadline=capture.deadline)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error)
                capture.require_complete()
                row['status'] = 'completed'
            except BaseException as exc:
                row.update(status='failed', exception_type=type(exc).__name__)
                if capture.integrity_error: raise IntegrityFailure(capture.integrity_error) from exc
                if not isinstance(exc, (PairFailure, TimeoutError, ModelCallBudgetExceeded,
                                        SourceReadingError, ObservationValidationError, httpx.HTTPError, OpenAIError)):
                    raise
            finally:
                calls += len(capture.attempts); row['native_attempts'] = len(capture.attempts)
                try:
                    await owned_call(orchestrator.close()); capture.close_pending()
                except BaseException:
                    row.update(status='failed', cleanup_failed=True); raise
                finally:
                    row.update(model_sha256=dict(capture.hashes), wire_sha256=dict(capture.wires),
                               stage_sha256=dict(stages['hashes']) if stages else {})
                    write_private(directory/'outcome.json', row)
    except BaseException as exc:
        stopped = type(exc).__name__; raise
    finally:
        remaining = [dict(index=i, **b, status='not_run') for i, b in enumerate(manifest['pairs']) if i >= len(rows)]
        write_private(output/'run.json', {'status': 'stopped' if stopped else 'completed',
            'stop_reason': stopped, 'rows': rows + remaining, 'native_attempts': calls,
            'elapsed_seconds': time.monotonic()-started, 'semantic_grade': 'pending'})
