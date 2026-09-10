"""Execute a frozen document-reader diagnostic; no search, audit or grading."""
import asyncio
import hashlib
import json
import time
import platform
from importlib.metadata import version
from pathlib import Path

from scripts.eval_source_audit import write_private
from scripts.live_query_capture import ModelCapture, ModelCallBudgetExceeded
from scripts.live_query_stages import capture_stages

CASE_ORDER = ('replacement', 'late-application', 'conditional-hours',
              'measured-target', 'posted-credit', 'refund-action')


def schedule():
    return [(repetition, case, arm)
            for repetition, cases, arms in ((1, CASE_ORDER, ('R', 'F')),
                                            (2, reversed(CASE_ORDER), ('F', 'R')))
            for case in cases for arm in arms]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def runtime_identity():
    from app import strands_orchestrator as native
    from app.config import settings
    return {'python':platform.python_version(),
            'packages':{name:version(name) for name in ('strands-agents','openai','httpx')},
            'model':settings.strands_model or settings.gemini_model,
            'enabled':bool(settings.strands_enabled and native.STRANDS_AVAILABLE),
            'provider_route_sha256':hashlib.sha256(settings.litellm_url.encode()).hexdigest(),
            'call_timeout_seconds':settings.strands_call_timeout_seconds,
            'concurrency':settings.strands_max_concurrent_calls,
            'sdk_retries':0, 'agent_retries':None, 'cache':'bypass', 'output_limit':None}


class CaptureIntegrityError(RuntimeError): pass
class ReaderTransportFailure(RuntimeError): pass


class DiagnosticCapture(ModelCapture):
    integrity_error = None

    def write(self, name, payload):
        try:return super().write(name,payload)
        except Exception as exc:
            self.integrity_error = 'capture_write_failed'
            raise CaptureIntegrityError('Capture write failed') from exc

    def start(self, kind, kwargs):
        if time.monotonic() >= self.deadline:raise TimeoutError('Reader invocation expired')
        try:return super().start(kind,kwargs)
        except ModelCallBudgetExceeded as exc:
            if len(self.attempts) < self.max_calls and time.monotonic() >= self.deadline:
                raise TimeoutError('Reader invocation expired') from exc
            self.integrity_error = 'capture_attempt_overrun'
            raise CaptureIntegrityError('Unexpected attempt overrun') from exc
        except Exception as exc:
            self.integrity_error = 'capture_admission_failed'
            raise CaptureIntegrityError('Capture admission failed') from exc



async def owned_call(awaitable, *, deadline=None):
    """Cancel the owned operation once; repeated caller cancellation cannot unjoin it."""
    task = asyncio.create_task(awaitable)
    try:
        async with asyncio.timeout_at(deadline):
            return await asyncio.shield(task)
    except BaseException:
        if not task.done():task.cancel()
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError:continue
            except BaseException:break
        # Retrieve terminal exceptions even if cancellation arrived during cleanup.
        if task.done() and not task.cancelled():task.exception()
        raise



def admission_subject(manifest):
    body = {key:value for key,value in manifest.items() if key != 'review_receipts'}
    return hashlib.sha256(json.dumps(body,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def validate_inputs(manifest, root):
    if (manifest.get('kind') != 'reader-retention-v1'
            or manifest.get('schedule') != [list(row) for row in schedule()]
            or manifest.get('limits') != {'logical_seconds':120, 'total_seconds':3000, 'native_attempts':48}
            or manifest.get('failure_policy') != 'continue_individual_stop_shared'
            or manifest.get('provider_capacity') not in ('verified', 'unknown')
            or manifest.get('review_status') != 'admitted'):
        raise ValueError('Diagnostic admission differs from fixed protocol')
    reviews = manifest.get('review_receipts',[])
    if (len(reviews) != 2 or any(not isinstance(r,dict) for r in reviews)
            or len({r.get('reviewer') for r in reviews}) != 2
            or any(not isinstance(r.get('reviewer'),str) or not r['reviewer'].strip()
                   or r.get('status') != 'approved' or r.get('subject_sha256') != admission_subject(manifest)
                   for r in reviews)):
        raise ValueError('Two independent reviews must bind this exact admission')
    if (manifest.get('protocol') not in manifest['sha256']
            or set(manifest.get('originals',{})) != set(CASE_ORDER)
            or any(name not in manifest['sha256'] for name in manifest['originals'].values())):
        raise ValueError('Originals and protocol must be bound')
    for name, expected in manifest['sha256'].items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or digest(root/path) != expected:
            raise ValueError('Diagnostic input changed')
    if set(manifest.get('payloads',{})) != set(CASE_ORDER):
        raise ValueError('Missing diagnostic cases')
    for arms in manifest['payloads'].values():
        if set(arms) != {'R','F'} or any(name not in manifest['sha256'] for name in arms.values()):
            raise ValueError('Unbound diagnostic payload')
    if not manifest.get('gold') or manifest['gold'] not in manifest['sha256']:
        raise ValueError('Unbound diagnostic gold')
    if not manifest.get('runtime') or not manifest.get('code_sha256'):
        raise ValueError('Frozen runtime and code required')
    return manifest


async def run(manifest_path, output):
    """Observe runtime and source hashes before opening an exclusive output package."""
    manifest_path = Path(manifest_path); root = manifest_path.parent
    manifest = validate_inputs(json.loads(manifest_path.read_text()), root)
    code_root = Path(__file__).resolve().parents[1]
    observed = {}
    for name in manifest['code_sha256']:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts:raise ValueError('Invalid code path')
        observed[name] = digest(code_root/path)
    # Bind the entire application and diagnostic dependencies, not an empty or
    # hand-selected subset that could omit the executable under review.
    required = {str(p.relative_to(code_root)) for folder in ('app','scripts')
                for p in (code_root/folder).glob('*.py')} | {'requirements.lock'}
    if set(observed) != required:raise ValueError('Incomplete candidate code inventory')
    if runtime_identity() != manifest['runtime'] or observed != manifest['code_sha256']:
        raise ValueError('Runtime or candidate changed')
    from app.strands_orchestrator import StrandsQueryOrchestrator
    factory = StrandsQueryOrchestrator
    output = Path(output); output.mkdir(mode=0o700, exist_ok=False)
    write_private(output/'manifest.json', manifest)
    started = time.monotonic(); deadline = started + 3000; calls = 0; rows = []; stopped = None
    try:
        for index, (repetition, case, arm) in enumerate(schedule()):
            if time.monotonic() >= deadline or calls >= 48:
                stopped = 'aggregate_budget_exhausted'; break
            # Integrity is rechecked before each invocation, never after spending a call.
            validate_inputs(manifest, root)
            payload_name = manifest['payloads'][case][arm]
            payload_bytes = (root/payload_name).read_bytes()
            payload_hash = hashlib.sha256(payload_bytes).hexdigest()
            if payload_hash != manifest['sha256'][payload_name]:raise ValueError('Dispatch payload changed')
            payload = json.loads(payload_bytes)
            directory = output/f'invocation-{index:02d}'; directory.mkdir(mode=0o700)
            capture = DiagnosticCapture(directory, max_calls=min(2,48-calls),
                                   seconds=min(120,deadline-time.monotonic()))
            row = {'index':index, 'repetition':repetition, 'case':case, 'arm':arm,
                   'status':'failed', 'payload_sha256':payload_hash}
            rows.append(row); orchestrator = factory(); stages = None
            try:
                if not orchestrator.enabled: raise ValueError('Reader runtime unavailable')
                with capture_stages(orchestrator,capture,directory,reader_inventory=True) as stages:
                    from scripts.eval_source_coverage import validate_native_call
                    captured_text = orchestrator._text_agent
                    async def qualified_text(*args, **kwargs):
                        before = len(capture.attempts)
                        text = await captured_text(*args, **kwargs)
                        if capture.integrity_error:raise CaptureIntegrityError(capture.integrity_error)
                        try:validate_native_call(capture, stages, before)
                        except ValueError as exc:raise ReaderTransportFailure('Nonterminal or non-text reader attempt') from exc
                        return text
                    orchestrator._text_agent = qualified_text
                    result = await owned_call(orchestrator.read_question_sources(payload), deadline=capture.deadline)
                if capture.integrity_error:raise CaptureIntegrityError(capture.integrity_error)
                if capture.pending or capture.failures or capture.exhausted or not capture.attempts:
                    row['error']='incomplete_transport'
                else:
                    write_private(directory/'reading.json',result); row['status']='completed'
            except (asyncio.CancelledError, KeyboardInterrupt):
                row['error']='cancelled'; raise
            except Exception as exc:
                row['error']=type(exc).__name__
                # Model protocol/transport errors belong to this cell. Capture and
                # integrity errors invalidate continuation; do not disguise them.
                if capture.integrity_error:raise CaptureIntegrityError(capture.integrity_error) from exc
                from app.source_reading import SourceReadingError
                import httpx
                from openai import OpenAIError
                if not isinstance(exc,(SourceReadingError,ReaderTransportFailure,TimeoutError,httpx.HTTPError,OpenAIError)):
                    raise
            finally:
                # Account for requests before any cleanup/artifact operation that
                # could itself fail. A broken output cannot erase a spent call.
                calls += len(capture.attempts)
                row['native_attempts'] = len(capture.attempts)
                try:
                    await owned_call(orchestrator.close())
                    capture.close_pending()
                except BaseException as exc:
                    row.update(status='failed',error=type(exc).__name__,cleanup_failed=True)
                    raise
                finally:
                    row.update(model_sha256=dict(capture.hashes),
                               stage_sha256=dict(stages['hashes']) if stages else {})
                    write_private(directory/'outcome.json',row)
    except BaseException as exc:
        stopped = type(exc).__name__
        raise
    finally:
        remaining = [dict(index=i,repetition=rep,case=case,arm=arm,status='not_run')
                     for i,(rep,case,arm) in enumerate(schedule()) if i >= len(rows)]
        write_private(output/'run.json',{'status':'stopped' if stopped else 'completed',
            'stop_reason':stopped,'rows':rows+remaining,'native_attempts':calls,
            'elapsed_seconds':time.monotonic()-started,'semantic_grade':'not_graded'})
