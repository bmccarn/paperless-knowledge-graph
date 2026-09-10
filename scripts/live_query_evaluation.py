"""Admission and request isolation for the inactive live-query experiment.

Importing this module opens no clients. Native execution is deliberately separate
from prerequisite validation and the ASGI route boundary.
"""
import asyncio
import hashlib
import json
import re
from pathlib import Path


FAILURE_COUNTS = (
    'raw_false_approvals', 'delivered_false_approvals', 'missing_required_aspects',
    'false_complete_coverage', 'unsupported_extras', 'false_exclusion_approvals',
)
AXES = ('spec', 'standards')


def sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def reviewed_snapshot(directory, snapshot):
    """Bind both original-source grades to the already captured result bytes."""
    captured = dict(snapshot)
    result_hash = sha256(captured['result.json'])
    review = json.loads(captured['review.json'])
    for axis in AXES:
        name = f'grade-{axis}.json'
        captured[name] = (directory / name).read_bytes()
        grade = json.loads(captured[name])
        if (grade.get('verdict') != 'pass' or grade.get('result_sha256') != result_hash
                or review.get(axis) != 'pass'
                or review.get('grade_sha256', {}).get(axis) != sha256(captured[name])):
            raise ValueError('Independent grade is missing or differs from reviewed bytes')
        for key in FAILURE_COUNTS:
            if type(grade.get(key)) is not int or grade[key] != 0:
                raise ValueError('Independent grade records a failure')
            if type(review.get(key)) is not int or review[key] != 0:
                raise ValueError('Aggregate review records a failure')
        underreported = grade.get('coverage_underreported_aspects')
        if (type(underreported) is not int or underreported < 0
                or review.get('coverage_underreported_aspects') != underreported):
            raise ValueError('Coverage review disagreement')
    if review.get('result_sha256') != result_hash:
        raise ValueError('Aggregate review belongs to a different result')
    reviewers = review.get('reviewers')
    if (not isinstance(reviewers, list) or len(reviewers) != 2
            or any(not isinstance(v, str) or not v.strip() for v in reviewers)
            or len(set(reviewers)) != 2):
        raise ValueError('Two independent reviewer identities required')
    return captured


def admit_all_modes(dataset, initial_output, all_mode_output):
    """Read-only admission; a partial or differently configured run cannot pass."""
    from scripts.eval_question_pipeline import (
        MODES, load_data, manifest_for, previous_runs, read_run,
    )
    dataset_bytes = Path(dataset).read_bytes()
    data = load_data(dataset_bytes)
    root = Path(all_mode_output)
    manifest_bytes = (root / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    expected = manifest_for(Path(dataset), payload=dataset_bytes, stage='all-modes',
                            initial_output=Path(initial_output))
    if manifest != expected:
        raise ValueError('All-mode candidate, runtime or prerequisite artifacts changed')
    cases = [case for case in data['cases'] for _ in MODES]
    snapshots = []
    for index in range(48):
        directory = root / f'case-{index:02d}'
        snapshots.append(reviewed_snapshot(directory, read_run(directory, bind_attempts=True)))
    calls, seconds = previous_runs(root, 48, manifest, cases, snapshots=snapshots,
                                   modes=list(MODES) * 12)
    if calls > manifest['max_native_calls'] or seconds > manifest['elapsed_seconds']:
        raise ValueError('All-mode experiment exceeded its admitted budget')
    inventory = {'manifest.json': sha256(manifest_bytes)}
    for index, snapshot in enumerate(snapshots):
        inventory.update({f'case-{index:02d}/{name}': sha256(payload)
                          for name, payload in snapshot.items()})
    return {'manifest': manifest, 'artifacts_sha256': inventory,
            'native_calls': calls, 'active_seconds': seconds}


class UnscheduledRequest(ValueError):
    pass


class SingleRequest:
    """Admit one exact engine invocation, including resolved conversation history."""
    def __init__(self, *, question, mode, history, model):
        self.expected = json.dumps({'question': question, 'mode': mode,
                                    'history': history or [], 'model': model}, sort_keys=True)
        self.started = False
        self._lock = asyncio.Lock()

    async def invoke(self, operation, *, question, mode, history, model):
        actual = json.dumps({'question': question, 'mode': mode,
                             'history': history or [], 'model': model}, sort_keys=True)
        async with self._lock:
            if actual != self.expected or self.started:
                raise UnscheduledRequest('Only the single frozen request is admitted')
            self.started = True
        # Errors and cancellation consume the attempt too; never retry implicitly.
        return await operation()


class EvaluationRoutes:
    """Default-deny ASGI boundary around the real application route handlers.

    Adapters own UI metadata, titles and private conversations. They cannot fall
    through to the serving app. Only explicitly admitted query routes reach it;
    SingleRequest must also guard the engine so an allowed path is not a new run.
    """
    def __init__(self, app, *, adapters, query_paths=('/query/stream',)):
        if not set(query_paths) <= {'/query', '/query/stream'}:
            raise ValueError('Only reviewed query routes may reach the application')
        for method, path in adapters:
            metadata = method == 'GET' and path in {
                '/_fixture', '/models', '/config', '/status', '/tags', '/conversations',
            }
            private_write = method == 'POST' and path in {'/conversations', '/generate-title'}
            private_conversation = (method in {'GET', 'PATCH', 'DELETE'}
                                    and re.fullmatch(r'/conversations/[A-Za-z0-9-]+', path))
            captured_source = method == 'GET' and re.fullmatch(r'/documents/[0-9]+', path)
            if not (metadata or private_write or private_conversation or captured_source):
                raise ValueError('Unreviewed adapter route')
        self.app = app
        self.adapters = dict(adapters)
        self.query_paths = frozenset(query_paths)

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            raise ValueError('Only HTTP is admitted; serving startup is disabled')
        server = scope.get('server') or ()
        if not server or server[0] not in {'127.0.0.1', '::1'}:
            await self.reject(send, 403)
            return
        key = (scope['method'], scope['path'])
        adapter = self.adapters.get(key)
        if adapter is not None:
            await adapter(scope, receive, send)
        elif scope['method'] == 'POST' and scope['path'] in self.query_paths:
            await self.app(scope, receive, send)
        else:
            await self.reject(send, 403)

    @staticmethod
    async def reject(send, status):
        await send({'type': 'http.response.start', 'status': status,
                    'headers': [(b'content-type', b'application/json')]})
        await send({'type': 'http.response.body',
                    'body': b'{"detail":"Route not admitted for evaluation"}'})
