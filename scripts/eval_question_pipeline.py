#!/usr/bin/env python3
"""One native development case per invocation, with independent review between cases."""
import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.eval_source_audit import (CURRENT_ATTEMPT, AttemptLog, configure_proxy_cache,
                                      evidence_pack, runtime_snapshot, write_private)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def load_data(payload):
    data = json.loads(payload)
    if (data.get('version') != 1 or data.get('partition') != 'development'
            or data.get('synthetic_only') is not True or len(data.get('cases', [])) != 12):
        raise ValueError('Reviewed synthetic development dataset required')
    if len({c['id'] for c in data['cases']}) != 12:
        raise ValueError('Duplicate case identity')
    for case in data['cases']:
        if not case['required_aspects'] or not case['forbidden_inferences']:
            raise ValueError('Independent rubric required')
        evidence_pack(case)
    return data


MODES = ('quick', 'deep', 'timeline', 'strict')
EXTENSION_PATHS = {
    'scripts/eval_question_pipeline.py',
    'docs/specs/question-pipeline-development-evaluation.md',
    'docs/specs/question-all-mode-evaluation.md',
}


def manifest_for(dataset, *, payload=None, stage='initial', initial_output=None, conservative_admission=None):
    payload = dataset.read_bytes() if payload is None else payload
    data = load_data(payload)
    if stage not in {'initial', 'all-modes'}:
        raise ValueError('Unknown evaluation stage')
    paths = sorted((ROOT / 'app').glob('*.py')) + [ROOT / name for name in (
        'scripts/eval_source_audit.py', 'scripts/eval_question_pipeline.py', 'requirements.lock',
        'scripts/conservative_query_admission.py', 'docs/specs/question-reader-inventory.md',
        'docs/specs/question-pipeline-development-evaluation.md',
        'docs/specs/question-coverage-recovery.md',
        'docs/specs/question-fact-conservation-integration.md',
        'docs/specs/question-exclusion-authority.md')]
    if stage == 'all-modes':
        paths.append(ROOT / 'docs/specs/question-all-mode-evaluation.md')
    if conservative_admission is not None:
        paths.append(ROOT / 'docs/specs/question-conservative-coverage-admission.md')
    runtime = runtime_snapshot()
    if runtime['packages']['strands-agents'] != '1.55.0':
        raise ValueError('Qualification requires locked Strands 1.55.0 runtime')
    manifest = {'version': 1, 'grading_version': 2, 'fact_filtering': 'disabled', 'stage': 'fixed_originals_question_pipeline_development',
        'dataset_sha256': digest(payload),
        'code_sha256': {str(p.relative_to(ROOT)): digest(p.read_bytes()) for p in paths},
        'runtime': runtime, 'mode': 'strict', 'cases': 12, 'max_native_calls': 300,
        'elapsed_seconds': 1800, 'estimated_total_tokens': 2000000,
        'sdk_retry_policy': 'single_attempt', 'proxy_cache_policy': 'bypass',
        'retrieval': 'fixed case originals; live retrieval not evaluated',
        'grading': 'independent original-source review of each case before continuing'}
    if conservative_admission is not None:
        from scripts.conservative_query_admission import load_admission
        manifest['conservative_admission'] = load_admission(conservative_admission,
            code_sha256=manifest['code_sha256'], runtime=runtime,
            policy_bytes=(ROOT / 'docs/specs/question-conservative-coverage-admission.md').read_bytes())
    if stage == 'all-modes':
        manifest.update(mode='all-modes', cases=48, max_native_calls=900,
                        elapsed_seconds=3600, estimated_total_tokens=5000000,
                        schedule=[{'case_id': case['id'], 'mode': mode}
                                  for case in data['cases'] for mode in MODES])
        manifest['initial_admission'] = initial_admission(initial_output, manifest, data['cases'])
    return manifest


def request_identity(manifest, case, index, mode):
    identity = {'manifest': manifest, 'case': case['id']}
    if manifest.get('mode') == 'all-modes':
        identity.update(stage=manifest['stage'], schedule_index=index, mode=mode)
    return digest(json.dumps(identity, sort_keys=True).encode())


def read_run(directory, *, bind_attempts=False, bind_grades=False):
    # Each byte snapshot supplies both validation and hashing. Never re-open an
    # artifact after grading it within one admission operation.
    names = ['manifest.json', 'case.json', 'result.json', 'review.json']
    if bind_grades:
        names.extend(('grade-spec.json', 'grade-standards.json'))
    snapshot = {name: (directory / name).read_bytes() for name in names}
    result = json.loads(snapshot['result.json'])
    count = result.get('native_call_count')
    if type(count) is not int or not 1 <= count <= 900:
        raise ValueError('Invalid recorded attempt count')
    expected = {f'attempt-{index:03d}-{kind}.json'
                for index in range(count) for kind in ('input', 'output')}
    if {p.name for p in directory.glob('attempt-*.json')} != expected:
        raise ValueError('Missing or extra raw attempt artifacts')
    for name in sorted(expected):
        snapshot[name] = (directory / name).read_bytes()
        attempt = json.loads(snapshot[name])
        if attempt.get('index') != int(name.split('-')[1]):
            raise ValueError('Raw attempt identity mismatch')
    if bind_attempts and result.get('attempt_sha256') != {name: digest(snapshot[name]) for name in sorted(expected)}:
        raise ValueError('Raw attempt bytes differ from reviewed result')
    return snapshot


def validate_scheduled_result(result, manifest, case, index, mode):
    final = result['final']
    plan = final.get('query_plan', {})
    expected = request_identity(manifest, case, index, mode)
    recorded_mode = result.get('mode', 'strict' if manifest.get('mode') == 'strict' else None)
    if (recorded_mode != mode or final.get('mode') != mode
            or plan.get('mode') != mode or final.get('question') != case['question']
            or plan.get('original_question') != case['question']
            or plan.get('request_identity_digest') != expected
            or final.get('finalization', {}).get('request_identity_digest') != expected):
        raise ValueError('Scheduled request identity mismatch')
    if mode == 'timeline':
        from app.timeline import restore_timeline
        if restore_timeline(final)[1]['status'] not in {'ready', 'no_dates'}:
            raise ValueError('Invalid Timeline projection')


def initial_admission(root, current, cases):
    if root is None:
        raise ValueError('All-mode execution requires the passing initial slice')
    bound_grades = current.get('grading_version') == 2 or 'conservative_admission' in current
    snapshots = [read_run(root / f'case-{i:02d}', bind_attempts=True, bind_grades=True)
                 if bound_grades else read_run(root / f'case-{i:02d}') for i in range(12)]
    original = json.loads(snapshots[0]['manifest.json'])
    expected = {key: value for key, value in current.items()
                if key not in {'code_sha256', 'schedule', 'initial_admission'}}
    expected.update(mode='strict', cases=12, max_native_calls=300,
                    elapsed_seconds=1800, estimated_total_tokens=2000000)
    if {key: value for key, value in original.items() if key != 'code_sha256'} != expected:
        raise ValueError('Initial slice differs from the full frozen Strict contract')
    previous_code = original.get('code_sha256', {})
    current_code = current['code_sha256']
    if ({k: v for k, v in previous_code.items() if k not in EXTENSION_PATHS}
            != {k: v for k, v in current_code.items() if k not in EXTENSION_PATHS}):
        raise ValueError('Initial candidate code differs outside reviewed extension')
    calls, elapsed = previous_runs(root, 12, original, cases, snapshots=snapshots, modes=['strict'] * 12)
    if calls > original['max_native_calls'] or elapsed > original['elapsed_seconds']:
        raise ValueError('Initial slice exceeded its frozen budget')
    return {f'case-{i:02d}/{name}': digest(payload)
            for i, snapshot in enumerate(snapshots) for name, payload in snapshot.items()}


def previous_runs(output, case_index, manifest, cases, *, snapshots=None, modes=None):
    from app.answer_coverage import restore_question_coverage
    calls, elapsed = 0, 0.0
    for index in range(case_index):
        directory = output / f'case-{index:02d}'
        if snapshots is not None:
            snapshot = snapshots[index]
        elif manifest.get('grading_version') == 2 or 'conservative_admission' in manifest:
            snapshot = read_run(directory, bind_attempts=True, bind_grades=True)
        elif manifest.get('mode') == 'all-modes':
            snapshot = read_run(directory, bind_attempts=True)
        else:
            snapshot = {name: (directory / name).read_bytes()
                        for name in ('result.json', 'review.json', 'manifest.json', 'case.json')}
        result_bytes = snapshot['result.json']
        result = json.loads(result_bytes)
        review = json.loads(snapshot['review.json'])
        coverage = restore_question_coverage(result.get('final'))
        if (json.loads(snapshot['manifest.json']) != manifest
                or json.loads(snapshot['case.json']) != cases[index]
                or result.get('manifest_sha256') != digest(json.dumps(manifest, sort_keys=True).encode())
                or type(result.get('native_call_count')) is not int or result['native_call_count'] < 1
                or type(result.get('elapsed_seconds')) not in (int, float)
                or not math.isfinite(result['elapsed_seconds']) or result['elapsed_seconds'] < 0
                or result.get('case_index') != index or result.get('case_id') != cases[index]['id']
                or not isinstance(result.get('final'), dict)
                or not isinstance(coverage, dict) or coverage.get('status') not in {'complete', 'partial'}
                or coverage.get('planning_status') != 'complete'
                or review.get('result_sha256') != digest(result_bytes)
                or review.get('spec') != 'pass' or review.get('standards') != 'pass'
                or result['error'] is not None):
            raise ValueError('Prior case lacks passing independent review')
        if manifest.get('grading_version') == 2 or 'conservative_admission' in manifest:
            from scripts.conservative_query_admission import validate_case_grades
            validate_case_grades(snapshot, result, review)
        if modes is not None or manifest.get('grading_version') == 2 or 'conservative_admission' in manifest:
            mode = (modes[index] if modes is not None else
                    manifest['schedule'][index]['mode'] if manifest.get('mode') == 'all-modes' else 'strict')
            validate_scheduled_result(result, manifest, cases[index], index, mode)
        calls += result['native_call_count']
        elapsed += result['elapsed_seconds']
    return calls, elapsed


async def execute(dataset, manifest, output, case_index, *, initial_output=None, conservative_admission=None):
    payload = dataset.read_bytes()
    stage = 'all-modes' if manifest.get('mode') == 'all-modes' else 'initial'
    if manifest != manifest_for(dataset, payload=payload, stage=stage, initial_output=initial_output,
                                conservative_admission=conservative_admission):
        raise ValueError('Frozen code, dataset or runtime changed')
    data = load_data(payload)
    cases = data['cases'] if stage == 'initial' else [case for case in data['cases'] for _ in MODES]
    modes = ['strict'] * 12 if stage == 'initial' else list(MODES) * 12
    if type(case_index) is not int or not 0 <= case_index < len(cases):
        raise ValueError('Invalid case index')
    prior_calls, prior_elapsed = previous_runs(output, case_index, manifest, cases,
        modes=modes if stage == 'all-modes' or manifest.get('grading_version') == 2 or 'conservative_admission' in manifest else None)
    remaining_calls = manifest['max_native_calls'] - prior_calls
    remaining_seconds = manifest['elapsed_seconds'] - prior_elapsed
    if remaining_calls <= 0 or remaining_seconds <= 0:
        raise ValueError('Development budget exhausted')
    from app import strands_orchestrator as native_module
    from app.config import settings
    from app.question_evidence import validate_requirements, coarse_requirements, PIPELINE_VERSION
    from app.question_pipeline import finalize_question
    from app.query_quality import merge_agent_plan
    from app.query_metrics import CURRENT_QUERY_METRICS, QueryMetrics
    case, mode = cases[case_index], modes[case_index]
    if settings.source_date_order != case['source_date_order']:
        raise ValueError('Case source date order differs from runtime')
    directory = output / f'case-{case_index:02d}'
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    write_private(directory / 'case.json', case)
    write_private(directory / 'manifest.json', manifest)
    orchestrator = native_module.StrandsQueryOrchestrator(audit_strategy='document_local_corrected')
    if not orchestrator.enabled:
        raise ValueError('Native orchestrator disabled')
    attempts = []
    attempt_hashes = {}

    def write_attempt(name, attempt):
        path = directory / name
        write_private(path, attempt)
        attempt_hashes[name] = digest(path.read_bytes())

    native_text, native_model, native_agent = orchestrator._text_agent, orchestrator._model, native_module.Agent

    async def captured_text(name, system_prompt, prompt, *, response_format=None):
        if len(attempts) >= remaining_calls:
            raise RuntimeError('Native call budget exhausted')
        attempt = {'index': len(attempts), 'name': name, 'system_prompt': system_prompt,
                   'prompt': prompt, 'response_format': response_format, 'diagnostics': []}
        attempts.append(attempt)
        write_attempt(f"attempt-{attempt['index']:03d}-input.json", attempt)
        token = CURRENT_ATTEMPT.set(attempt)
        started = time.monotonic()
        try:
            if manifest.get('fact_filtering') == 'disabled' and name in {'fact_selector', 'fact_exclusion'}:
                raise ValueError('Reader-inventory evaluation forbids semantic filtering stages')
            attempt['response'] = await native_text(name, system_prompt, prompt, response_format=response_format)
            return attempt['response']
        except BaseException as exc:
            attempt['exception_type'] = type(exc).__name__
            raise
        finally:
            attempt['elapsed_seconds'] = time.monotonic() - started
            write_attempt(f"attempt-{attempt['index']:03d}-output.json", attempt)
            CURRENT_ATTEMPT.reset(token)

    class CapturedAgent(native_agent):
        def __init__(self, *args, **kwargs):
            kwargs['retry_strategy'] = None
            super().__init__(*args, **kwargs)

        async def invoke_async(self, *args, **kwargs):
            result = await super().invoke_async(*args, **kwargs)
            attempt = CURRENT_ATTEMPT.get()
            if attempt is not None:
                attempt['native_result'] = {'text': str(result), 'message': result.message,
                                            'stop_reason': result.stop_reason}
            return result

    native_module.Agent = CapturedAgent
    orchestrator._text_agent = captured_text
    orchestrator._model = lambda **kwargs: configure_proxy_cache(native_model(**kwargs), 'bypass')
    handler = AttemptLog()
    logger = logging.getLogger('app.strands_orchestrator')
    prior_level = logger.level
    logger.setLevel(logging.INFO); logger.addHandler(handler)
    metrics = QueryMetrics(); metrics_token = CURRENT_QUERY_METRICS.set(metrics)
    started = time.monotonic(); final = None; error = None
    try:
        async with asyncio.timeout(remaining_seconds):
            native_plan = await orchestrator.plan_query(case['question'], mode, include_requirements=True)
            try:
                requirements = validate_requirements({k: native_plan[k] for k in ('resolved_question', 'requirements')})
                planning_status = 'complete'
            except (TypeError, KeyError, ValueError):
                requirements = coarse_requirements(case['question']); planning_status = 'coarse'
            plan = merge_agent_plan(requirements['resolved_question'], mode, native_plan or {})
            plan.update(requirements, original_question=case['question'], evaluated_at=case['evaluated_at'],
                source_date_order=case['source_date_order'], requirements_status=planning_status,
                pipeline_version=PIPELINE_VERSION,
                request_identity_digest=request_identity(manifest, case, case_index, mode))
            final = await finalize_question(orchestrator, case['question'], evidence_pack(case), plan, mode)
            final.update(query_plan=plan, question=case['question'], mode=mode)
    except (Exception, asyncio.CancelledError) as exc:
        error = type(exc).__name__
    finally:
        elapsed = time.monotonic() - started
        await orchestrator.close()
        CURRENT_QUERY_METRICS.reset(metrics_token)
        native_module.Agent = native_agent
        logger.removeHandler(handler); logger.setLevel(prior_level)
    result = {'manifest_sha256': digest(json.dumps(manifest, sort_keys=True).encode()),
        'case_id': case['id'], 'case_index': case_index, 'mode': mode, 'final': final, 'error': error,
        'attempt_sha256': attempt_hashes,
        'elapsed_seconds': elapsed, 'native_call_count': len(attempts), 'execution': metrics.report(),
        'usage': {key: sum(a['usage'][key] for a in attempts)
            if attempts and all(key in a.get('usage', {}) for a in attempts) else None
            for key in ('inputTokens', 'outputTokens', 'totalTokens')},
        'usage_reporting_attempts': sum(bool(a.get('usage')) for a in attempts),
        'upstream_transport_attempts': 'unknown', 'output_token_cap': None,
        'grading_status': 'pending_independent_review', 'passed': None,
        'not_started_cases': len(cases) - case_index - 1}
    write_private(directory / 'result.json', result)
    return {'case_index': case_index, 'native_call_count': len(attempts), 'error': error,
            'grading_status': result['grading_status']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset', type=Path)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--case-index', type=int, default=0)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--stage', choices=('initial', 'all-modes'), default='initial')
    parser.add_argument('--initial-output', type=Path)
    parser.add_argument('--conservative-admission', type=Path)
    args = parser.parse_args()
    if not args.execute:
        write_private(args.manifest, manifest_for(args.dataset, stage=args.stage,
                      initial_output=args.initial_output, conservative_admission=args.conservative_admission))
        return
    if args.output is None:
        parser.error('--execute requires --output')
    print(json.dumps(asyncio.run(execute(args.dataset, json.loads(args.manifest.read_bytes()),
                                         args.output, args.case_index, initial_output=args.initial_output,
                                         conservative_admission=args.conservative_admission))))


if __name__ == '__main__':
    main()
