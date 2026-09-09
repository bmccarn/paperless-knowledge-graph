#!/usr/bin/env python3
"""One native development case per invocation, with independent review between cases."""
import argparse
import asyncio
import hashlib
import json
import logging
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


def load_data(path):
    data = json.loads(path.read_bytes())
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


def manifest_for(dataset):
    load_data(dataset)
    paths = sorted((ROOT / 'app').glob('*.py')) + [ROOT / name for name in (
        'scripts/eval_source_audit.py', 'scripts/eval_question_pipeline.py', 'requirements.lock',
        'docs/specs/question-pipeline-development-evaluation.md')]
    runtime = runtime_snapshot()
    if runtime['packages']['strands-agents'] != '1.55.0':
        raise ValueError('Qualification requires locked Strands 1.55.0 runtime')
    return {'version': 1, 'stage': 'fixed_originals_question_pipeline_development',
        'dataset_sha256': digest(dataset.read_bytes()),
        'code_sha256': {str(p.relative_to(ROOT)): digest(p.read_bytes()) for p in paths},
        'runtime': runtime, 'mode': 'strict', 'cases': 12, 'max_native_calls': 300,
        'elapsed_seconds': 1800, 'estimated_total_tokens': 2000000,
        'sdk_retry_policy': 'single_attempt', 'proxy_cache_policy': 'bypass',
        'retrieval': 'fixed case originals; live retrieval not evaluated',
        'grading': 'independent original-source review of each case before continuing'}


def previous_runs(output, case_index):
    calls, elapsed = 0, 0.0
    for index in range(case_index):
        directory = output / f'case-{index:02d}'
        result_bytes = (directory / 'result.json').read_bytes()
        result = json.loads(result_bytes)
        review = json.loads((directory / 'review.json').read_bytes())
        if (review.get('result_sha256') != digest(result_bytes)
                or review.get('spec') != 'pass' or review.get('standards') != 'pass'
                or result['error'] is not None):
            raise ValueError('Prior case lacks passing independent review')
        calls += result['native_call_count']
        elapsed += result['elapsed_seconds']
    return calls, elapsed


async def execute(dataset, manifest, output, case_index):
    if manifest != manifest_for(dataset):
        raise ValueError('Frozen code, dataset or runtime changed')
    data = load_data(dataset)
    if type(case_index) is not int or not 0 <= case_index < len(data['cases']):
        raise ValueError('Invalid case index')
    prior_calls, prior_elapsed = previous_runs(output, case_index)
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
    case = data['cases'][case_index]
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
    native_text, native_model, native_agent = orchestrator._text_agent, orchestrator._model, native_module.Agent

    async def captured_text(name, system_prompt, prompt, *, response_format=None):
        if len(attempts) >= remaining_calls:
            raise RuntimeError('Native call budget exhausted')
        attempt = {'index': len(attempts), 'name': name, 'system_prompt': system_prompt,
                   'prompt': prompt, 'response_format': response_format, 'diagnostics': []}
        attempts.append(attempt)
        write_private(directory / f"attempt-{attempt['index']:03d}-input.json", attempt)
        token = CURRENT_ATTEMPT.set(attempt)
        started = time.monotonic()
        try:
            attempt['response'] = await native_text(name, system_prompt, prompt, response_format=response_format)
            return attempt['response']
        except BaseException as exc:
            attempt['exception_type'] = type(exc).__name__
            raise
        finally:
            attempt['elapsed_seconds'] = time.monotonic() - started
            write_private(directory / f"attempt-{attempt['index']:03d}-output.json", attempt)
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
            native_plan = await orchestrator.plan_query(case['question'], 'strict', include_requirements=True)
            try:
                requirements = validate_requirements({k: native_plan[k] for k in ('resolved_question', 'requirements')})
                planning_status = 'complete'
            except (TypeError, KeyError, ValueError):
                requirements = coarse_requirements(case['question']); planning_status = 'coarse'
            plan = merge_agent_plan(requirements['resolved_question'], 'strict', native_plan or {})
            plan.update(requirements, original_question=case['question'], evaluated_at=case['evaluated_at'],
                source_date_order=case['source_date_order'], requirements_status=planning_status,
                pipeline_version=PIPELINE_VERSION,
                request_identity_digest=digest(json.dumps({'manifest': manifest, 'case': case['id']}, sort_keys=True).encode()))
            final = await finalize_question(orchestrator, case['question'], evidence_pack(case), plan, 'strict')
            final.update(query_plan=plan, question=case['question'], mode='strict')
    except (Exception, asyncio.CancelledError) as exc:
        error = type(exc).__name__
    finally:
        elapsed = time.monotonic() - started
        await orchestrator.close()
        CURRENT_QUERY_METRICS.reset(metrics_token)
        native_module.Agent = native_agent
        logger.removeHandler(handler); logger.setLevel(prior_level)
    result = {'case_id': case['id'], 'case_index': case_index, 'final': final, 'error': error,
        'elapsed_seconds': elapsed, 'native_call_count': len(attempts), 'execution': metrics.report(),
        'usage': {key: sum(a['usage'][key] for a in attempts)
            if attempts and all(key in a.get('usage', {}) for a in attempts) else None
            for key in ('inputTokens', 'outputTokens', 'totalTokens')},
        'usage_reporting_attempts': sum(bool(a.get('usage')) for a in attempts),
        'upstream_transport_attempts': 'unknown', 'output_token_cap': None,
        'grading_status': 'pending_independent_review', 'passed': None,
        'not_started_cases': len(data['cases']) - case_index - 1}
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
    args = parser.parse_args()
    if not args.execute:
        write_private(args.manifest, manifest_for(args.dataset))
        return
    if args.output is None:
        parser.error('--execute requires --output')
    print(json.dumps(asyncio.run(execute(args.dataset, json.loads(args.manifest.read_bytes()),
                                         args.output, args.case_index))))


if __name__ == '__main__':
    main()
