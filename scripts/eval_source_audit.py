#!/usr/bin/env python3
"""Evaluate the production native audit and source finalizer on frozen assertions.

Default is offline preparation. Execution is explicit and writes private artifacts;
this stage does not evaluate retrieval, repair, delivery or browser behavior.
"""
from __future__ import annotations

import argparse
import asyncio
import contextvars
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CODE_FILES = tuple(sorted(str(p.relative_to(ROOT)) for p in (ROOT / 'app').glob('*.py'))) + ('scripts/eval_source_audit.py', 'requirements.lock')
CURRENT_ATTEMPT = contextvars.ContextVar('evaluation_attempt', default=None)


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_private(path: Path, data):
    # Exclusive creation: an earlier failure is never overwritten by a rerun.
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        json.dump(data, stream, ensure_ascii=False, indent=2)
        stream.write('\n')


def load_dataset(path: Path):
    return parse_dataset(path.read_bytes())


def parse_dataset(payload: bytes):
    data = json.loads(payload)
    if data.get('version') != 1 or data.get('partition') not in {'development', 'holdout'}:
        raise ValueError('Unsupported dataset contract')
    if type(data.get('synthetic_only')) is not bool or not data.get('cases'):
        raise ValueError('Dataset must declare source classification and cases')
    ids = set()
    documents = {}
    for case in data['cases']:
        if not isinstance(case.get('id'), str) or case['id'] in ids:
            raise ValueError('Duplicate or missing case identity')
        ids.add(case['id'])
        if not all(isinstance(case.get(k), str) and case[k] for k in
                   ('family', 'domain', 'question', 'evaluated_at', 'source_date_order')):
            raise ValueError('Missing case metadata')
        from datetime import date
        date.fromisoformat(case['evaluated_at'])
        if case['source_date_order'] not in {'mdy', 'dmy', 'reject_ambiguous'}:
            raise ValueError('Invalid source date order')
        if not case.get('documents') or not case.get('claims') or len(case['claims']) > 80:
            raise ValueError('Missing sources/claims or excessive audit units')
        for doc in case['documents']:
            if type(doc.get('document_id')) is not int or not isinstance(doc.get('content'), str) or not doc['content']:
                raise ValueError('Invalid original document')
            previous = documents.setdefault(doc['document_id'], doc)
            if previous != doc:
                raise ValueError('Document identity has conflicting source text')
        for index, claim in enumerate(case['claims'], 1):
            if claim.get('id') != f'u{index}' or not isinstance(claim.get('text'), str) or not claim['text']:
                raise ValueError('Claims must have ordered production unit identities')
            if type(claim.get('expected_supported')) is not bool or type(claim.get('required')) is not bool:
                raise ValueError('Missing independent labels')
            if not claim.get('reason') or claim['required'] and not claim['expected_supported']:
                raise ValueError('Invalid required aspect or missing label rationale')
    return data


def runtime_snapshot():
    from importlib.metadata import version
    from app.config import settings
    return dict(model=settings.strands_model or settings.gemini_model,
                destination=settings.litellm_url,
                call_timeout_seconds=settings.strands_call_timeout_seconds,
                audit_timeout_seconds=settings.answer_audit_timeout_seconds,
                concurrency=settings.strands_max_concurrent_calls,
                enabled=settings.strands_enabled,
                packages={name: version(name) for name in ('strands-agents', 'openai', 'httpx', 'pydantic')})


def prepare(dataset_path, **options):
    return prepare_bytes(dataset_path.read_bytes(), **options)


def prepare_bytes(payload, *, model, runtime, repetitions, max_attempts, seconds, estimated_tokens, cache_note):
    data = parse_dataset(payload)
    if data['partition'] != 'development':
        raise ValueError('Holdouts require a separate frozen G5 qualification manifest')
    if repetitions < 1 or max_attempts < 1 or not math.isfinite(seconds) or seconds <= 0 or estimated_tokens < 1:
        raise ValueError('Experiment limits must be positive')
    if runtime.get('model') != model or set(runtime) != {'model', 'destination', 'call_timeout_seconds', 'audit_timeout_seconds', 'concurrency', 'enabled', 'packages'}:
        raise ValueError('A captured matching runtime contract is required')
    if not model.strip() or not cache_note.strip():
        raise ValueError('Model route and cache disclosure are required')
    return dict(version=1, stage='native_audit_and_source_finalization',
                dataset_sha256=digest(payload),
                code_sha256={name: digest((ROOT / name).read_bytes()) for name in CODE_FILES},
                model=model, runtime=runtime, repetitions=repetitions, max_provider_attempts=max_attempts,
                elapsed_seconds=seconds, estimated_total_tokens=estimated_tokens,
                cache_note=cache_note, independent_repetitions=False,
                conditions='exact case documents and ordering; four-unit production batches',
                cases=len(data['cases']), assertions=sum(len(c['claims']) for c in data['cases']),
                synthetic_only=data['synthetic_only'])


def score_case(case, ledger, audits):
    claims = {c['id']: c for c in ledger.get('claims', [])}
    # Corrections replace malformed attempts only; every attempt stays in artifacts.
    assessments = {}
    for audit in audits:
        if isinstance(audit, dict):
            assessments.update({a['unit_id']: a for a in audit.get('assessments', [])})
    rows = []
    for expected in case['claims']:
        unit_id = expected['id']
        claim, assessment = claims.get(unit_id, {}), assessments.get(unit_id, {})
        available = bool(ledger.get('complete')) and assessment.get('status') in {'supported', 'unsupported', 'missing', 'conflicting'}
        raw_accepts = assessment.get('model_status', assessment.get('status')) == 'supported'
        audit_accepts = assessment.get('status') == 'supported'
        source_accepts = claim.get('status') == 'supported'
        positive = expected['expected_supported']
        rows.append(dict(unit_id=unit_id, expected_supported=positive,
                         available=available, model_supported=raw_accepts, auditor_supported=audit_accepts,
                         source_supported=source_accepts,
                         false_acceptance=not positive and (raw_accepts or audit_accepts or source_accepts),
                         false_rejection=positive and available and not source_accepts,
                         missing_required=expected['required'] and not source_accepts,
                         reasons=claim.get('rejection_reasons', []),
                         passed=available and ((positive and source_accepts) or
                                               (not positive and not raw_accepts and not audit_accepts and not source_accepts))))
    return dict(case_id=case['id'], passed=all(r['passed'] for r in rows), assertions=rows)


class AttemptLog(logging.Handler):
    """Retain production's content-free usage/stop diagnostics per async attempt."""
    def emit(self, record):
        attempt = CURRENT_ATTEMPT.get()
        if attempt is not None:
            attempt['diagnostics'].append(record.getMessage())
            if record.msg.startswith('Strands stage=%s outcome=') and isinstance(record.args, tuple):
                attempt['outcome'] = record.args[1]
                attempt['usage'] = json.loads(record.args[-1])


async def execute(dataset_path, manifest, output):
    payload = dataset_path.read_bytes()
    data = parse_dataset(payload)
    expected = prepare_bytes(payload, model=manifest['model'], runtime=manifest['runtime'], repetitions=manifest['repetitions'],
                       max_attempts=manifest['max_provider_attempts'], seconds=manifest['elapsed_seconds'],
                       estimated_tokens=manifest['estimated_total_tokens'], cache_note=manifest['cache_note'])
    if manifest != expected:
        raise ValueError('Frozen manifest no longer matches dataset, code or execution contract')
    from app.config import settings
    from app.answer_finalization import AnswerFinalizer
    from app.answer_observations import ObservationCandidate
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app import strands_orchestrator as native_module
    if runtime_snapshot() != manifest['runtime']:
        raise ValueError('Configured runtime differs from the frozen experiment')
    auditor = StrandsQueryOrchestrator()
    if not auditor.enabled:
        raise ValueError('Native model adapter is unavailable')
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    write_private(output / 'manifest.json', manifest)
    write_private(output / 'dataset.json', data)
    write_private(output / 'runtime.json', dict(model=manifest['model'],
                  call_timeout_seconds=settings.strands_call_timeout_seconds,
                  audit_timeout_seconds=settings.answer_audit_timeout_seconds,
                  audit_concurrency=settings.strands_max_concurrent_calls,
                  request_identifiers='Not exposed by native adapter; unavailable',
                  provider_destination=settings.litellm_url, output_token_cap=None))
    attempts, results = [], []
    native_text, native_audit = auditor._text_agent, auditor.audit_answer_units
    case_audits = []

    async def captured_text(name, system_prompt, prompt, *, response_format=None):
        if len(attempts) >= manifest['max_provider_attempts']:
            raise RuntimeError('Evaluation attempt budget exhausted')
        attempt = dict(index=len(attempts), name=name, system_prompt=system_prompt,
                       prompt=prompt, response_format=response_format, diagnostics=[])
        attempts.append(attempt)
        write_private(output / f"attempt-{attempt['index']:04d}-input.json", attempt)
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
            write_private(output / f"attempt-{attempt['index']:04d}-output.json", attempt)
            CURRENT_ATTEMPT.reset(token)

    async def captured_audit(*args, **kwargs):
        result = await native_audit(*args, **kwargs)
        case_audits.append(result)
        return result

    native_agent = native_module.Agent

    class CapturedAgent(native_agent):
        async def invoke_async(self, *args, **kwargs):
            result = await super().invoke_async(*args, **kwargs)
            attempt = CURRENT_ATTEMPT.get()
            if attempt is not None:
                # Observe before the production adapter rejects nonterminal output.
                attempt['native_result'] = dict(text=str(result), message=result.message,
                                                stop_reason=result.stop_reason)
            return result

    # This runner owns an isolated process. Restore the SDK binding on every exit.
    native_module.Agent = CapturedAgent
    auditor._text_agent, auditor.audit_answer_units = captured_text, captured_audit
    handler = AttemptLog()
    logger = logging.getLogger('app.strands_orchestrator')
    prior_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    started = time.monotonic()
    failure = None
    try:
        async with asyncio.timeout(manifest['elapsed_seconds']):
            for repetition in range(manifest['repetitions']):
                for case in data['cases']:
                    case_audits.clear()
                    candidate = ObservationCandidate.from_response({'observations': [c['text'] for c in case['claims']]})
                    pack = {'items': [dict(id=f"document-{doc['document_id']}", document_id=doc['document_id'],
                                          title=doc.get('title', ''), chunk_index=0, source_kind='ocr',
                                          content=doc['content'], source_content=doc['content']) for doc in case['documents']]}
                    finalizer = AnswerFinalizer(auditor, timeout_seconds=settings.answer_audit_timeout_seconds,
                                                concurrency=settings.strands_max_concurrent_calls, date_order=case['source_date_order'])
                    case_started = time.monotonic()
                    first_attempt = len(attempts)
                    ledger = {}
                    error = None
                    interrupted = None
                    try:
                        async with asyncio.timeout(finalizer._audit_timeout(candidate.text, candidate)):
                            ledger = await finalizer._audit(case['question'], candidate.text, pack,
                                                           {'evaluated_at': case['evaluated_at']}, observations=candidate)
                    except asyncio.CancelledError as exc:
                        error = type(exc).__name__
                        interrupted = exc
                    except Exception as exc:
                        error = type(exc).__name__
                    scored = score_case(case, ledger, case_audits)
                    scored.update(repetition=repetition, elapsed_seconds=time.monotonic() - case_started,
                                  first_attempt=first_attempt, attempts=len(attempts) - first_attempt, error=error,
                                  completed=bool(ledger.get('complete')) and error is None)
                    write_private(output / f'case-{len(results):04d}.json', dict(score=scored, ledger=ledger, audits=list(case_audits)))
                    results.append(scored)
                    if interrupted is not None:
                        raise interrupted
                    if len(attempts) >= manifest['max_provider_attempts'] and len(results) < manifest['cases'] * manifest['repetitions']:
                        raise RuntimeError('Evaluation attempt budget exhausted')
    except (Exception, asyncio.CancelledError) as exc:
        failure = type(exc).__name__
    finally:
        await auditor.close()
        logger.removeHandler(handler)
        logger.setLevel(prior_level)
        auditor._text_agent, auditor.audit_answer_units = native_text, native_audit
        native_module.Agent = native_agent
    rows = [row for result in results for row in result['assertions']]
    durations = [r['elapsed_seconds'] for r in results]
    scheduled = data['cases'] * manifest['repetitions']
    not_started = scheduled[len(results):]
    not_started_assertions = sum(len(case['claims']) for case in not_started)
    summary = dict(stage=manifest['stage'], manifest_sha256=digest(json.dumps(manifest, sort_keys=True).encode()),
                   expected_runs=len(scheduled), recorded_runs=len(results),
                   completed_runs=sum(r['completed'] for r in results),
                   incomplete_runs=sum(not r['completed'] for r in results),
                   not_started_runs=len(not_started), not_started_assertions=not_started_assertions,
                   provider_attempts=len(attempts), failure=failure,
                   usage={key: sum(a['usage'][key] for a in attempts)
                          if attempts and all(key in a.get('usage', {}) for a in attempts) else None
                          for key in ('inputTokens', 'outputTokens', 'totalTokens', 'cacheReadInputTokens', 'cacheWriteInputTokens')},
                   usage_coverage={key: sum(key in a.get('usage', {}) for a in attempts)
                                   for key in ('inputTokens', 'outputTokens', 'totalTokens', 'cacheReadInputTokens', 'cacheWriteInputTokens')},
                   usage_reporting_attempts=sum(bool(a.get('usage')) for a in attempts),
                   usage_complete=bool(attempts) and all({'inputTokens', 'outputTokens'} <= a.get('usage', {}).keys() for a in attempts),
                   passed=failure is None and len(results) == manifest['cases'] * manifest['repetitions'] and all(r['passed'] for r in results),
                   false_acceptances=sum(r['false_acceptance'] for r in rows),
                   false_rejections=sum(r['false_rejection'] for r in rows),
                   missing_required=sum(r['missing_required'] for r in rows) + sum(c['required'] for case in not_started for c in case['claims']),
                   unavailable=sum(not r['available'] for r in rows) + not_started_assertions,
                   median_seconds=statistics.median(durations) if durations else None,
                   max_seconds=max(durations) if durations else None, elapsed_seconds=time.monotonic() - started,
                   independent_repetitions=False, cache_note=manifest['cache_note'],
                   coverage_limit='Assertion audit only; no retrieval, repair, final answer delivery or browser acceptance.')
    write_private(output / 'summary.json', summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset', type=Path)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--model', default='gemini-3.8-flash')
    parser.add_argument('--runtime', type=Path, help='Previously captured destination/settings/package contract')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--max-attempts', type=int, default=96)
    parser.add_argument('--seconds', type=float, default=1800)
    parser.add_argument('--estimated-tokens', type=int, default=250000)
    parser.add_argument('--cache-note', default='No application cache used; proxy/provider cache independence unverified.')
    args = parser.parse_args()
    if args.execute:
        if args.output is None:
            parser.error('--execute requires a new private --output directory')
        report = asyncio.run(execute(args.dataset, json.loads(args.manifest.read_bytes()), args.output))
        print(json.dumps(report))
        return 0 if report['passed'] else 1
    if args.runtime is None:
        parser.error('Preparation requires --runtime with the captured execution configuration')
    manifest = prepare(args.dataset, model=args.model, runtime=json.loads(args.runtime.read_bytes()), repetitions=args.repetitions,
                       max_attempts=args.max_attempts, seconds=args.seconds,
                       estimated_tokens=args.estimated_tokens, cache_note=args.cache_note)
    write_private(args.manifest, manifest)
    print(json.dumps(manifest))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
