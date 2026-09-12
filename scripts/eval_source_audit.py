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
        if case.get('candidate_encoding', 'plain_observations') not in {'plain_observations', 'rendered_observation_units'}:
            raise ValueError('Unsupported candidate encoding')
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
            if type(doc.get('document_id')) is not int or doc['document_id'] <= 0 or not isinstance(doc.get('content'), str) or not doc['content']:
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


def prepare_bytes(payload, *, model, runtime, repetitions, max_attempts, seconds, estimated_tokens, cache_note, allow_noncontiguous=False, proxy_cache_policy='configured', audit_strategy='flat', sdk_retry_policy='configured'):
    data = parse_dataset(payload)
    if data['partition'] != 'development':
        raise ValueError('Holdouts require a separate frozen G5 qualification manifest')
    from app.source_reading import STRATEGIES
    if audit_strategy not in STRATEGIES:
        raise ValueError('Unknown audit strategy')
    if sdk_retry_policy not in {'configured', 'single_attempt'}:
        raise ValueError('Unknown SDK retry policy')
    if proxy_cache_policy not in {'configured', 'bypass'}:
        raise ValueError('Unknown proxy cache policy')
    if type(allow_noncontiguous) is not bool:
        raise ValueError('Invalid reconstruction admission must be explicit')
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
                synthetic_only=data['synthetic_only'], allow_noncontiguous_reconstruction=allow_noncontiguous,
                proxy_cache_policy=proxy_cache_policy, audit_strategy=audit_strategy, sdk_retry_policy=sdk_retry_policy)


def evidence_pack(case, *, allow_noncontiguous=False):
    """Keep captured production window boundaries; verify them against originals."""
    if 'evidence_pack' not in case:
        return {'items': [dict(id=f"document-{doc['document_id']}", document_id=doc['document_id'],
                              title=doc.get('title', ''), chunk_index=0, source_kind='ocr',
                              content=doc['content'], source_content=doc['content']) for doc in case['documents']]}
    from app.embeddings import chunk_text
    from app.evidence import evidence_item_id
    from app.source_text import certifying_text, certified_document_context
    pack = json.loads(json.dumps(case['evidence_pack']))
    documents = {doc['document_id']: doc for doc in case['documents']}
    if not case.get('source_capture') or not isinstance(pack, dict) or not pack.get('items'):
        raise ValueError('Captured windows require declared provenance and sources')
    for item in pack['items']:
        doc_id, index = item.get('document_id'), item.get('chunk_index')
        if (item.get('source_kind') != 'ocr' or type(doc_id) is not int or doc_id <= 0
                or doc_id not in documents or type(index) is not int or index < 0):
            raise ValueError('Invalid captured source identity')
        document = documents[doc_id]
        original = document['content']
        if item.get('title', '') != document.get('title', ''):
            raise ValueError('Captured title differs from original document metadata')
        expanded = index >= 100000
        chunks = chunk_text(original, chunk_size=3600 if expanded else 4000,
                            overlap=500 if expanded else 800, include_table_headers=not bool(item.get('source_context')))
        offset = index - 100000 if expanded else index
        if (offset >= len(chunks) or chunks[offset] != item.get('content')
                or chunks[offset] != certifying_text(item) or evidence_item_id(item) != item.get('id')):
            raise ValueError('Captured window no longer matches original-source chunking')
        if 'source_context' in item:
            if not isinstance(item['source_context'], dict):
                raise ValueError('Captured source context must be a complete certificate')
            if item['source_context'].get('digest') != digest(original.encode()):
                raise ValueError('Captured source context has a mismatched digest')
            item['_source_document_content'] = original
            if certified_document_context(item, chunks[offset]) is None:
                raise ValueError('Captured source context has invalid original offsets')
        elif '_source_document_content' in item:
            raise ValueError('Unexpected source context')
    if source_continuity(case, pack)['noncontiguous_items'] and not allow_noncontiguous:
        raise ValueError('Noncontiguous reconstructed input requires explicit diagnostic admission')
    return pack


def source_continuity(case, pack):
    from app.source_text import certifying_text
    originals = {d['document_id']: d['content'] for d in case['documents']}
    invalid = [index for index, item in enumerate(pack['items'])
               if certifying_text(item) not in originals[item['document_id']]]
    return dict(original_contiguous=not invalid, noncontiguous_items=invalid,
                classification='diagnosed_invalid_reconstruction' if invalid else 'original_contiguous')


def observation_candidate(case):
    from app.answer_observations import ObservationCandidate
    texts = [claim['text'] for claim in case['claims']]
    if case.get('candidate_encoding') == 'rendered_observation_units':
        candidate = ObservationCandidate.from_text('\n\n'.join(texts))
        if [unit['text'] for unit in candidate.units()] != texts:
            raise ValueError('Saved audit units changed during reconstruction')
        return candidate
    return ObservationCandidate.from_response({'observations': texts})


def score_case(case, ledger, audits, *, raw_audits=()):
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
        raw_accepts = any(row.get('model_status', row.get('status')) == 'supported'
                          for audit in [*audits, *raw_audits] if isinstance(audit, dict)
                          for row in audit.get('assessments', []) if row.get('unit_id') == unit_id)
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


def configure_proxy_cache(model, policy):
    """Request-local proxy control; do not flush or modify shared cache settings."""
    if policy == 'configured':
        return model
    if policy != 'bypass':
        raise ValueError('Unknown proxy cache policy')
    params = dict(model.get_config().get('params') or {})
    extra = dict(params.get('extra_body') or {})
    if 'cache' in extra:
        raise ValueError('Existing request cache settings require separate review')
    extra['cache'] = {'no-cache': True, 'no-store': True}
    model.update_config(params={**params, 'extra_body': extra})
    return model


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
                       estimated_tokens=manifest['estimated_total_tokens'], cache_note=manifest['cache_note'],
                       allow_noncontiguous=manifest['allow_noncontiguous_reconstruction'],
                       proxy_cache_policy=manifest['proxy_cache_policy'], audit_strategy=manifest['audit_strategy'],
                       sdk_retry_policy=manifest['sdk_retry_policy'])
    if manifest != expected:
        raise ValueError('Frozen manifest no longer matches dataset, code or execution contract')
    from app.config import settings
    from app.answer_finalization import AnswerFinalizer
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app import strands_orchestrator as native_module
    if runtime_snapshot() != manifest['runtime']:
        raise ValueError('Configured runtime differs from the frozen experiment')
    auditor = StrandsQueryOrchestrator(audit_strategy=manifest['audit_strategy'])
    if not auditor.enabled:
        raise ValueError('Native model adapter is unavailable')
    packs = {case['id']: evidence_pack(case, allow_noncontiguous=manifest['allow_noncontiguous_reconstruction']) for case in data['cases']}
    input_diagnostics = {case['id']: source_continuity(case, packs[case['id']]) for case in data['cases']}
    inputs_valid = all(d['original_contiguous'] for d in input_diagnostics.values())
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    write_private(output / 'manifest.json', manifest)
    write_private(output / 'dataset.json', data)
    write_private(output / 'source_input_diagnostics.json', input_diagnostics)
    write_private(output / 'runtime.json', dict(model=manifest['model'],
                  call_timeout_seconds=settings.strands_call_timeout_seconds,
                  audit_timeout_seconds=settings.answer_audit_timeout_seconds,
                  audit_concurrency=settings.strands_max_concurrent_calls,
                  request_identifiers='Not exposed by native adapter; unavailable',
                  provider_destination=settings.litellm_url, output_token_cap=None,
                  sdk_retry_policy=manifest['sdk_retry_policy']))
    attempts, results = [], []
    native_text, native_audit, native_model = auditor._text_agent, auditor.audit_answer_units, auditor._model
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

    def captured_model(*, response_format=None):
        model = configure_proxy_cache(native_model(response_format=response_format), manifest['proxy_cache_policy'])
        attempt = CURRENT_ATTEMPT.get()
        if attempt is not None:
            attempt['proxy_cache_policy'] = manifest['proxy_cache_policy']
        return model

    native_agent = native_module.Agent

    class CapturedAgent(native_agent):
        def __init__(self, *args, **kwargs):
            if manifest['sdk_retry_policy'] == 'single_attempt':
                kwargs['retry_strategy'] = None
            super().__init__(*args, **kwargs)

        async def invoke_async(self, *args, **kwargs):
            result = await super().invoke_async(*args, **kwargs)
            attempt = CURRENT_ATTEMPT.get()
            if attempt is not None:
                # Observe before the production adapter rejects nonterminal output.
                attempt['native_result'] = dict(text=str(result), message=result.message,
                                                stop_reason=result.stop_reason)
                if attempt['name'] == 'source_auditor':
                    from app import source_audit
                    try:
                        # Observe schema-valid model verdicts before request-owned handle guards.
                        # These records affect failure scoring only, never availability or evidence.
                        attempt['raw_audit'] = source_audit.parse_decisions(
                            str(result), json.loads(attempt['prompt'])['expected_unit_ids'])
                    except source_audit.SourceAuditProtocolError:
                        pass
            return result

    # This runner owns an isolated process. Restore the SDK binding on every exit.
    native_module.Agent = CapturedAgent
    auditor._text_agent, auditor.audit_answer_units, auditor._model = captured_text, captured_audit, captured_model
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
                    candidate = observation_candidate(case)
                    pack = packs[case['id']]
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
                    raw_audits = [a['raw_audit'] for a in attempts[first_attempt:] if 'raw_audit' in a]
                    scored = score_case(case, ledger, case_audits, raw_audits=raw_audits)
                    scored.update(repetition=repetition, elapsed_seconds=time.monotonic() - case_started,
                                  first_attempt=first_attempt, attempts=len(attempts) - first_attempt, error=error,
                                  completed=bool(ledger.get('complete')) and error is None)
                    write_private(output / f'case-{len(results):04d}.json', dict(score=scored, ledger=ledger, audits=list(case_audits), raw_audits=raw_audits))
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
        auditor._text_agent, auditor.audit_answer_units, auditor._model = native_text, native_audit, native_model
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
                   inputs_valid=inputs_valid, noncontiguous_input_count=sum(len(d['noncontiguous_items']) for d in input_diagnostics.values()),
                   passed=inputs_valid and failure is None and len(results) == manifest['cases'] * manifest['repetitions'] and all(r['passed'] for r in results),
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
    parser.add_argument('--allow-noncontiguous-reconstruction', action='store_true',
                        help='Admit a diagnosed invalid reconstruction for baseline investigation; it cannot pass')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--model', default='gemini-3.8-flash')
    parser.add_argument('--audit-strategy', choices=['flat', 'grouped', 'source_first', 'document_local', 'document_local_corrected'], default='flat')
    parser.add_argument('--sdk-retry-policy', choices=['configured', 'single_attempt'], default='configured')
    parser.add_argument('--proxy-cache-policy', choices=['configured', 'bypass'], default='configured')
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
                       estimated_tokens=args.estimated_tokens, cache_note=args.cache_note,
                       allow_noncontiguous=args.allow_noncontiguous_reconstruction, proxy_cache_policy=args.proxy_cache_policy, audit_strategy=args.audit_strategy, sdk_retry_policy=args.sdk_retry_policy)
    write_private(args.manifest, manifest)
    print(json.dumps(manifest))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
