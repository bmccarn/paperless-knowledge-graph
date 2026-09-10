#!/usr/bin/env python3
"""Once-only classification of nine frozen omissions using the production protocol."""
import argparse
import asyncio
from datetime import date
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.eval_source_coverage import digest, require_reviews, strict_json, validate_native_call

MAX_CALLS = 9
ACTIVE_SECONDS = 900
CODE_FILES = (
    'scripts/eval_exclusion_authority.py', 'scripts/eval_source_coverage.py',
    'scripts/eval_source_audit.py', 'scripts/live_query_stages.py',
    'scripts/live_query_capture.py', 'scripts/live_query_evaluation.py',
    'tests/test_exclusion_authority_diagnostic.py', 'requirements.lock',
    'docs/specs/question-exclusion-authority.md',
    'docs/specs/question-fact-conservation-integration.md',
)


def _object(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError('Unexpected diagnostic fields')


def _text(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Nonempty diagnostic text required')


def _identities(values):
    if not isinstance(values, list) or not values:
        raise ValueError('Nonempty identity list required')
    for value in values:
        _text(value)
    if len(set(values)) != len(values):
        raise ValueError('Duplicate diagnostic identity')
    return set(values)


def validate_payload(payload):
    """Admit only original sources, owned notes and a neutral omission request."""
    keys = {'original_question', 'evaluated_at', 'source_documents', 'observations',
            'delivered_ids', 'omitted_id'}
    if isinstance(payload, dict) and 'conversation_context' in payload:
        keys.add('conversation_context')
    _object(payload, keys)
    _text(payload['original_question'])
    evaluated = payload['evaluated_at']
    if not isinstance(evaluated, str) or date.fromisoformat(evaluated).isoformat() != evaluated:
        raise ValueError('Canonical calendar date required')
    if 'conversation_context' in payload:
        from app.question_evidence import CONVERSATION_CONTEXT_MAX_CHARS
        context = payload['conversation_context']
        if not isinstance(context, str) or len(context) > CONVERSATION_CONTEXT_MAX_CHARS:
            raise ValueError('Invalid bounded conversation context')
    documents = payload['source_documents']
    if not isinstance(documents, list) or not documents:
        raise ValueError('Original source documents required')
    document_ids, owners = set(), {}
    for document in documents:
        _object(document, {'document_id', 'title', 'spans'})
        identity = document['document_id']
        if type(identity) is not int or identity <= 0 or identity in document_ids:
            raise ValueError('Unique positive document identity required')
        document_ids.add(identity)
        _text(document['title'])
        if not isinstance(document['spans'], list) or not document['spans']:
            raise ValueError('Original spans required')
        for span in document['spans']:
            _object(span, {'span_id', 'text'})
            _text(span['span_id']); _text(span['text'])
            if span['span_id'] in owners:
                raise ValueError('Duplicate original span')
            owners[span['span_id']] = identity
    observations = payload['observations']
    if not isinstance(observations, list) or not observations:
        raise ValueError('Owned reader inventory required')
    for observation in observations:
        _object(observation, {'id', 'text', 'references'})
        _text(observation['id']); _text(observation['text'])
        references = observation['references']
        if not isinstance(references, list) or not references:
            raise ValueError('Original source references required')
        for reference in references:
            _object(reference, {'span_id'})
        span_ids = _identities([r['span_id'] for r in references])
        if not span_ids <= owners.keys() or len({owners[s] for s in span_ids}) != 1:
            raise ValueError('Reader observation must reference its own original document')
    allowed = _identities([r['id'] for r in observations])
    delivered = _identities(payload['delivered_ids'])
    omitted = payload['omitted_id']
    if (not delivered <= allowed or not isinstance(omitted, str)
            or omitted not in allowed or omitted in delivered):
        raise ValueError('One known omitted ID and distinct selected targets required')


def parse_classification(text, payload):
    """Use the same strict response parser as the production omission reviewer."""
    from app.answer_fact_selection import parse_exclusion
    row = parse_exclusion(text, payload['omitted_id'], payload['delivered_ids'])
    return {'decisions': [{key: row[key] for key in ('observation_id', 'decision', 'target_id')}]}


def calls_for(payload):
    data = strict_json(payload)
    _object(data, {'version', 'partition', 'experiment', 'calls'})
    if (type(data['version']) is not int or data['version'] != 2
            or data['partition'] != 'diagnostic-development'
            or data['experiment'] != 'exclusion-authority'):
        raise ValueError('New frozen development protocol required')
    calls = data['calls']
    if not isinstance(calls, list) or len(calls) != MAX_CALLS:
        raise ValueError('Exactly nine scheduled classifications required')
    for call in calls:
        _object(call, {'id', 'payload', 'gold', 'origin'})
        _text(call['id'])
        if not isinstance(call['origin'], dict):
            raise ValueError('Frozen origin metadata required')
        validate_payload(call['payload'])
        gold = call['gold']
        _object(gold, {'decision', 'target_ids'})
        kind, targets = gold['decision'], gold['target_ids']
        if not isinstance(kind, str) or kind not in {'outside_request', 'covered_by', 'reject'}:
            raise ValueError('Independent classification gold required')
        if kind == 'covered_by':
            if not _identities(targets) <= set(call['payload']['delivered_ids']):
                raise ValueError('Gold targets must be direct selected observations')
        elif targets != []:
            raise ValueError('Noncoverage gold cannot name targets')
    _identities([call['id'] for call in calls])
    return calls


def manifest_for(directory, *, payload=None):
    from app.answer_fact_selection import EXCLUSION_PROMPT
    from scripts.eval_source_audit import runtime_snapshot
    payload = (directory / 'inputs.json').read_bytes() if payload is None else payload
    calls_for(payload)
    reviews = require_reviews(directory, payload, 'inputs')
    runtime = runtime_snapshot()
    if runtime['packages']['strands-agents'] != '1.55.0' or runtime['enabled'] is not True:
        raise ValueError('Locked enabled Strands 1.55.0 required')
    paths = sorted((ROOT / 'app').glob('*.py')) + [ROOT / path for path in CODE_FILES]
    return {
        'version': 2, 'experiment': 'exclusion-authority', 'input_sha256': digest(payload),
        'reviews': reviews, 'runtime': runtime,
        'prompt_sha256': digest(EXCLUSION_PROMPT.encode()),
        'code_sha256': {str(path.relative_to(ROOT)): digest(path.read_bytes()) for path in paths},
        'max_calls': MAX_CALLS, 'active_seconds': ACTIVE_SECONDS, 'output_token_cap': None,
        'sdk_retries': 0, 'strands_retries': None, 'proxy_cache': 'bypass',
        'upstream_attempts': 'unknown',
    }


async def execute(directory, manifest):
    from app.answer_fact_selection import EXCLUSION_PROMPT
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from scripts.eval_source_audit import write_private
    from scripts.live_query_capture import ModelCapture
    from scripts.live_query_stages import capture_stages
    # One read binds admission and dispatch; caller-owned metadata cannot change it.
    payload = (directory / 'inputs.json').read_bytes()
    frozen_manifest = strict_json(json.dumps(manifest, allow_nan=False))
    if frozen_manifest != manifest_for(directory, payload=payload):
        raise ValueError('Frozen classification identity changed')
    calls = calls_for(payload)
    output = directory / 'results'
    output.mkdir(mode=0o700)
    write_private(output / 'manifest.json', frozen_manifest)
    capture = ModelCapture(output, max_calls=MAX_CALLS, seconds=ACTIVE_SECONDS)
    orchestrator, stages = None, {'attempts': [], 'hashes': {}}
    results, record_hashes = [], {}
    started = time.monotonic()
    interrupted = close_error = None
    try:
        orchestrator = StrandsQueryOrchestrator()
        with capture_stages(orchestrator, capture, output) as stages:
            for index, call in enumerate(calls):
                if time.monotonic() >= capture.deadline:
                    break
                before = len(capture.attempts)
                record = {'index': index, 'id': call['id'], 'status': 'failed'}
                try:
                    async with asyncio.timeout_at(capture.deadline):
                        text = await orchestrator._text_agent('fact_exclusion', EXCLUSION_PROMPT,
                            json.dumps(call['payload'], ensure_ascii=False))
                    validate_native_call(capture, stages, before)
                    record.update(status='valid_ungraded',
                                  response=parse_classification(text, call['payload']))
                except BaseException as exc:
                    record['exception_type'] = type(exc).__name__
                    if not isinstance(exc, Exception):
                        raise
                finally:
                    capture.close_pending()
                    results.append(record)
                    name = f'call-{index:02d}.json'
                    write_private(output / name, record)
                    record_hashes[name] = digest((output / name).read_bytes())
    except BaseException as exc:
        interrupted = type(exc).__name__
        raise
    finally:
        capture.close_pending()
        try:
            if orchestrator is not None:
                await orchestrator.close()
        except BaseException as exc:
            close_error = type(exc).__name__
            raise
        finally:
            complete = (interrupted is None and close_error is None and len(results) == MAX_CALLS
                        and all(r['status'] == 'valid_ungraded' for r in results)
                        and len(capture.attempts) == MAX_CALLS and not capture.pending
                        and not capture.failures and not capture.exhausted)
            write_private(output / 'result.json', {
                'input_sha256': digest(payload),
                'manifest_sha256': digest((output / 'manifest.json').read_bytes()),
                'calls': results, 'interrupted': interrupted, 'close_error': close_error,
                'elapsed_seconds': time.monotonic() - started,
                'native_call_count': len(capture.attempts),
                'not_started_calls': MAX_CALLS - len(results), 'complete_execution': complete,
                'semantic_verdict': 'pending', 'capture_sha256': capture.hashes,
                'stage_sha256': stages['hashes'], 'call_sha256': record_hashes,
            })


def main():
    from scripts.eval_source_audit import write_private
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    path = args.directory / 'manifest.json'
    if args.execute:
        asyncio.run(execute(args.directory, strict_json(path.read_bytes())))
    else:
        if path.exists():
            raise ValueError('Manifest already frozen')
        write_private(path, manifest_for(args.directory))


if __name__ == '__main__':
    main()
