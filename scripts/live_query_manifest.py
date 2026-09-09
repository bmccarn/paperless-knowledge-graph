"""Freeze private live inputs only after exact all-mode qualification admission."""
import json
from pathlib import Path

from scripts.live_query_evaluation import admit_all_modes, sha256

ROOT = Path(__file__).resolve().parents[1]


def parse_requests(payload):
    data = json.loads(payload)
    if not isinstance(data, dict) or set(data) != {'version', 'cases'} or type(data['version']) is not int or data['version'] != 1:
        raise ValueError('Versioned frozen live requests required')
    cases = data['cases']
    if not isinstance(cases, list) or len(cases) != 6:
        raise ValueError('Exactly six reviewed live requests required')
    ids, modes = set(), set()
    for case in cases:
        if not isinstance(case, dict) or set(case) != {'id', 'question', 'mode', 'model', 'history'}:
            raise ValueError('Unexpected live request fields')
        if any(not isinstance(case[k], str) or not case[k].strip() for k in ('id', 'question', 'model')):
            raise ValueError('Nonempty request identity, question and model required')
        if case['id'] in ids or case['mode'] not in {'quick', 'deep', 'timeline', 'strict'}:
            raise ValueError('Invalid request schedule')
        if not isinstance(case['history'], list):
            raise ValueError('Explicit conversation history required')
        for message in case['history']:
            if (not isinstance(message, dict) or set(message) != {'role', 'content'}
                    or message['role'] not in {'user', 'assistant'}
                    or not isinstance(message['content'], str) or not message['content'].strip()):
                raise ValueError('Only explicit user and assistant antecedents are admitted')
        ids.add(case['id'])
        modes.add(case['mode'])
    if modes != {'quick', 'deep', 'timeline', 'strict'} or not any(c['history'] for c in cases):
        raise ValueError('All modes and a contextual follow-up are required')
    return cases


def private_inputs(directory):
    """Bind rubric originals without ever supplying them as candidate evidence."""
    directory = Path(directory)
    names = ('requests.json', 'rubric.md', 'inventory.json', 'originals-manifest.json')
    captured = {name: (directory / name).read_bytes() for name in names}
    parse_requests(captured['requests.json'])
    if not captured['rubric.md'].strip():
        raise ValueError('Independently reviewed rubric required')
    originals = json.loads(captured['originals-manifest.json'])
    if originals.get('inventory_sha256') != sha256(captured['inventory.json']):
        raise ValueError('Originals belong to a different inventory')
    expected = originals.get('originals_sha256')
    if not isinstance(expected, dict) or not expected:
        raise ValueError('Original-source rubric inventory required')
    for name, digest in expected.items():
        path = Path(name)
        if path.name != name or path.suffix != '.json':
            raise ValueError('Original artifact name must be a local JSON filename')
        payload = (directory / 'originals' / name).read_bytes()
        if sha256(payload) != digest:
            raise ValueError('Rubric original changed since review')
        captured['originals/' + name] = payload
    return {name: sha256(payload) for name, payload in captured.items()}


def prepare_manifest(*, dataset, initial_output, all_mode_output, inputs,
                     configuration, corpus_snapshot, evaluated_at):
    """No readers or models open here. Repeat and compare before each live case.

    Configuration contains only explicitly selected non-secret settings. Corpus
    snapshot and date must be freshly read by the separately reviewed runtime;
    accepting a caller value here is not a claim that freshness was established.
    """
    admission = admit_all_modes(dataset, initial_output, all_mode_output)
    snapshot = json.loads(json.dumps({'configuration': configuration,
                                     'corpus_snapshot': corpus_snapshot}, allow_nan=False))
    files = sorted((ROOT / 'scripts').glob('live_query_*.py'))
    files += sorted((ROOT / 'tests').glob('test_live_query_*.py'))
    files += [ROOT / 'docs/specs/question-live-retrieval-evaluation.md']
    return {'version': 1, 'stage': 'live_question_pipeline_development',
            'all_mode_admission': admission,
            'live_code_sha256': {str(p.relative_to(ROOT)): sha256(p.read_bytes()) for p in files},
            'private_inputs_sha256': private_inputs(inputs),
            'configuration': snapshot['configuration'], 'corpus_snapshot': snapshot['corpus_snapshot'],
            'evaluated_at': evaluated_at, 'max_model_calls': 300, 'active_seconds': 3600,
            'sdk_retries': 0, 'strands_retry_strategy': None, 'proxy_cache': 'bypass',
            'upstream_attempts': 'unknown', 'output_token_cap': None}
