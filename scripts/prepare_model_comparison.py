"""New private comparison package from immutable B2 inputs and reviewed challenge labels."""
import copy
import json
from pathlib import Path
from scripts import run_model_comparison as runner
from scripts.prepare_source_records import prepare as prepare_records
from scripts.eval_source_audit import write_private


async def prepare(b2_root, labels_path, output):
    output = Path(output)
    labels = json.loads(Path(labels_path).read_bytes())
    await prepare_records(b2_root, output)
    prepared = output/'manifest-prepared.json'
    legacy = json.loads(prepared.read_bytes())
    # Preserve the preparer's manifest as provenance; never mutate an old experiment.
    prepared.rename(output/'source-manifest.json')
    appendix = {case: [copy.deepcopy(row['observation']) for row in labels[case]] for case in runner.CASE_ORDER}
    write_private(output/'appendix.json', appendix)
    write_private(output/'appendix-labels.json', labels)
    cases = {}
    for case in runner.CASE_ORDER:
        rows = sorted([r for r in legacy['pairs'] if r['case'] == case], key=lambda r: r['repetition'])
        variants = [json.loads((output/r['input']).read_bytes()) for r in rows]
        pair = copy.deepcopy(variants[0]); doc = pair['primary']['documents'][0]
        origins = []
        for row, variant in zip(rows, variants):
            for ordinal, observation in enumerate(variant['primary']['documents'][0]['observations']):
                origins.append({'kind': 'b2', 'b2_index': row['b2_index'], 'ordinal': ordinal,
                                'observation': observation})
        doc['observations'] += variants[1]['primary']['documents'][0]['observations']
        doc['observations'] += appendix[case]
        origins += [{'kind': 'appendix', 'ordinal': i, 'observation': o} for i, o in enumerate(appendix[case])]
        for i, origin in enumerate(origins): origin['unit_id'] = f'u{i+1}'
        name = f'{case}-comparison.json'; sidecar = f'{case}-origins.json'
        write_private(output/name, pair); write_private(output/sidecar, origins)
        cases[case] = {'input': name, 'origins': sidecar}
    protocol = Path(__file__).resolve().parents[1]/'docs/specs/reader-verifier-model-diagnostic.md'
    (output/'model-protocol.md').write_bytes(protocol.read_bytes())
    manifest = {'kind': 'reader-verifier-model-v1', 'status': 'prepared_not_admitted',
        'source_manifest': 'source-manifest.json', 'protocol': 'model-protocol.md',
        'cases': cases, 'originals': legacy['originals'], 'executions': runner.schedule(),
        'appendix': 'appendix.json', 'appendix_labels': 'appendix-labels.json',
        'failure_policy': 'stop_pair_continue_controls_stop_shared', 'provider_capacity': 'unknown',
        'limits': None, 'runtimes': None, 'destinations': None, 'preflight': None,
        'review_receipts': [], 'code_sha256': runner.recovery.code_identity()}
    manifest['sha256'] = {str(p.relative_to(output)): runner.recovery.digest(p)
                          for p in sorted(output.rglob('*')) if p.is_file()}
    write_private(prepared, manifest)
    runner.validate_sources(manifest, runner.freeze(manifest, output))
    return {'route_executions': len(manifest['executions']),
            'appendix_observations': sum(map(len, appendix.values())), 'native_model_calls': 0}
