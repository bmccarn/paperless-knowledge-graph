"""Build source-recovery inputs from frozen local B2 artifacts; no network/models."""
import asyncio
import json
from pathlib import Path
import shutil

from scripts.eval_source_audit import write_private
from scripts.prepare_reader_retention import original_from, digest, encoded
from scripts.run_source_recovery import LIMITS, schedule, prepared_inputs, code_identity


async def full_pack(original, retained):
    from app.paperless import PaperlessClient
    from app.source_acquisition import SourceAcquisition, Execution
    generation = digest(encoded(original))
    class Originals:
        async def get_skip_tag_ids(self): return set()
        async def get_document(self, document_id):
            if document_id != original['id']: raise ValueError('Unexpected original')
            return json.loads(encoded(original))
    class Index:
        async def acquisition_document_page(self, *args, **kwargs): raise AssertionError('No discovery')
        async def get_open_feedback_document_ids(self, ids): return set()
        async def get_incomplete_document_ids(self, ids): return set()
        async def get_ingestion_fingerprints(self, ids): return {original['id']: PaperlessClient.ingestion_fingerprint(original)}
        async def get_doc_hash(self, document_id): return PaperlessClient.content_hash(original['content'])
    request = {k: retained[k] for k in ('question', 'resolved_question', 'evaluated_at', 'source_date_order')}
    request.update(mode='strict', conversation_context='', corpus_generation=generation)
    bundle = await SourceAcquisition(Index(), Originals(), lambda: asyncio.sleep(0, result=generation)).collect(
        request, [{'id': 'frozen_original', 'document_ids': [original['id']], 'status': 'complete',
                   'sampling': 'enumerated'}], Execution(concurrency=1))
    if not bundle.receipt['complete']: raise ValueError('Incomplete original transfer')
    return bundle.evidence_pack


async def prepare(b2_root, output):
    b2_root, output = Path(b2_root), Path(output)
    inventory = json.loads((b2_root/'results-inventory.json').read_bytes())
    for name, expected in inventory.items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or digest((b2_root/'results'/path).read_bytes()) != expected:
            raise ValueError('B2 result inventory changed')
    b2 = json.loads((b2_root/'inputs/manifest.json').read_bytes())
    for name, expected in b2['sha256'].items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or digest((b2_root/'inputs'/path).read_bytes()) != expected:
            raise ValueError('B2 input inventory changed')
    run = json.loads((b2_root/'results/run.json').read_bytes())
    output.mkdir(mode=0o700, exist_ok=False)
    def copy_input(source, name):
        target = output/name
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with target.open('xb') as stream: stream.write(Path(source).read_bytes())
        target.chmod(0o600)
        return name
    files = {key: copy_input(b2_root/source, target) for key, source, target in (
        ('b2_manifest', 'inputs/manifest.json', 'b2-manifest.json'),
        ('b2_run', 'results/run.json', 'b2-run.json'),
        ('b2_inventory', 'results-inventory.json', 'b2-inventory.json'),
        ('b2_grade_spec', 'grade-spec.json', 'b2-grade-spec.json'),
        ('b2_grade_standards', 'grade-standards.json', 'b2-grade-standards.json'),
        ('b2_adjudication', 'adjudication.json', 'b2-adjudication.json'),
        ('gold', 'inputs/gold.json', 'gold.json'))}
    files['protocol'] = copy_input(Path(__file__).resolve().parents[1]/
        'docs/specs/source-interpretation-recovery-diagnostic.md', 'protocol.md')
    requests, originals, pairs = {}, {}, []
    for case in b2['payloads']:
        requests[case] = copy_input(b2_root/'inputs'/b2['payloads'][case]['F'], f'{case}-request.json')
        originals[case] = copy_input(b2_root/'inputs'/b2['originals'][case], f'{case}-original.json')
    for index, row in enumerate(schedule()):
        source = next(r for r in run['rows'] if r['case'] == row['case'] and r['repetition'] == row['repetition'] and r['arm'] == 'F')
        if source['status'] != 'completed': raise ValueError('B2 primary incomplete')
        primary_name = copy_input(b2_root/'results'/f"invocation-{source['index']:02d}/reading.json", f'primary-{index:02d}.json')
        request = json.loads((output/requests[row['case']]).read_bytes())
        original = original_from(json.loads((output/originals[row['case']]).read_bytes()), request['source_documents'][0]['document_id'])
        pair = {'request': request, 'primary': json.loads((output/primary_name).read_bytes()),
                'evidence_pack': await full_pack(original, request)}
        prepared_inputs(pair)
        name = f'pair-{index:02d}.json'; write_private(output/name, pair)
        pairs.append({**row, 'input': name, 'primary': primary_name, 'b2_index': source['index']})
    manifest = {'kind': 'source-recovery-v1', 'status': 'prepared_not_admitted',
        'limits': LIMITS, 'schedule': schedule(), 'pairs': pairs, 'requests': requests, 'originals': originals,
        'failure_policy': 'stop_pair_continue_controls_stop_shared', 'provider_capacity': 'unknown',
        **files, 'runtime': None, 'preflight': None, 'review_receipts': [], 'code_sha256': code_identity()}
    manifest['sha256'] = {str(p.relative_to(output)): digest(p.read_bytes()) for p in output.rglob('*') if p.is_file()}
    write_private(output/'manifest-prepared.json', manifest)
    return {'pairs': len(pairs), 'files': len(manifest['sha256']), 'native_calls': 0}
