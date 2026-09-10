"""New private scope package from retained originals and separately reviewed challenges."""
import json
from pathlib import Path

from scripts import run_model_comparison as runner
from scripts import source_scope_diagnostic as profile
from scripts.eval_source_audit import write_private


def prepare(retained_root, challenges_path, output):
    retained_root, output = Path(retained_root), Path(output)
    parent_raw = (retained_root/'manifest.json').read_bytes()
    parent = json.loads(parent_raw)
    retained = runner.freeze(parent, retained_root)
    runner.validate_sources(parent, retained)
    challenge_raw = Path(challenges_path).read_bytes()
    json.loads(challenge_raw)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name, raw in {**retained, 'manifest.json': parent_raw}.items():
        path = output/'retained'/name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.open('xb') as stream: stream.write(raw)
        path.chmod(0o600)
    with (output/'challenges.json').open('xb') as stream: stream.write(challenge_raw)
    (output/'challenges.json').chmod(0o600)
    protocol = Path(__file__).resolve().parents[1]/'docs/specs/source-scope-diagnostic.md'
    (output/'protocol.md').write_bytes(protocol.read_bytes()); (output/'protocol.md').chmod(0o600)
    manifest = dict(kind=profile.KIND, status='prepared_not_admitted', retained_manifest='retained/manifest.json',
        challenges='challenges.json', protocol='protocol.md', executions=profile.schedule(),
        failure_policy='stop_pair_continue_controls_stop_shared', provider_capacity='unknown',
        limits=None, runtimes=None, destinations=None, preflight=None, review_receipts=[],
        code_sha256=runner.recovery.code_identity())
    manifest['sha256'] = {str(p.relative_to(output)): runner.recovery.digest(p)
                          for p in sorted(output.rglob('*')) if p.is_file()}
    write_private(output/'manifest-prepared.json', manifest)
    executions = profile.prepare(manifest, runner.freeze(manifest, output))
    return dict(route_executions=len(executions), matched_pairs=len(executions)//2, native_model_calls=0)
