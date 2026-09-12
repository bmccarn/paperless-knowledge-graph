"""Copy immutable B2 inputs into a new source-record comparison, never native calls."""
import json
from pathlib import Path
from scripts.eval_source_audit import write_private
from scripts.prepare_source_recovery import prepare as prepare_recovery
from scripts import run_source_records as runner


async def prepare(b2_root, output):
    output = Path(output)
    await prepare_recovery(b2_root, output)
    path = output/'manifest-prepared.json'
    manifest = json.loads(path.read_bytes())
    protocol = Path(__file__).resolve().parents[1]/'docs/specs/source-record-native-diagnostic.md'
    (output/'protocol.md').write_bytes(protocol.read_bytes())
    manifest.update(kind='source-record-comparison-v1', schedule=runner.schedule(), limits=runner.LIMITS,
                    code_sha256=runner.recovery.code_identity())
    for row, expected in zip(manifest['pairs'], runner.schedule()): row.update(expected)
    manifest['sha256']['protocol.md'] = runner.recovery.digest(output/'protocol.md')
    path.unlink(); write_private(path, manifest)
    runner.validate_sources(manifest, runner.freeze(manifest, output))
    return {'pairs': len(manifest['pairs']), 'native_model_calls': 0}
