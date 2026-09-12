#!/usr/bin/env python3
"""Remote owner for one statically admitted live browser request.

Prepare uses supplied corpus/build snapshots, which must be independently captured.
Run revalidates corpus before readiness. Neither command activates the serving flag.
"""
import argparse
import asyncio
from contextlib import redirect_stdout
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.conservative_query_admission import strict_json
from scripts.eval_source_audit import write_private
from scripts.live_query_admission import admit_case
from scripts.live_query_control import own_runtime, _identity
from scripts.live_query_evaluation import sha256
from scripts.live_query_manifest import prepare_manifest


def runtime_configuration():
    """Explicit non-secret settings only. Never serialize environment or credentials."""
    from app.config import settings
    names = ('gemini_model', 'fallback_model', 'strands_model', 'embedding_model',
             'strands_enabled', 'strands_call_timeout_seconds', 'strands_max_concurrent_calls',
             'stream_verification_timeout_seconds', 'answer_audit_timeout_seconds',
             'source_date_order', 'paperless_skip_tag_names', 'postgres_host',
             'postgres_port', 'postgres_db')
    values = {name: getattr(settings, name) for name in names}
    from urllib.parse import urlsplit
    for name, value in (('paperless_url', settings.effective_paperless_external_url),
                        ('paperless_source_url', settings.paperless_url),
                        ('litellm_url', settings.litellm_url), ('neo4j_uri', settings.neo4j_uri),
                        ('redis_url', settings.redis_url)):
        parts = urlsplit(value)
        if parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError('Credential-bearing endpoint cannot be included in a live manifest')
        values[name] = value
    # Owner context affects prompt meaning. Bind its bytes without publishing them.
    values['owner_context_sha256'] = sha256(json.dumps(
        [settings.owner_name, settings.owner_context], ensure_ascii=False).encode())
    return values


def preparation_options(payload):
    options = strict_json(payload)
    expected = {'dataset', 'initial_output', 'all_mode_output', 'inputs',
                'configuration', 'corpus_snapshot', 'evaluated_at'}
    if not isinstance(options, dict) or set(options) not in (expected, expected | {'conservative_admission'}):
        raise ValueError('Explicit live preparation arguments required')
    configuration = options['configuration']
    if not isinstance(configuration, dict) or configuration.get('runtime') != runtime_configuration():
        raise ValueError('Actual runtime configuration differs from preparation')
    # Expose only identity fields needed by artifact conservation. Preserve the
    # full checked settings under runtime for equality at every invocation.
    for name in ('paperless_url', 'strands_model', 'gemini_model', 'source_date_order'):
        if configuration.get(name) != configuration['runtime'][name]:
            raise ValueError('Request configuration differs from actual runtime')
    return options


async def run_case(options, manifest, output, index, identity, reader, emit):
    """Admission happens before any application reader/model/client is constructed."""
    identity = _identity(identity)
    if identity['manifest_sha256'] != sha256(json.dumps(manifest, sort_keys=True, allow_nan=False).encode()):
        raise ValueError('Control identity differs from frozen manifest')
    if identity['pod_uid'] != manifest['configuration']['cluster']['pod_uid']:
        raise ValueError('Control pod differs from frozen admission')
    admission = admit_case(manifest, index=index, output=output, **options)
    # Exclusive creation commits this attempt; any later failure remains evidence.
    directory = Path(output) / f'case-{index:02d}'
    directory.mkdir(mode=0o700)
    from scripts.live_query_runtime import query_runtime
    def factory():
        return query_runtime(admission['request'], directory, max_calls=admission['max_calls'],
                             seconds=admission['seconds'], expected_corpus=manifest['corpus_snapshot'],
                             evaluated_at=manifest['evaluated_at'])
    await own_runtime(factory, reader, emit, identity=identity, seconds=admission['seconds'])


async def command(args):
    options = preparation_options(Path(args.preparation).read_bytes())
    if args.command == 'prepare':
        write_private(Path(args.manifest), prepare_manifest(**options))
        return
    manifest = strict_json(Path(args.manifest).read_bytes())
    identity = strict_json(Path(args.identity).read_bytes())
    reader = asyncio.StreamReader(limit=4096)
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    # Keep protocol output distinct from application logging and print statements.
    control = os.fdopen(os.dup(sys.stdout.fileno()), 'w', buffering=1)
    async def emit(message):
        control.write(json.dumps(message, allow_nan=False) + '\n'); control.flush()
    try:
        with redirect_stdout(sys.stderr):
            await run_case(options, manifest, Path(args.output), args.index, identity, reader, emit)
    finally:
        transport.close(); control.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'run-case'))
    parser.add_argument('--preparation', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--output'); parser.add_argument('--index', type=int)
    parser.add_argument('--identity')
    args = parser.parse_args()
    if args.command == 'run-case' and (args.output is None or args.index is None or args.identity is None):
        parser.error('run-case requires --output, --index and --identity')
    os.umask(0o077)
    asyncio.run(command(args))


if __name__ == '__main__':
    main()
