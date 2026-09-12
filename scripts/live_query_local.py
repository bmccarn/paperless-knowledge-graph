#!/usr/bin/env python3
"""Own local forwarding/browser and the separately admitted remote request process."""
import argparse
import asyncio
import io
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import sys
import tarfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.conservative_query_admission import strict_json
from scripts.eval_source_audit import write_private
from scripts.live_query_admission import capture_result
from scripts.live_query_evaluation import sha256
from scripts.live_query_frontend import local_frontend
from scripts.live_query_local_identity import local_identity, node_environment
from scripts.live_query_manifest import parse_requests, reviewed_private_inputs
from scripts.live_query_process import process_bridge, _reap
from scripts.live_query_control import _join_owned


def unpack_retained(payload, directory):
    """Accept only relative regular files; never replace existing evidence bytes."""
    directory=Path(directory).resolve()
    with tarfile.open(fileobj=io.BytesIO(payload),mode='r:gz') as archive:
        members=archive.getmembers();names=set()
        for member in members:
            name=PurePosixPath(member.name)
            if (not member.isfile() or name.is_absolute() or '..' in name.parts
                    or member.name in names or str(name)!=member.name):
                raise ValueError('Unsafe or duplicate retained artifact')
            names.add(member.name)
        for member in members:
            path=directory/member.name
            if any(p.is_symlink() for p in (path,*path.parents)):
                raise ValueError('Symlinked destination cannot retain evidence')
            body=archive.extractfile(member).read()
            if path.exists():
                if path.read_bytes()!=body:raise ValueError('Existing retained evidence differs')
            else:
                path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                with path.open('xb') as out:out.write(body)
                path.chmod(0o600)


def validate_local(manifest, inputs, frontend, node):
    expected=manifest['all_mode_admission']['manifest']['code_sha256']
    for name,digest in {**expected,**manifest['live_code_sha256']}.items():
        if sha256((ROOT/name).read_bytes())!=digest:
            raise ValueError('Local application or live harness differs from admission')
    if reviewed_private_inputs(inputs)!=manifest['private_inputs_sha256']:
        raise ValueError('Local original-based question review changed')
    if local_identity(frontend,node)!=manifest['configuration']['local_runtime']:
        raise ValueError('Local frontend, Node or browser identity changed')
    payload=(Path(inputs)/'requests.json').read_bytes()
    if sha256(payload)!=manifest['private_inputs_sha256']['requests.json']:
        raise ValueError('Local scheduled request bytes changed')
    return parse_requests(payload)


async def checked(command, *, payload=None):
    process=None
    async def spawn():
        nonlocal process
        process=await asyncio.create_subprocess_exec(*command,stdin=asyncio.subprocess.PIPE if payload is not None else asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
    try:
        await _join_owned(asyncio.create_task(spawn()))
        stdout,stderr=await _join_owned(asyncio.create_task(process.communicate(payload)),seconds=60)
        if process.returncode:raise RuntimeError('Private control/transfer command failed')
        return stdout
    finally:
        if process is not None:
            async def close():
                async def discard(stream):
                    while await stream.read(65536):pass
                drains=[asyncio.create_task(discard(stream)) for stream in (process.stdout,process.stderr)]
                try:await _reap(process,5)
                finally:await asyncio.gather(*drains)
            await _join_owned(asyncio.create_task(close()),cancel_on_interrupt=False)


def cluster_prefix(cluster):
    return [cluster['kubectl'],'--context',cluster['context'],'-n',cluster['namespace']]


async def check_pod(cluster):
    uid=(await checked(cluster_prefix(cluster)+['get','pod',cluster['pod'],'-o','jsonpath={.metadata.uid}'])).decode()
    if uid!=cluster['pod_uid']:raise ValueError('Target pod identity changed')


async def transfer_files(cluster, remote, files):
    """Argument-vector transport; existing remote bytes must agree exactly."""
    payload=json.dumps({name:body.hex() for name,body in files.items()}).encode()
    program="""import json,pathlib,sys,os
os.umask(0o077)
root=pathlib.Path(sys.argv[1])
for name,encoded in json.load(sys.stdin).items():
 rel=pathlib.PurePosixPath(name)
 if rel.is_absolute() or '..' in rel.parts: raise ValueError('Invalid transfer path')
 path=root/name
 if any(p.is_symlink() for p in (path,*path.parents)): raise ValueError('Symlinked transfer target')
 data=bytes.fromhex(encoded)
 path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 if path.exists():
  if path.read_bytes()!=data: raise ValueError('Existing evidence differs')
 else: path.open('xb').write(data)
"""
    await checked(cluster_prefix(cluster)+['exec','-i',cluster['pod'],'--',remote['python'],'-c',program,remote['output']],payload=payload)


async def collect_case(cluster,remote,index,directory):
    program="""import io,pathlib,sys,tarfile
root=pathlib.Path(sys.argv[1]);output=sys.stdout.buffer
with tarfile.open(fileobj=output,mode='w|gz') as archive:
 for path in sorted(root.rglob('*')):
  if path.is_symlink(): raise ValueError('Symlinked remote evidence')
  if path.is_file(): archive.add(path,arcname=path.relative_to(root).as_posix(),recursive=False)
"""
    payload=await checked(cluster_prefix(cluster)+['exec',cluster['pod'],'--',remote['python'],'-c',program,
                          str(PurePosixPath(remote['output'])/f'case-{index:02d}')])
    unpack_retained(payload,directory)


async def run_local(options):
    index=options['index']
    if type(index) is not int or not 0<=index<6:raise ValueError('One frozen case index required')
    manifest=strict_json(Path(options['manifest']).read_bytes())
    requests=validate_local(manifest,options['inputs'],options['frontend'],options['node'])
    output=Path(options['output']);directory=output/f'case-{index:02d}'
    if {p.name for p in output.glob('case-*')}!={f'case-{i:02d}' for i in range(index)}:
        raise ValueError('Local live sequence changed or attempt already exists')
    cluster,remote=options['cluster'],options['remote']
    remote_path=PurePosixPath(remote['output'])
    if ('..' in remote_path.parts or not any(remote_path.is_relative_to(root) and remote_path != PurePosixPath(root)
            for root in ('/tmp','/private/tmp'))):
        raise ValueError('Remote artifacts must stay in the admitted temporary evaluation directory')
    if {key:cluster[key] for key in ('context','namespace','pod','pod_uid')} != manifest['configuration']['cluster']:
        raise ValueError('Cluster target differs from frozen admission')
    await check_pod(cluster)
    # Remote admission will validate complete predecessor artifacts and both grades
    # before readiness. Transfer exact bytes, never overwrite its earlier captures.
    for prior in range(index):
        files={}
        for path in sorted((output/f'case-{prior:02d}').rglob('*')):
            if path.is_symlink():raise ValueError('Symlinked prior evidence')
            if path.is_file():files[path.relative_to(output).as_posix()]=path.read_bytes()
        await transfer_files(cluster,remote,files)
    identity={'manifest_sha256':sha256(json.dumps(manifest,sort_keys=True,allow_nan=False).encode()),
              'pod_uid':cluster['pod_uid'],'nonce':secrets.token_hex(24)}
    identity_name=f'identity-{index:02d}.json'
    await transfer_files(cluster,remote,{identity_name:json.dumps(identity).encode()})
    directory.mkdir(mode=0o700)
    # Runtime independently rejects a forged remaining budget through admit_case.
    calls=0;elapsed=0
    for prior in range(index):
        result=strict_json((output/f'case-{prior:02d}'/'result.json').read_bytes())
        calls+=result['native_call_count'];elapsed+=result['elapsed_seconds']
    seconds=manifest['active_seconds']-elapsed
    command=cluster_prefix(cluster)+['exec','-i',cluster['pod'],'--',remote['python'],
        str(PurePosixPath(remote['code'])/'scripts/live_query_command.py'),'run-case',
        '--preparation',remote['preparation'],'--manifest',remote['manifest'],'--output',remote['output'],
        '--index',str(index),'--identity',str(PurePosixPath(remote['output'])/identity_name)]
    def frontend(ready):
        return local_frontend(ready,**{key:cluster[key] for key in ('kubectl','context','namespace','pod')},
                              node=options['node'],frontend=options['frontend'],directory=directory)
    primary=None
    try:
        async with process_bridge(command,frontend,directory,identity=identity,active_seconds=seconds) as local:
            browser_options={'base':local['url'],'request':requests[index],'directory':str(directory),
                             'timeout_ms':int(seconds*1000)}
            write_private(directory/'browser-options.json',browser_options)
            with (directory/'browser-process.log').open('xb') as log:
                browser=None
                async def spawn_browser():
                    nonlocal browser
                    browser=await asyncio.create_subprocess_exec(options['node'],str(ROOT/'scripts/live_query_ui.mjs'),
                                  str(directory/'browser-options.json'),stdout=log,stderr=asyncio.subprocess.STDOUT,env=node_environment())
                try:
                    await _join_owned(asyncio.create_task(spawn_browser()))
                    if await browser.wait()!=0:raise RuntimeError('Actual built browser failed')
                finally:
                    if browser is not None:
                        await _join_owned(asyncio.create_task(_reap(browser,5)),cancel_on_interrupt=False)
            await check_pod(cluster)
    except BaseException as exc:
        primary=exc
    finally:
        try:await collect_case(cluster,remote,index,directory)
        except BaseException as exc:
            write_private(directory/'collection-error.json',{'exception_type':type(exc).__name__})
            if primary is None:primary=exc
    if primary is not None:raise primary
    validate_local(manifest,options['inputs'],options['frontend'],options['node'])
    write_private(directory/'result.json',capture_result(directory,manifest,requests[index],index))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('options')
    args=parser.parse_args();os.umask(0o077)
    asyncio.run(run_local(strict_json(Path(args.options).read_bytes())))


if __name__=='__main__':main()
