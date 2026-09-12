"""Disposable local-only cluster transport/runtime for the assembled owner test."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


async def runtime_entry(args):
    from tests.runtime import configure_test_environment
    configure_test_environment()
    from app import main
    from tests.test_live_query_delivery import Engine
    from scripts.live_query_session import delivery_session
    from scripts.live_query_server import loopback_server
    from scripts.live_query_browser import browser_adapters
    from scripts.live_query_control import own_runtime
    from scripts.eval_source_audit import write_private
    identity=json.loads(Path(args[args.index('--identity')+1]).read_bytes())
    output=Path(args[args.index('--output')+1]);index=int(args[args.index('--index')+1])
    directory=output/f'case-{index:02d}';directory.mkdir(mode=0o700)
    request=json.loads((output/'synthetic-request.json').read_bytes());engine=Engine()
    # Actual results carry confidence; preserve the same restored copy controls.
    original=engine.query
    async def query(*args,**kwargs):
        final=await original(*args,**kwargs);final['confidence']=1.0;return final
    engine.query=query
    @asynccontextmanager
    async def factory():
        try:
            async with delivery_session(main,engine,request,lambda history:browser_adapters(history,request,paperless_url=main._get_paperless_url())) as delivery:
                async with loopback_server(delivery['app']) as server:yield server
        finally:
            final=delivery['observed']['final']
            if final is not None:final=delivery['delivery'].conserved_final(final,main._get_paperless_url())
            write_private(directory/'synthetic-runtime.json',{'calls':len(engine.calls),'final':final,
                'workers_joined':all(t.done() for t in delivery['workers'])})
    reader=asyncio.StreamReader();protocol=asyncio.StreamReaderProtocol(reader)
    transport,_=await asyncio.get_running_loop().connect_read_pipe(lambda:protocol,sys.stdin.buffer)
    async def emit(message):print(json.dumps(message),flush=True)
    try:await own_runtime(factory,reader,emit,identity=identity,seconds=60)
    finally:transport.close()


async def forward(args):
    local,remote=map(int,args[-1].split(':'))
    tasks=set()
    async def connect(reader,writer):
        task=asyncio.current_task();tasks.add(task);other=None
        async def pipe(source,target):
            while data:=await source.read(65536):target.write(data);await target.drain()
        try:
            upstream,other=await asyncio.open_connection('127.0.0.1',remote)
            copies=[asyncio.create_task(pipe(reader,other)),asyncio.create_task(pipe(upstream,writer))]
            try:await asyncio.wait(copies,return_when=asyncio.FIRST_COMPLETED)
            finally:
                for copy in copies:copy.cancel()
                await asyncio.gather(*copies,return_exceptions=True)
        finally:
            for stream in (writer,other):
                if stream:stream.close();await stream.wait_closed()
            tasks.remove(task)
    server=await asyncio.start_server(connect,'127.0.0.1',local)
    print(f'Forwarding from 127.0.0.1:{local} -> {remote}',flush=True)
    async with server:await server.serve_forever()


def main():
    args=sys.argv[1:]
    if args and args[0]=='run-case':asyncio.run(runtime_entry(args));return
    if 'get' in args:print('synthetic-pod',end='');return
    if 'port-forward' in args:asyncio.run(forward(args));return
    if 'exec' in args:
        command=args[args.index('--')+1:]
        os.execv(command[0],command)
    raise ValueError('Unexpected synthetic cluster command')


if __name__=='__main__':main()
