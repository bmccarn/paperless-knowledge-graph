"""Own a localhost pod forward and the already frozen standalone frontend."""
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import os
from pathlib import Path
import socket

import httpx

from scripts.live_query_process import _reap
from scripts.live_query_control import _join_owned


def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


@asynccontextmanager
async def child(command, directory, name, ready_line, *, env=None, cwd=None):
    """Require this child's own readiness banner, not another listener's response."""
    process, drain = None, None
    ready = asyncio.Event()
    capture_error = None
    with os.fdopen(os.open(directory / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'wb') as log:
        try:
            async def spawn():
                nonlocal process
                process = await asyncio.create_subprocess_exec(*command, cwd=cwd, env=env,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await _join_owned(asyncio.create_task(spawn()), seconds=45)
            async def capture():
                nonlocal capture_error
                pending = b''
                # Always drain pipe bytes: readline's size limit can stop draining
                # and deadlock Process.wait even after a successful SIGKILL.
                while chunk := await process.stdout.read(65536):
                    if capture_error is None:
                        try:
                            log.write(chunk); log.flush()
                        except OSError as exc:
                            capture_error = exc
                            ready.set()  # Wake startup so it can tear down the child.
                    pending += chunk
                    if ready_line in pending:
                        ready.set()
                    pending = pending[-max(1, len(ready_line) - 1):]
                if not ready.is_set():
                    raise RuntimeError('Local child exited before readiness')
            drain = asyncio.create_task(capture())
            waiter = asyncio.create_task(ready.wait())
            try:
                async with asyncio.timeout(45):
                    done, _ = await asyncio.wait({waiter, drain}, return_when=asyncio.FIRST_COMPLETED)
                    if drain in done:
                        await drain
                        raise RuntimeError('Local child closed during startup')
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
            if capture_error is not None:
                raise capture_error
            if process.returncode is not None:
                raise RuntimeError('Local child is not running')
            yield process
            if capture_error is not None:
                raise capture_error
            if process.returncode is not None or drain.done():
                raise RuntimeError('Owned local child exited during browser work')
        finally:
            async def close():
                try:
                    if process is not None:
                        await _reap(process, 5)
                finally:
                    if drain is not None:
                        await asyncio.gather(drain, return_exceptions=True)
            await _join_owned(asyncio.create_task(close()), cancel_on_interrupt=False)


@asynccontextmanager
async def local_frontend(ready, *, kubectl, context, namespace, pod,
                         node, frontend, directory):
    """Static assets must already be copied and identity-bound during preparation.

    Caller validates frozen build/runtime identities before this scope. All resources
    remain alive until the process bridge receives remote closure acknowledgment.
    Browser ownership belongs inside the caller's body; no query is submitted here.
    """
    frontend = Path(frontend)
    if not (frontend / '.next/standalone/server.js').is_file():
        raise ValueError('Frozen standalone frontend build required')
    if not (frontend / '.next/standalone/.next/static').is_dir():
        raise ValueError('Frozen standalone static assets required')
    backend_port, ui_port = free_port(), free_port()
    while ui_port == backend_port:
        ui_port = free_port()
    backend, base = f'http://127.0.0.1:{backend_port}', f'http://127.0.0.1:{ui_port}'
    forward_command = [str(kubectl), '--context', context, '-n', namespace, 'port-forward',
                       '--address=127.0.0.1', 'pod/' + pod, f'{backend_port}:{ready["port"]}']
    async with AsyncExitStack() as stack:
        forward = await stack.enter_async_context(child(forward_command, directory, 'forward.log',
                    f'Forwarding from 127.0.0.1:{backend_port} -> {ready["port"]}'.encode()))
        env = {**os.environ, 'BACKEND_URL': backend, 'NEXT_TELEMETRY_DISABLED': '1',
               'HOSTNAME': '127.0.0.1', 'PORT': str(ui_port)}
        ui = await stack.enter_async_context(child([str(node), '.next/standalone/server.js'],
                    directory, 'frontend.log', b'Ready in', env=env, cwd=frontend))
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            response = await client.get(base + '/api/_fixture')
            if response.status_code != 200 or response.json() != {'fixture': 'paperless-live-evaluation-v1'}:
                raise ValueError('Built frontend did not reach the isolated runtime')
        if forward.returncode is not None or ui.returncode is not None:
            raise RuntimeError('Owned forwarding or frontend exited before readiness')
        yield {'url': base, 'frontend': ui, 'forward': forward}
