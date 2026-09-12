"""Supervise a private remote runtime and keep local forwarding alive until close.

Commands and the local resource factory come from the admitted executable. This
module opens no application clients and does not grant admission or model access.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import math
import os

from scripts.conservative_query_admission import strict_json
from scripts.eval_source_audit import write_private
from scripts.live_query_control import _identity, _join_owned


async def _message(process, identity, timeout):
    async with asyncio.timeout(timeout):
        line = await process.stdout.readline()
    if not line or len(line) > 4096:
        raise ValueError('Missing or oversized remote control response')
    value = strict_json(line)
    if not isinstance(value, dict) or any(value.get(k) != v for k, v in identity.items()):
        raise ValueError('Remote control identity differs from admission')
    return value


async def _reap(process, seconds):
    """Bound local child shutdown; forced exit is never a remote cleanup receipt."""
    if process.returncode is not None:
        return False
    try:
        process.terminate()
    except ProcessLookupError:
        await process.wait()
        return False
    try:
        async with asyncio.timeout(seconds):
            await process.wait()
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.wait()
    return True


@asynccontextmanager
async def process_bridge(command, local_factory, directory, *, identity,
                         active_seconds, shutdown_seconds=30):
    """Yield local browser readiness, then close remote before local forwarding.

    The local factory must own and join all its children, including partial startup.
    A body error sends EOF instead of success. Only an exact stop acknowledgment and
    clean child exit confirm remote closure; even that is not a qualifying result.
    All logs stay private. Caller must separately validate runtime and browser output.
    """
    identity = _identity(identity)
    for value in (active_seconds, shutdown_seconds):
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 3600:
            raise ValueError('Bounded process deadlines required')
    if not command or any(not isinstance(arg, str) or not arg for arg in command):
        raise ValueError('Explicit subprocess argument vector required')
    process, manager, entered = None, None, False
    primary, cleanup_failure = None, None
    report = {'identity': identity, 'ready': None, 'closed': None,
              'remote_cleanup_confirmed': False, 'forced_local_exit': False,
              'exception_type': None, 'cleanup_exception_type': None, 'returncode': None}
    loop = asyncio.get_running_loop(); deadline = loop.time() + active_seconds
    with os.fdopen(os.open(directory / 'remote-stderr.log',
                           os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stderr:
        try:
            async def spawn():
                nonlocal process
                process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE, stderr=stderr, limit=4096)
            await _join_owned(asyncio.create_task(spawn()),
                              seconds=max(0, deadline - loop.time()))
            ready = await _message(process, identity, max(0, deadline - loop.time()))
            if (set(ready) != {'event', *identity, 'port'} or ready['event'] != 'ready'
                    or type(ready['port']) is not int or not 1 <= ready['port'] <= 65535):
                raise ValueError('Invalid remote readiness')
            report['ready'] = ready
            manager = local_factory(dict(ready))
            async def enter():
                nonlocal entered
                value = await manager.__aenter__()
                entered = True
                return value
            local = await _join_owned(asyncio.create_task(enter()),
                                      seconds=max(0, deadline - loop.time()))
            async with asyncio.timeout(max(0, deadline - loop.time())) as active:
                yield local
            if active.expired() or loop.time() >= deadline:
                raise TimeoutError('Browser exceeded the active deadline')
            if asyncio.current_task().cancelling():
                raise asyncio.CancelledError
        except BaseException as exc:
            primary = exc
        finally:
            async def close():
                nonlocal cleanup_failure
                try:
                    if process is not None:
                        try:
                            async with asyncio.timeout(shutdown_seconds):
                                if primary is None:
                                    process.stdin.write((json.dumps({'command': 'stop', **identity}) + '\n').encode())
                                    await process.stdin.drain()
                                process.stdin.close()
                                closed = await _message(process, identity, shutdown_seconds)
                                report['closed'] = closed
                                expected = {'event', *identity, 'ready', 'reason',
                                            'exception_type', 'cleanup_exception_type'}
                                await process.wait()
                                extra_output = await process.stdout.read()
                                if (set(closed) != expected or closed['event'] != 'closed'
                                        or closed['ready'] is not True or closed['reason'] != 'stop'
                                        or closed['exception_type'] is not None
                                        or closed['cleanup_exception_type'] is not None):
                                    raise ValueError('Remote runtime did not confirm clean explicit closure')
                                # Extra protocol output or nonzero exit invalidates an ACK.
                                if process.returncode != 0 or extra_output:
                                    raise ValueError('Remote process did not exit cleanly')
                                report['remote_cleanup_confirmed'] = True
                        except BaseException as exc:
                            cleanup_failure = exc
                        finally:
                            report['forced_local_exit'] = await _reap(process, shutdown_seconds)
                            report['returncode'] = process.returncode
                except BaseException as exc:
                    if cleanup_failure is None:
                        cleanup_failure = exc
                finally:
                    if entered:
                        try:
                            await manager.__aexit__(type(primary) if primary else None, primary,
                                                    primary.__traceback__ if primary else None)
                        except BaseException as exc:
                            if cleanup_failure is None:
                                cleanup_failure = exc
            try:
                await _join_owned(asyncio.create_task(close()), cancel_on_interrupt=False)
            except BaseException as exc:
                if primary is None:
                    primary = exc
            report['exception_type'] = type(primary).__name__ if primary else None
            report['cleanup_exception_type'] = type(cleanup_failure).__name__ if cleanup_failure else None
            write_private(directory / 'process.json', report)
    if primary is not None:
        raise primary
    if cleanup_failure is not None:
        raise cleanup_failure
