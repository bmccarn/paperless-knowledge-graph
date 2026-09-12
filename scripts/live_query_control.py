"""Own a remote loopback evaluation lifetime through a private control stream.

The executable supplies an already admitted resource factory. It must nest server,
query workers, readers and captures so exiting the factory drains them in that order.
This module imports no application clients and grants no evaluation admission.
"""
import asyncio
import math


def _identity(identity):
    if (not isinstance(identity, dict) or set(identity) != {'manifest_sha256', 'pod_uid', 'nonce'}
            or any(not isinstance(value, str) or not value for value in identity.values())
            or len(identity['manifest_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in identity['manifest_sha256'])
            or len(identity['nonce']) < 32):
        raise ValueError('Bound remote control identity required')
    return dict(identity)


async def wait_stop(reader, identity):
    """EOF closes the owned runtime but can never establish a successful run."""
    line = await reader.readline()
    if not line:
        return 'control_eof'
    if len(line) > 2048:
        raise ValueError('Oversized control message')
    try:
        # Reject duplicate keys instead of accepting an ambiguous identity.
        from scripts.conservative_query_admission import strict_json
        message = strict_json(line)
    except (ValueError, UnicodeDecodeError):
        raise ValueError('Invalid control message') from None
    if message != {'command': 'stop', **identity}:
        raise ValueError('Unscheduled remote control')
    return 'stop'


async def _join_owned(task, *, seconds=None, cancel_on_interrupt=True):
    """Deliver at most one cancellation, then drain through owner recancellation."""
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
        if not done:
            raise TimeoutError('Remote active deadline expired')
    except BaseException as primary:
        if cancel_on_interrupt and not task.done() and not task.cancelling():
            task.cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                pass
            except BaseException:
                break
        try:
            task.result()
        except BaseException:
            pass
        raise primary
    return task.result()


async def own_runtime(factory, reader, emit, *, identity, seconds):
    """Acknowledge closure only after the resource context has fully exited.

    `emit` uses the private owner channel, never the public HTTP route. A dead channel
    may prevent acknowledgment; the local owner must treat that as unconfirmed
    cleanup and perform its own bounded teardown. The caller still validates final
    results, corpus and capture completeness; a stop acknowledgment is not a pass.
    """
    identity = _identity(identity)
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 3600:
        raise ValueError('Bounded remote lifetime required')
    ready, reason, failure = False, None, None
    manager, entered, cleanup_error = None, False, None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    try:
        manager = factory()
        # Entry owns partial-startup cleanup too. A deadline and later owner
        # cancellation must not deliver two cancellations into that cleanup.
        async def enter():
            nonlocal entered
            runtime = await manager.__aenter__()
            entered = True  # Commit ownership before the entry task can finish.
            return runtime
        runtime = await _join_owned(asyncio.create_task(enter()),
                                    seconds=max(0, deadline - loop.time()))
        port = runtime['port']
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('Loopback runtime port required')
        async def active():
            nonlocal ready
            await emit({'event': 'ready', **identity, 'port': port})
            ready = True
            return await wait_stop(reader, identity)
        reason = await _join_owned(asyncio.create_task(active()),
                                   seconds=max(0, deadline - loop.time()))
    except BaseException as exc:
        failure = exc
    if entered:
        cleanup = asyncio.create_task(manager.__aexit__(
            type(failure) if failure else None, failure,
            failure.__traceback__ if failure else None))
        try:
            # Active timeout has ended. The resource/process owner bounds shutdown;
            # caller cancellation cannot interrupt joins or restore readers early.
            await _join_owned(cleanup, cancel_on_interrupt=False)
        except BaseException as exc:
            cleanup_error = exc
            if failure is None:
                failure = exc
    receipt = {'event': 'closed' if failure is None else 'failed', **identity,
               'ready': ready, 'reason': reason,
               'exception_type': type(failure).__name__ if failure else None,
               'cleanup_exception_type': type(cleanup_error).__name__ if cleanup_error else None}
    try:
        async with asyncio.timeout(5):
            await emit(receipt)
    except BaseException:
        if failure is None:
            raise
    if failure is not None:
        raise failure
    return receipt
