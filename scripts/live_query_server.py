"""A loopback-only ASGI server whose listener and requests have one owner."""
import asyncio
from contextlib import asynccontextmanager, nullcontext
import socket


@asynccontextmanager
async def loopback_server(app):
    """Nest inside delivery_session, so route workers outlive request teardown.

    The enclosing command owns its stop signal/deadline. Uvicorn must not re-raise
    SIGTERM and terminate the process before artifacts and readers are closed.
    """
    import uvicorn

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server, task = None, None
    try:
        listener.bind(('127.0.0.1', 0))
        listener.listen(128)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port,
                                lifespan='off', log_level='warning', timeout_graceful_shutdown=2))
        server.capture_signals = lambda: nullcontext()
        task = asyncio.create_task(server.serve(sockets=[listener]))
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    task.result()
                    raise RuntimeError('Evaluation server exited before readiness')
                await asyncio.sleep(0.01)
        yield {'url': f'http://127.0.0.1:{port}', 'port': port}
    finally:
        try:
            if server is not None:
                server.should_exit = True
                try:
                    if task is not None:
                        try:
                            await asyncio.wait_for(asyncio.shield(task), timeout=5)
                        except (TimeoutError, asyncio.CancelledError):
                            server.force_exit = True
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                            raise
                finally:
                    # Also own failed/startup/forced-shutdown paths, not just serve's
                    # normal return. Stop accepting before draining ASGI tasks.
                    listeners = getattr(server, 'servers', [])
                    for owned in listeners:
                        owned.close()
                    for connection in list(server.server_state.connections):
                        connection.shutdown()
                    requests = list(server.server_state.tasks)
                    for request in requests:
                        if not request.done() and not request.cancelling():
                            request.cancel()
                    if requests:
                        await asyncio.gather(*requests, return_exceptions=True)
                    await asyncio.gather(*(owned.wait_closed() for owned in listeners))
        finally:
            listener.close()
