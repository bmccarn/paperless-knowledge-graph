"""Join owned async operations even when caller cancellation repeats."""
import asyncio


async def owned_call(awaitable, *, deadline=None):
    """Cancel the owned operation once; repeated caller cancellation cannot unjoin it."""
    task = asyncio.create_task(awaitable)
    try:
        async with asyncio.timeout_at(deadline):
            return await asyncio.shield(task)
    except BaseException:
        if not task.done() and not task.cancelling():task.cancel()
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError:continue
            except BaseException:break
        # Retrieve terminal exceptions even if cancellation arrived during cleanup.
        if task.done() and not task.cancelled():task.exception()
        raise
