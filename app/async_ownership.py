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


async def bounded_map(operation, items, *, limit=3, return_exceptions=False):
    """Run bounded children, returning input order and joining every child on exit."""
    async def run():
        semaphore = asyncio.Semaphore(limit)
        async def invoke(item):
            async with semaphore:
                try:
                    return await operation(item)
                except Exception as exc:
                    if return_exceptions:
                        return exc
                    raise
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(invoke(item)) for item in items]
        return [task.result() for task in tasks]
    return await owned_call(run())
