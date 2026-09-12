"""Own actual query-route workers before isolated readers and capture are closed."""
import asyncio
from contextlib import asynccontextmanager

from scripts.live_query_delivery import DeliveredEvents, PrivateConversation, guard_query
from scripts.live_query_evaluation import EvaluationRoutes


@asynccontextmanager
async def delivery_session(main, engine, request, adapters_for):
    """The caller must stop its server before leaving this inner context.

    Enter inside reader/model capture contexts, so worker cancellation and joins
    finish while their dependencies are still attached. Never enter serving
    startup, or close another process's clients. The real stream implementation
    owns and joins its inner query task when its route worker is cancelled.
    """
    if main._startup_ready or main._background_workers:
        raise ValueError('Delivery evaluation requires an idle isolated application')
    previous = (main.query_engine, main.conversations, main._start_background_worker)
    history = PrivateConversation(request.get('history', []))
    owned = set()
    closing = False

    def start(coro):
        if closing:
            coro.close()
            raise RuntimeError('Evaluation delivery is closing')
        task = previous[2](coro)
        # Keep completed tasks as well, so teardown observes every owned result.
        owned.add(task)
        return task

    boundary = DeliveredEvents(EvaluationRoutes(main.app, adapters=adapters_for(history)))

    async def app(scope, receive, send):
        if closing:
            await EvaluationRoutes.reject(send, 403)
            return
        await boundary(scope, receive, send)

    with guard_query(engine, request) as observed:
        main.query_engine, main.conversations, main._start_background_worker = engine, history, start
        try:
            yield {'app': app, 'delivery': boundary, 'observed': observed,
                   'conversation': history, 'workers': owned}
        finally:
            closing = True
            try:
                for task in owned:
                    if not task.done():
                        task.cancel()
                if owned:
                    await asyncio.gather(*owned, return_exceptions=True)
            finally:
                main.query_engine, main.conversations, main._start_background_worker = previous
