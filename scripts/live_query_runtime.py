"""Compose an admitted isolated query runtime; never start the serving lifespan.

The executable must revalidate its frozen prerequisites before calling this factory.
This module imports no application clients until entry and does not grant admission.
"""
import asyncio
import base64
import json
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import date
import time

from scripts.conservative_query_admission import strict_json
from scripts.eval_source_audit import write_private
from scripts.live_query_browser import browser_adapters
from scripts.live_query_capture import ModelCapture, CapturedClient
from scripts.live_query_corpus import current_corpus
from scripts.live_query_readers import attach_readers
from scripts.live_query_server import loopback_server
from scripts.live_query_session import delivery_session
from scripts.live_query_stages import capture_stages


def _freeze(value):
    return strict_json(json.dumps(value, allow_nan=False))


def _exception_types(exc):
    types, seen = [], set()
    while exc is not None and id(exc) not in seen and len(types) < 16:
        seen.add(id(exc)); types.append(type(exc).__name__)
        exc = exc.__cause__ or exc.__context__
    return types


@asynccontextmanager
async def query_runtime(request, directory, *, max_calls, seconds, expected_corpus, evaluated_at):
    """Open only after static admission; yield readiness and retain private artifacts.

    A new process is required for each case. Imported singleton clients are owned
    by that process too. The controller must signal stop only after browser checks;
    an early stop still tears down workers but cannot produce a qualifying result.
    """
    admitted = _freeze({'request': request, 'corpus': expected_corpus})
    request, expected_corpus = admitted['request'], admitted['corpus']
    if date.today().isoformat() != evaluated_at:
        raise ValueError('Live evaluation date changed')
    from app import main, query, cache
    from app.config import settings
    from app.cache import get_corpus_generation_async
    from app.paperless import PaperlessClient
    from app.strands_orchestrator import StrandsQueryOrchestrator
    from app.answer_coverage import restore_question_coverage

    if (main._startup_ready or main._background_workers or query.graph_store.driver is not None
            or query.embeddings_store.pool is not None or cache._corpus_generation._pending):
        raise ValueError('Live evaluation requires a fresh uninitialized process')
    capture = ModelCapture(directory, max_calls=max_calls, seconds=seconds)
    state = {'capture': capture, 'final': None, 'corpus_before': None, 'corpus_after': None}
    stages, readers, delivery = None, None, None
    started, error, exception_types = time.monotonic(), None, []
    try:
        async with AsyncExitStack() as stack:
            # This is the isolated process's unused imported engine, not a serving
            # instance; close it even if later attachment or preflight fails.
            stack.push_async_callback(main.query_engine.close)
            if cache._corpus_generation._redis is not None:
                stack.push_async_callback(asyncio.to_thread, cache._corpus_generation._redis.close)
            engine = query.QueryEngine(question_pipeline=True)
            stack.push_async_callback(engine.close)
            engine.client.max_retries = 0
            engine.client = CapturedClient(engine.client, capture, label='query', bypass_cache=True)
            orchestrator = StrandsQueryOrchestrator()
            stack.push_async_callback(orchestrator.close)
            if not orchestrator.enabled:
                raise ValueError('Frozen Strands runtime is unavailable')
            previous = query.strands_orchestrator
            stack.callback(setattr, query, 'strands_orchestrator', previous)
            query.strands_orchestrator = orchestrator
            inventory_client = PaperlessClient()
            with capture_stages(orchestrator, capture, directory, reader_inventory=True) as stages:
                async with attach_readers(capture, directory / 'originals') as readers:
                    async def snapshot():
                        return _freeze(await current_corpus(inventory_client, readers['graph_reads'],
                                                    readers['vector_reads'], get_corpus_generation_async))
                    state['corpus_before'] = await snapshot()
                    if state['corpus_before'] != expected_corpus or date.today().isoformat() != evaluated_at:
                        raise ValueError('Live corpus or date differs from the admitted snapshot')
                    def adapters(history):
                        return browser_adapters(history, request, paperless_url=settings.effective_paperless_external_url)
                    async with delivery_session(main, engine, request, adapters) as delivery:
                        async with loopback_server(delivery['app']) as server:
                            state.update(server)
                            yield {key: state[key] for key in ('port', 'url')}
                    # Server and route/query workers are joined before checking
                    # outcomes or restoring their read-only dependencies.
                    final = delivery['observed']['final']
                    if not delivery['observed']['guard'].started or final is None:
                        raise ValueError('Frozen browser request did not complete')
                    state['final'] = delivery['delivery'].conserved_final(final, settings.effective_paperless_external_url)
                    coverage = restore_question_coverage(state['final'])
                    if (coverage is None or coverage['status'] not in {'complete', 'partial'}
                            or state['final'].get('query_plan', {}).get('requirements_status') != 'complete'):
                        raise ValueError('Live planning or coverage execution is unavailable')
                    # Re-read fetched originals, never preload rubric documents.
                    for document_id in list(readers['documents'].documents):
                        await readers['documents'].get_document(document_id)
                    state['corpus_after'] = await snapshot()
                    if (state['corpus_after'] != expected_corpus or readers['documents'].changed
                            or readers['graph_reads'].denied or readers['vector_reads'].denied
                            or date.today().isoformat() != evaluated_at):
                        raise ValueError('Live corpus, date or read-only boundary changed')
                    capture.require_complete()
    except BaseException as exc:
        error = type(exc).__name__
        exception_types = _exception_types(exc)
        raise
    finally:
        # All resource scopes unwind first. Failed attempts and partially delivered
        # responses remain available for diagnosis; they are never resumed.
        capture.close_pending()
        if delivery is not None:
            state['engine_final'] = delivery['observed']['final']
            state['history'] = delivery['conversation'].messages
            state['delivery'] = {
                'statuses': list(delivery['delivery'].statuses),
                'completed': list(delivery['delivery'].completed),
                'bodies_base64': [base64.b64encode(body).decode('ascii')
                                  for body in delivery['delivery'].bodies]}
        state.update(error=error, exception_types=exception_types, elapsed_seconds=time.monotonic() - started,
                     native_call_count=len(capture.attempts), model_sha256=dict(capture.hashes),
                     stage_sha256=dict(stages['hashes']) if stages else {},
                     originals_sha256=dict(readers['documents'].hashes) if readers else {},
                     denied_graph_operations=list(readers['graph_reads'].denied) if readers else [],
                     denied_vector_operations=list(readers['vector_reads'].denied) if readers else [])
        write_private(directory / 'runtime.json', {key: value for key, value in state.items()
                                                   if key not in {'capture', 'url', 'port'}})
