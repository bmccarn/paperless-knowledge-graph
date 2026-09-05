import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO = next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p / 'app' / 'pipeline.py').exists())
sys.path.insert(0, str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
import app.query as query_module
from app.cache import invalidate_on_sync, get_corpus_generation_async
from tests.test_query_delivery import RetrievedEngine
from tests.test_answer_finalization import SupportedAuditor

async def main():
    invalidate_on_sync()
    engine = RetrievedEngine()
    with patch('app.query.strands_orchestrator', SupportedAuditor()), patch('app.query.embeddings_store.get_incomplete_document_ids', AsyncMock(return_value=set())), patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
        first = await engine.query('Recorded premium?')
        before = await get_corpus_generation_async()
        original_get = query_module.cache_get
        read_started, resume = asyncio.Event(), asyncio.Event()
        async def delayed_get(cache, key):
            value = await original_get(cache, key)
            if value is not None:
                read_started.set()
                await resume.wait()
            return value
        with patch('app.query.cache_get', delayed_get):
            request = asyncio.create_task(engine.query('Recorded premium?'))
            await read_started.wait()
            invalidate_on_sync()
            after = await get_corpus_generation_async()
            resume.set()
            result = await request
        print(json.dumps({'generation_changed_while_cache_read_pending':before != after, 'cached': result['cached'], 'disposition': result['finalization']['disposition'], 'complete':result['finalization']['complete'], 'answer':result['answer'], 'synthesis_calls':len(engine.calls)}, indent=2))
        assert before != after and result['cached'] and result['finalization']['disposition'] == 'supported'

asyncio.run(main())
