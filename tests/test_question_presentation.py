"""Rejected retrieval interpretations must not escape through auxiliary UI fields."""
import copy
import json
import unittest
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests import test_question_pipeline as pipeline_controls
from app.answer_coverage import restore_pipeline_metadata
from app.cache import invalidate_on_sync


UNSAFE = ['What coverage remains following the completed cancellation?']
NEUTRAL = ['Which source documents should I review?',
           'What information could not be established from the sources?']
INDEX_SOURCE = {'document_id': 101, 'title': 'Indexed title', 'date': 'March 9, 2026',
                'date_signals': {'effective_date': '2026-03-09'},
                'excerpt': 'Document: indexed title\nDate: March 9, 2026\nWrong index header'}


class QuestionPresentationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = pipeline_controls.QuestionPipelineTests.asyncSetUp
    asyncTearDown = pipeline_controls.QuestionPipelineTests.asyncTearDown
    model = pipeline_controls.QuestionPipelineTests.model

    async def poisoned_query(self, *, failed=False, stream=False):
        self.fail_stage = 'source_reader' if failed else None
        async def gaps(question, context, history, mode, broad=False):
            return {'follow_up_suggestions': UNSAFE}, context, [], []
        with patch.object(self.engine, '_gap_review', side_effect=gaps), \
                patch.object(self.engine, '_build_sources', return_value=copy.deepcopy([INDEX_SOURCE])):
            if stream:
                return [e async for e in self.engine.query_stream('What monthly premium is recorded?', mode='strict')][-1]
            return await self.engine.query('What monthly premium is recorded?', mode='strict')

    def assert_safe(self, result, *, failed=False):
        self.assertEqual(result['follow_up_suggestions'], NEUTRAL)
        for key in ('latest_source_date', 'latest_supporting_source_date', 'latest_retrieved_source_date'):
            self.assertIsNone(result['source_summary'].get(key))
        for source in result['sources']:
            self.assertNotIn('date', source)
            self.assertNotIn('date_signals', source)
            self.assertNotIn('Wrong index header', source['excerpt'])
        if failed:
            self.assertEqual(result['sources'], [])
        else:
            self.assertEqual(result['sources'][0]['excerpt'], 'Monthly premium: $321.00 USD.')

    async def test_success_and_failure_http_style_and_sse_do_not_publish_draft_authority(self):
        for failed in (False, True):
            for stream in (False, True):
                with self.subTest(failed=failed, stream=stream):
                    invalidate_on_sync()
                    result = await self.poisoned_query(failed=failed, stream=stream)
                    self.assert_safe(result, failed=failed)

    async def test_cache_hit_rebuilds_presentation_without_another_model_call(self):
        result = await self.poisoned_query()
        result['follow_up_suggestions'] = UNSAFE
        result['source_summary']['latest_source_date'] = 'March 9, 2026'
        result['sources'] = copy.deepcopy([INDEX_SOURCE])
        calls = len(self.calls)
        with patch('app.query.cache_get', AsyncMock(return_value=result)):
            restored = await self.engine.query('What monthly premium is recorded?', mode='strict')
        self.assertTrue(restored['cached'])
        self.assertEqual(len(self.calls), calls)
        self.assert_safe(restored)

    async def test_metadata_failure_shortcut_removes_unsafe_summary_and_suggestions(self):
        result = await self.poisoned_query(failed=True)
        result['follow_up_suggestions'] = UNSAFE
        result['source_summary']['latest_source_date'] = 'March 9, 2026'
        original = copy.deepcopy(result)
        restored = restore_pipeline_metadata(result, result['answer'])
        self.assert_safe(restored, failed=True)
        self.assertEqual(result, original)

    async def test_saved_followup_column_is_sanitized_for_old_failed_pipeline(self):
        from app import conversations
        result = await self.poisoned_query(failed=True)
        result['source_summary']['latest_source_date'] = 'March 9, 2026'
        now = datetime.now(timezone.utc)
        row = {'id': uuid.uuid4(), 'role': 'assistant', 'content': result['answer'],
               'sources': json.dumps([INDEX_SOURCE]), 'entities': None, 'confidence': 0,
               'query_time_ms': 10, 'cached': False, 'follow_ups': json.dumps(UNSAFE),
               'metadata': json.dumps(result), 'created_at': now}
        original = copy.deepcopy(row)
        connection = SimpleNamespace(fetchrow=AsyncMock(return_value={
            'id': uuid.uuid4(), 'title': 'Saved failure', 'created_at': now, 'updated_at': now}),
            fetch=AsyncMock(return_value=[row]))
        @asynccontextmanager
        async def acquire():
            yield connection
        with patch.object(conversations, '_pool', SimpleNamespace(acquire=acquire)):
            saved = await conversations.get_conversation(str(uuid.uuid4()))
        message = saved['messages'][0]
        self.assertEqual(message['follow_ups'], NEUTRAL)
        self.assertEqual(message['sources'], [])
        self.assertIsNone(message['source_summary'].get('latest_source_date'))
        self.assertEqual(message['content'], result['answer'])
        self.assertEqual(row, original)
