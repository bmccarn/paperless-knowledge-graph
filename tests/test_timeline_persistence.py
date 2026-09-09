"""Stored dates retain their exact accepted answer binding across API restoration."""
import copy
import json
import unittest
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import conversations
from app.answer_finalization import AnswerFinalizer
from tests.test_source_dates import ExactAuditor, pack
from tests import test_query_persistence as persistence


class TimelineHTTPTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = persistence.QueryPersistenceTests.asyncSetUp
    asyncTearDown = persistence.QueryPersistenceTests.asyncTearDown
    async def test_bound_timeline_metadata_is_identical_in_http_sse_and_storage(self):
        result=await AnswerFinalizer(ExactAuditor()).finalize(
            'Recorded history?', 'The service request was dated January 3, 2024.',
            pack('The service request was dated January 3, 2024.'),mode='timeline')
        payload={**persistence.final_payload(),**result,'mode':'timeline'}
        with patch('tests.test_query_persistence.final_payload',return_value=payload):
            ordinary=await self.client.post('/query',json={'question':'History?','conversation_id':'ordinary','mode':'timeline'})
            stream=await self.client.post('/query/stream',json={'question':'History?','conversation_id':'stream','mode':'timeline'})
        terminal=next(json.loads(line[6:]) for line in stream.text.splitlines()
                      if line.startswith('data: ') and json.loads(line[6:]).get('type')=='complete')
        self.assertEqual(ordinary.json()['timeline_events'],terminal['timeline_events'])
        for message in self.saved.messages:
            if message['role']=='assistant':
                self.assertEqual(message['metadata'],{key:payload.get(key) for key in persistence.METADATA_KEYS})


class TimelineRestoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_restores_valid_projection_and_suppresses_old_events_without_writing_rows(self):
        result=await AnswerFinalizer(ExactAuditor()).finalize(
            'Recorded history?', 'Service requested January 3, 2024.',pack('Service requested January 3, 2024.'),mode='timeline')
        now=datetime.now(timezone.utc)
        base={'id':uuid.uuid4(),'role':'assistant','content':result['answer'],'sources':None,'entities':None,
              'confidence':.8,'query_time_ms':1000,'cached':False,'follow_ups':None,'created_at':now}
        rows=[{**base,'metadata':json.dumps({**result,'mode':'timeline'})},
              {**base,'id':uuid.uuid4(),'metadata':json.dumps({'mode':'timeline','timeline_events':[
                  {'date':'2024-01-03','title':'Service completed','document_id':101}]})}]
        original=copy.deepcopy(rows)
        connection=SimpleNamespace(fetchrow=AsyncMock(return_value={'id':uuid.uuid4(),'title':'History','created_at':now,'updated_at':now}),
                                   fetch=AsyncMock(return_value=rows))
        @asynccontextmanager
        async def acquire():yield connection
        with patch.object(conversations,'_pool',SimpleNamespace(acquire=acquire)):
            saved=await conversations.get_conversation(str(uuid.uuid4()))
        self.assertEqual(saved['messages'][0]['timeline_events'],result['timeline_events'])
        self.assertEqual(saved['messages'][1]['timeline_events'],[])
        self.assertEqual(saved['messages'][1]['finalization']['timeline']['status'],'unavailable')
        self.assertEqual(rows,original)
