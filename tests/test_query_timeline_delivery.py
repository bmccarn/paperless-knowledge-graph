"""Finalized timeline dates share answer sources, feedback and delivery gates."""
import copy
import unittest
from unittest.mock import AsyncMock, patch
from tests.test_query_delivery import RetrievedEngine
from tests.test_answer_finalization import PACK
from app.cache import invalidate_on_sync
from app.timeline import restore_timeline

MEETING_QUOTE = 'Meeting date: 2024-02-29.'

class TimelineEngine(RetrievedEngine):
    async def _build_evidence_pack(self,*args,**kwargs):
        result=copy.deepcopy(PACK)
        result['items'].append({'id':'meeting-tail','document_id':102,'title':'Meeting record','chunk_index':0,
                               'content':'irrelevant filler '*1500 + MEETING_QUOTE,'excerpt':'irrelevant filler'})
        return result
    async def _final_synthesis(self,*args,**kwargs):
        self.calls.append('synthesis')
        return {'answer':'Monthly premium: $321.00 USD.\n'+MEETING_QUOTE}

class TimelineAuditor:
    async def repair_answer(self,*args):
        return {'observations':['The recorded monthly premium is $321.00 USD.', 'The recorded meeting date is 2024-02-29.']}
    async def audit_answer_units(self,question,units,spans,plan):
        result=[]
        for unit in units:
            quote=MEETING_QUOTE if 'meeting' in unit['text'].lower() else 'Monthly premium: $321.00 USD.'
            span=next((s for s in spans if quote in s['content'] and not s['feedback_open']),None)
            result.append({'unit_id':unit['id'],'status':'supported' if span else 'missing','temporal_scope':'historical',
                           'references':[{'span_id':span['span_id'],'evidence_id':span['evidence_id'],
                               'document_id':span['document_id'],'quote':quote}] if span else []})
        return {'assessments':result}

class TimelineDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        invalidate_on_sync();self.engine=TimelineEngine();self.flags=AsyncMock(return_value=set())
        self.patches=[patch('app.query.strands_orchestrator',TimelineAuditor()),
                      patch('app.query.embeddings_store.get_incomplete_document_ids',AsyncMock(return_value=set())),
                      patch('app.query.embeddings_store.get_open_feedback_document_ids',self.flags)]
        for p in self.patches:p.start()
    async def asyncTearDown(self):
        for p in reversed(self.patches):p.stop()

    async def test_tail_quote_and_all_dates_survive_public_stream_cache_and_restore(self):
        result=await self.engine.query('Recorded premium and meeting timeline?',mode='timeline')
        self.assertEqual(result['finalization']['disposition'],'supported')
        self.assertEqual({s['document_id'] for s in result['sources']},{101,102})
        event=result['timeline_events'][0]
        self.assertEqual(event['references'][0]['document_id'],102)
        item=next(i for i in result['evidence_pack']['items'] if i['document_id']==102)
        self.assertEqual(item['excerpt'],MEETING_QUOTE)
        self.assertGreater(item['support_spans'][0]['start'],18000)
        streamed=[e async for e in self.engine.query_stream('Recorded premium and meeting timeline?',mode='timeline')]
        self.assertTrue(streamed[-1]['cached'])
        self.assertEqual(streamed[-1]['timeline_events'],result['timeline_events'])
        self.assertEqual(restore_timeline(result)[0],result['timeline_events'])
        self.assertEqual(len(self.engine.calls),1)

    async def test_feedback_prevents_unaccepted_timeline_source_resurrection(self):
        self.flags.return_value={102}
        result=await self.engine.query('Recorded premium and meeting timeline?',mode='timeline')
        self.assertEqual(result['timeline_events'],[])
        self.assertEqual({s['document_id'] for s in result['sources']},{101})
        self.assertEqual(result['finalization']['disposition'],'partial')
        self.assertFalse(result['cached'])

    async def test_retrieved_date_absent_from_final_answer_is_not_a_timeline_fact(self):
        with patch.object(self.engine,'_final_synthesis',AsyncMock(return_value={'answer':'Monthly premium: $321.00 USD.'})):
            result=await self.engine.query('Recorded history?',mode='timeline')
        self.assertEqual(result['timeline_events'],[])
        self.assertEqual(result['finalization']['timeline']['status'],'no_dates')
        self.assertEqual({s['document_id'] for s in result['sources']},{101})

    async def test_snapshot_change_clears_timeline_and_cache_eligibility(self):
        with patch('app.query.embeddings_store.get_incomplete_document_ids',AsyncMock(return_value={102})):
            result=await self.engine.query('Recorded history?',mode='timeline')
        self.assertEqual(result['timeline_events'],[])
        self.assertEqual(result['finalization']['disposition'],'corpus_changed')
        self.assertFalse(self.engine._cacheable_answer(result,'timeline'))
        self.assertEqual(result['finalization']['timeline']['status'],'unavailable')

    async def test_invalid_projection_is_not_cacheable_but_keeps_verified_answer(self):
        result=await self.engine.query('Recorded history?',mode='timeline')
        result['timeline_events'][0]['claim']='A different action happened.'
        self.assertFalse(self.engine._cacheable_answer(result,'timeline'))
        self.assertIn(MEETING_QUOTE,result['answer'])
