"""Recent record discovery and expansion preserve distinct source opportunities."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.history_coverage import choose_documents, choose_recent_documents
from app.query import QueryEngine
from tests.test_history_coverage import HistoricalEngine, HistoryAuditor
from app.cache import invalidate_on_sync


class CurrentRecordCoverageTests(unittest.IsolatedAsyncioTestCase):
    def test_identifier_only_title_does_not_create_newest_calendar_period(self):
        rows = [{'document_id': 1, 'title': 'Invoice 8500 Lake Road', 'doc_type': 'invoice',
                 'preview': 'Invoice account 8500. Charge $320.'},
                {'document_id': 2, 'title': 'Invoice record', 'doc_type': 'invoice',
                 'preview': 'Invoice dated March 1, 2026. Charge $321.'},
                {'document_id': 3, 'title': 'Invoice record', 'doc_type': 'invoice',
                 'preview': 'Invoice dated September 1, 2026. Charge $421.'}]
        result = choose_documents(rows, 'Invoice history', limit=1)
        self.assertEqual(result[0]['document_id'], 3)
        self.assertNotIn('8500', result[0]['period'])

    def test_recent_families_preserve_identifiers_and_calendar_precision(self):
        rows = [{"document_id":1,"title":"Invoice account AX742 September 1, 2026", "preview":"Charge $30.","doc_type":"invoice"},
                {"document_id":2,"title":"Invoice account BY813 September 2026", "preview":"Charge $40.","doc_type":"invoice"},
                {"document_id":3,"title":"Invoice account AX742 August 1, 2026", "preview":"Charge $20.","doc_type":"invoice"}]
        selected=choose_recent_documents(rows, "Current invoices",limit=2)
        self.assertEqual({r["document_id"] for r in selected},{1,2})
        self.assertEqual(choose_recent_documents(list(reversed(rows)),"Current invoices",limit=2),selected)

    async def test_strict_current_query_discovers_both_subjects_outside_vector_top_k(self):
        for subject, kind, unit, mode in [(subject, kind, unit, mode) for subject, kind, unit in
                                         [("Invoice", "financial", "USD"), ("Laboratory", "medical", "mg")]
                                         for mode in ("strict", "timeline")]:
            old_text = f'{subject} Alpha dated January 1, 2022 records 20 {unit}.'
            alpha = f'{subject} Alpha dated September 1, 2026 records 30 {unit}.'
            beta = f'{subject} Beta dated September 2026 records 40 {unit}.'
            rows, chunks = [], []
            for did, title, text in [(1, f'Archived {subject}', old_text),
                                     (2, f'{subject} Alpha September 1, 2026', alpha),
                                     (3, f'{subject} Beta September 2026', beta)]:
                rows.append({'document_id':did, 'title':title, 'doc_type':kind, 'preview':text})
                chunks.append({'document_id':did, 'title':title, 'doc_type':kind, 'chunk_index':0,
                               'source_kind':'ocr', 'source_content':text, 'content':text, 'similarity':0.01})
            for did in range(20, 55):
                text=f'{subject} Alpha notice dated August 1, 2026 records 25 {unit}. '
                row={'document_id':did,'title':f'{subject} Alpha August 2026 notice','doc_type':kind,'preview':text}
                rows.append(row)
                for idx in range(12):
                    chunks.append({**row,'chunk_index':idx,'source_kind':'ocr','content':text*100,
                                   'source_content':text*100,'similarity':0.99})
            class Engine(HistoricalEngine):
                async def _retrieve(self, text): return {'vector_results':self.chunks[3:]}
                _retrieve_light = _retrieve
                async def _llm_generate(self, prompt):
                    self.prompt=prompt
                    payload=json.JSONDecoder().raw_decode(prompt.split('Canonical evidence pack used for this answer:\n',1)[1])[0]
                    supplied='\n'.join(s['content'] for s in payload['spans'])
                    self_test.assertIn(alpha,supplied)
                    self_test.assertIn(beta,supplied)
                    if mode == 'timeline':self_test.assertIn(old_text,supplied)
                    return (old_text+'\n' if mode=='timeline' else '')+alpha+'\n'+beta
            self_test=self
            engine=Engine(subject,chunks)
            async def hydrate(ids,**kwargs):
                result=[]
                for did in ids:
                    result.extend([c for c in chunks if c['document_id']==did][:kwargs.get('chunks_per_doc',2)])
                return result
            with self.subTest(subject=subject,mode=mode), ExitStack() as stack:
                invalidate_on_sync()
                stack.enter_context(patch('app.query.graph_store.get_document_dates',AsyncMock(return_value={})))
                discovery=stack.enter_context(patch('app.query.embeddings_store.historical_document_candidates',AsyncMock(return_value={'documents':rows,'candidate_count':len(rows),'truncated':False})))
                stack.enter_context(patch('app.query.embeddings_store.get_chunks_for_documents',side_effect=hydrate))
                stack.enter_context(patch.object(engine,'_expand_source_documents',AsyncMock(return_value=chunks[3:])))
                stack.enter_context(patch('app.query.embeddings_store.get_open_feedback_document_ids',AsyncMock(return_value=set())))
                stack.enter_context(patch('app.query.embeddings_store.get_incomplete_document_ids',AsyncMock(return_value=set())))
                stack.enter_context(patch('app.query.strands_orchestrator',HistoryAuditor()))
                result=await engine.query(f'What are my current {subject} records?',mode=mode)
                discovery.assert_awaited_once()
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertIn(alpha,result['answer'])
                self.assertIn(beta,result['answer'])
                self.assertTrue({2,3}.issubset({i['document_id'] for i in result['evidence_pack']['items']}))
