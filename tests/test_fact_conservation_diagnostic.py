import json
import unittest
from scripts.eval_fact_conservation import parse_selection, parse_exclusion


def payload():
    return {'observations':[{'id':'a'},{'id':'b'}]}


def row(identity,status='delivered',target=None):
    return {'observation_id':identity,'status':status,'target_id':target}


class SelectionTests(unittest.TestCase):
    def test_full_accounting_and_direct_delivered_duplicate(self):
        data={'dispositions':[row('a'),row('b','duplicate_of','a')]}
        self.assertEqual(parse_selection(json.dumps(data),payload()),data)
        for rows in ([row('a')],[row('a'),row('a')],[row('a'),row('foreign')],
                     [row('a','duplicate_of','a'),row('b')],
                     [row('a','duplicate_of','b'),row('b','duplicate_of','a')],
                     [row('a','outside_request'),row('b','duplicate_of','a')],
                     [row('a','delivered','b'),row('b')],
                     [row('a','outside_request','b'),row('b')],
                     [row('a','duplicate_of'),row('b')],
                     [row('a'),row('b','unknown')],
                     [row('a'),row('b')|{'extra':True}]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                parse_selection(json.dumps({'dispositions':rows}),payload())

    def test_strict_transport_text_and_schema(self):
        good=json.dumps({'dispositions':[row('a'),row('b')]})
        for text in (None,'','null','false','{}','[]','{"dispositions":[]}',
                     'prefix '+good,good+' suffix','```json\n'+good+'\n```',
                     '{"dispositions":[],"dispositions":[]}'):
            with self.subTest(text=text),self.assertRaises((ValueError,TypeError)):
                parse_selection(text,payload())


class ExclusionTests(unittest.TestCase):
    def test_only_all_excluded_ids_have_decisions(self):
        p=payload()|{'proposal':{'dispositions':[row('a'),row('b','outside_request')]}}
        good={'decisions':[{'observation_id':'b','decision':'reject'}]}
        self.assertEqual(parse_exclusion(json.dumps(good),p),good)
        for rows in ([],[{'observation_id':'a','decision':'accept'}],
                     good['decisions']*2,[{'observation_id':'b','decision':None}],
                     [{'observation_id':'b','decision':'unknown'}],
                     [{'observation_id':'b','decision':'accept','extra':False}]):
            with self.subTest(rows=rows),self.assertRaises(ValueError):
                parse_exclusion(json.dumps({'decisions':rows}),p)
        for text in (None,'','null','{}','{"decisions":[],"decisions":[]}'):
            with self.subTest(text=text),self.assertRaises((ValueError,TypeError)):
                parse_exclusion(text,p)
        with self.assertRaises(ValueError):
            parse_exclusion(json.dumps(good),payload()|{'proposal':{'dispositions':[row('a'),row('b')]}})


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_frozen_bytes_failure_preservation_and_exclusive_run(self):
        from contextlib import contextmanager
        from pathlib import Path
        import tempfile
        from unittest.mock import AsyncMock,patch
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from scripts import eval_fact_conservation as probe
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path=root/'inputs.json';path.write_bytes(b'{"identity":"approved"}')
            admitted=[];executed=[]
            def admit(directory,*,payload):
                admitted.append(payload);path.write_bytes(b'{"identity":"replaced"}');return {'frozen':True}
            def calls(value):
                executed.append(value)
                return [{'id':str(i),'kind':'selection','payload':payload()} for i in range(15)]
            @contextmanager
            def stages(*args):yield {'attempts':[],'hashes':{}}
            good=json.dumps({'dispositions':[row('a'),row('b')]})
            orchestrator=AsyncMock();orchestrator._text_agent.side_effect=[None]+[good]*14
            with patch.object(probe,'manifest_for',side_effect=admit),patch.object(probe,'calls_for',side_effect=calls), \
                 patch.object(probe,'validate_native_call'),patch('scripts.live_query_stages.capture_stages',stages), \
                 patch('app.strands_orchestrator.StrandsQueryOrchestrator',return_value=orchestrator):
                await probe.execute(root,{'frozen':True})
            self.assertEqual(admitted,[b'{"identity":"approved"}'])
            self.assertEqual(executed,[b'{"identity":"approved"}'])
            result=json.loads((root/'results/result.json').read_bytes())
            self.assertFalse(result['complete_execution']);self.assertEqual(result['calls'][0]['status'],'failed')
            self.assertTrue(all(r['status']=='valid_ungraded' for r in result['calls'][1:]))
            self.assertEqual(orchestrator._text_agent.await_count,15)
            with patch.object(probe,'manifest_for',return_value={'frozen':True}), \
                 patch.object(probe,'calls_for',return_value=[]),self.assertRaises(FileExistsError):
                await probe.execute(root,{'frozen':True})

    async def test_cancelled_run_retains_failure_and_closes_orchestrator(self):
        import asyncio
        from contextlib import contextmanager
        from pathlib import Path
        import tempfile
        from unittest.mock import AsyncMock,patch
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from scripts import eval_fact_conservation as probe
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);(root/'inputs.json').write_bytes(b'{}')
            @contextmanager
            def stages(*args):yield {'attempts':[],'hashes':{}}
            orchestrator=AsyncMock();orchestrator._text_agent.side_effect=asyncio.CancelledError
            with patch.object(probe,'manifest_for',return_value={}), \
                 patch.object(probe,'calls_for',return_value=[{'id':'a','kind':'selection','payload':payload()}]), \
                 patch('scripts.live_query_stages.capture_stages',stages), \
                 patch('app.strands_orchestrator.StrandsQueryOrchestrator',return_value=orchestrator), \
                 self.assertRaises(asyncio.CancelledError):
                await probe.execute(root,{})
            result=json.loads((root/'results/result.json').read_bytes())
            self.assertEqual(result['interrupted'],'CancelledError');self.assertFalse(result['complete_execution'])
            orchestrator.close.assert_awaited_once()


class InputAdmissionTests(unittest.TestCase):
    def test_nested_fields_cannot_smuggle_gold_and_exclusions_are_single(self):
        from copy import deepcopy
        from scripts.eval_fact_conservation import calls_for
        source={'original_question':'What changed?','evaluated_at':'2026-09-09',
                'source_documents':[{'document_id':1,'title':'Synthetic',
                    'spans':[{'span_id':'s1','text':'Original source.'}]}],
                'observations':[{'id':v,'text':'Source observation '+v,'references':[{'span_id':'s1'}]} for v in ('a','b')]}
        calls=[{'id':str(i),'kind':'selection' if i<7 else 'exclusion','payload':deepcopy(source)} for i in range(15)]
        for c in calls[7:]:c['payload']['proposal']={'dispositions':[row('a'),row('b','outside_request')]}
        data={'version':1,'partition':'diagnostic-development','calls':calls}
        self.assertEqual(len(calls_for(json.dumps(data))),15)
        for where in ('document','span','zero_exclusions','two_exclusions','object_date','invalid_date','noncanonical_date'):
            changed=deepcopy(data)
            if where=='document':changed['calls'][0]['payload']['source_documents'][0]['gold']='deliver everything'
            if where=='object_date':changed['calls'][0]['payload']['evaluated_at']={'gold':'deliver all'}
            if where=='invalid_date':changed['calls'][0]['payload']['evaluated_at']='2026-02-30'
            if where=='noncanonical_date':changed['calls'][0]['payload']['evaluated_at']='20260909'
            if where=='span':changed['calls'][0]['payload']['source_documents'][0]['spans'][0]['gold']='deliver everything'
            if where=='zero_exclusions':changed['calls'][7]['payload']['proposal']={'dispositions':[row('a'),row('b')]}
            if where=='two_exclusions':changed['calls'][7]['payload']['proposal']={'dispositions':[row('a','outside_request'),row('b','outside_request')]}
            with self.subTest(where=where),self.assertRaises(ValueError):calls_for(json.dumps(changed))
