import json
import unittest
from scripts.eval_source_coverage import parse_gaps, strict_json


class GapProtocolTests(unittest.TestCase):
    def test_empty_report_is_only_explicit_valid_empty(self):
        self.assertEqual(parse_gaps('{"gaps":[]}', {'s1'}), [])
        for value in (None, '', ' ', 'null', 'false', '{}', '[]', '{"gaps":null}',
                      'prefix {"gaps":[]}', '{"gaps":[]} suffix', '```json\n{"gaps":[]}\n```',
                      '{"gaps":[],"gaps":[]}', '{"gaps":[],"extra":false}'):
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                parse_gaps(value, {'s1'})

    def test_local_reference_and_single_line_text_contract(self):
        good = {'text':'Missing source meaning', 'span_ids':['s1']}
        self.assertEqual(parse_gaps(json.dumps({'gaps':[good]}), {'s1'}), [good])
        for gap in ({}, {**good,'text':''}, {**good,'text':'a\nb'}, {**good,'text':False},
                    {**good,'span_ids':[]}, {**good,'span_ids':['s2']},
                    {**good,'span_ids':['s1','s1']}, {**good,'span_ids':[1]},
                    {**good,'extra':True}):
            with self.subTest(gap=gap), self.assertRaises(ValueError):
                parse_gaps(json.dumps({'gaps':[gap]}), {'s1'})
        with self.assertRaises(ValueError):
            parse_gaps(json.dumps({'gaps':[good,good]}), {'s1'})
        with self.assertRaises(ValueError):
            parse_gaps('{"gaps":[{"text":"a","text":"b","span_ids":["s1"]}]}', {'s1'})

    def test_non_json_constants_rejected(self):
        for value in ('NaN', 'Infinity', '-Infinity'):
            with self.assertRaises(ValueError): strict_json(value)


class CallOutcomeTests(unittest.TestCase):
    def test_unavailable_cannot_become_empty_and_previous_failure_does_not_hide_control(self):
        from pathlib import Path
        from types import SimpleNamespace
        import tempfile
        from scripts.eval_source_coverage import successful_call
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/'model-001-output.json').write_text(json.dumps({'chunks':[{'choices':[{
                'delta':{'content':'{"gaps":[]}'}, 'finish_reason':'stop'}]}]}))
            capture = SimpleNamespace(directory=root, attempts=[{},{}], pending={},
                                      failures={0:'EarlierTimeout'}, exhausted=False)
            stages = {'attempts':[{'native_result':{'stop_reason':'end_turn'}}]}
            self.assertEqual(successful_call(capture, stages, 1, '{"gaps":[]}', {'s1'}), [])
            for changes in ({'pending':{1:0}}, {'failures':{1:'IncompleteStream'}},
                            {'failures':{1:'TimeoutError'}}, {'failures':{1:'CancelledError'}},
                            {'exhausted':True}, {'attempts':[{}]}):
                modified = SimpleNamespace(**(vars(capture) | changes))
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    successful_call(modified, stages, 1, '{"gaps":[]}', {'s1'})
            with self.assertRaises(ValueError):
                successful_call(capture, {'attempts':[{}]}, 1, '{"gaps":[]}', {'s1'})
            for body,reason in [({'refusal':'No'},'stop'), ({'tool_calls':[{}]},'tool_calls'),
                                ({},'length'), ({},'content_filter')]:
                (root/'model-001-output.json').write_text(json.dumps({'chunks':[{
                    'choices':[{'delta':body,'finish_reason':reason}]}]}))
                with self.subTest(body=body,reason=reason), self.assertRaises(ValueError):
                    successful_call(capture, stages, 1, '{"gaps":[]}', {'s1'})


class ReviewIdentityTests(unittest.TestCase):
    def test_both_reviewers_must_be_named_and_distinct(self):
        from pathlib import Path
        import tempfile
        from scripts.eval_source_coverage import require_reviews, digest
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); payload=b'input'
            review={'verdict':'pass','input_sha256':digest(payload),'reviewer':'spec-agent'}
            (root/'inputs-spec-review.json').write_text(json.dumps(review))
            for identity in (None, '', ' ', False, 'spec-agent'):
                (root/'inputs-standards-review.json').write_text(json.dumps(review|{'reviewer':identity}))
                with self.subTest(identity=identity), self.assertRaises(ValueError):
                    require_reviews(root,payload,'inputs')
            (root/'inputs-standards-review.json').write_text(json.dumps(review|{'reviewer':'standards-agent'}))
            self.assertEqual(len(require_reviews(root,payload,'inputs')),2)
            with self.assertRaises(ValueError): require_reviews(root,b'changed','inputs')


class ExecutionSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_execution_uses_the_same_bytes_as_admission(self):
        from contextlib import contextmanager
        from pathlib import Path
        import tempfile
        from unittest.mock import AsyncMock, patch
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from scripts import eval_source_coverage as probe
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); path=root/'inputs.json'; path.write_text('{"identity":"approved"}')
            admitted=[]; executed=[]
            def admit(directory, *, payload):
                admitted.append(payload)
                path.write_text('{"identity":"changed after admission"}')
                return {'manifest':'frozen'}
            def calls(data):
                executed.append(data)
                return [{'case_id':'test','document_id':1,'payload':{
                    'original_document':{'spans':[{'span_id':'s1','text':'Original'}]}}}]*12
            @contextmanager
            def stages(*args):
                yield {'attempts':[], 'hashes':{}}
            orchestrator=AsyncMock()
            with patch.object(probe,'manifest_for',side_effect=admit), \
                 patch.object(probe,'calls_for',side_effect=calls), \
                 patch.object(probe,'successful_call',return_value=[]), \
                 patch('scripts.live_query_stages.capture_stages',stages), \
                 patch('app.strands_orchestrator.StrandsQueryOrchestrator',return_value=orchestrator):
                await probe.execute(root,{'manifest':'frozen'})
            self.assertEqual(admitted,[b'{"identity":"approved"}'])
            self.assertEqual(executed,[{'identity':'approved'}])
            self.assertEqual(orchestrator._text_agent.await_count,12)
            with self.assertRaises(FileExistsError), \
                 patch.object(probe,'manifest_for',return_value={'manifest':'frozen'}):
                await probe.execute(root,{'manifest':'frozen'})
