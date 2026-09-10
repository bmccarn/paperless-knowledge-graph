"""Fail live continuation before resources when captures or reviews change."""
import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.live_query_admission import admit_case, capture_result, expected_request_identity
from scripts.live_query_evaluation import FAILURE_COUNTS, sha256


class LiveContinuationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.output=self.root/'run';self.output.mkdir()
        self.inputs=self.root/'inputs';self.inputs.mkdir()
        self.requests=[{'id':str(i),'question':f'Question {i}','mode':mode,'model':'synthetic',
                       'history':[{'role':'user','content':'Antecedent'}] if i==4 else []}
                      for i,mode in enumerate(('strict','quick','deep','timeline','deep','strict'))]
        self.write(self.inputs/'requests.json',{'version':1,'cases':self.requests})
        self.manifest={'corpus_snapshot':{'generation':'redis:1'},'max_model_calls':300,'active_seconds':3600,
                       'configuration':{'paperless_url':'http://synthetic.invalid', 'strands_model':'synthetic',
                                        'gemini_model':'synthetic','source_date_order':'MDY'},
                       'evaluated_at':'2026-09-10',
                       'private_inputs_sha256':{'requests.json':sha256((self.inputs/'requests.json').read_bytes())}}
        self.restore=patch('app.answer_coverage.restore_question_coverage',return_value={'status':'complete'})
        self.restore.start();self.addCleanup(self.restore.stop)
        # Actual final/SSE semantics have their own delivery/runtime integration
        # controls. This fixture isolates artifact and continuation admission.
        self.delivery=patch('scripts.live_query_delivery.DeliveredEvents.conserved_final',
                            return_value={'query_plan':{'requirements_status':'complete'}})
        self.delivery.start();self.addCleanup(self.delivery.stop)
        self.prepare=patch('scripts.live_query_admission.prepare_manifest',return_value=copy.deepcopy(self.manifest))
        self.prepare.start();self.addCleanup(self.prepare.stop)

    def write(self,path,value):
        path.write_text(json.dumps(value))

    def fixture(self):
        directory=self.output/'case-00';directory.mkdir();(directory/'originals').mkdir()
        raw={
            'model-000-input.json':{'index':0,'kind':'query:chat','request':{'model':'synthetic','messages':[]}},
            'stage-000-input.json':{'index':0,'name':'source_auditor','prompt':'synthetic','system_prompt':'synthetic','response_format':None},
            'originals/document-0000.json':{'id':17,'content':'Synthetic original'},
        }
        raw['model-000-output.json']={**raw['model-000-input.json'],'status':'completed',
                                      'response':{'choices':[{'index':0,'finish_reason':'stop'}]}}
        raw['stage-000-output.json']={**raw['stage-000-input.json'],'response':'Synthetic stage output'}
        for name,value in raw.items():self.write(directory/name,value)
        (directory/'desktop.png').write_bytes(b'\x89PNG\r\n\x1a\nsynthetic fixture')
        body=b'data: {"type":"complete","answer":"Synthetic"}\n\n'
        manifestsha=sha256(json.dumps(self.manifest,sort_keys=True,allow_nan=False).encode())
        identity={'manifest_sha256':manifestsha,'pod_uid':'synthetic','nonce':'b'*32}
        process={'identity':identity,'remote_cleanup_confirmed':True,'exception_type':None,
                 'cleanup_exception_type':None,'forced_local_exit':False,'returncode':0,
                 'ready':{'event':'ready',**identity,'port':1234},
                 'closed':{'event':'closed',**identity,'ready':True,'reason':'stop','exception_type':None,'cleanup_exception_type':None}}
        self.write(directory/'process.json',process)
        self.write(directory/'browser.json',{'completed':True,'request':self.requests[0],'page_errors':[],
                'submission_count':1,'sse_sha256':sha256(body),'screenshots_sha256':{'desktop.png':sha256((directory/'desktop.png').read_bytes())}})
        self.write(directory/'runtime.json',{'error':None,'exception_types':[],'denied_graph_operations':[],
                'denied_vector_operations':[],'final':{'query_plan':{'requirements_status':'complete'}},'engine_final':{'question':self.requests[0]['question'],'mode':self.requests[0]['mode'],
                    'query_plan':{'original_question':self.requests[0]['question'],
                                  'request_identity_digest':expected_request_identity(self.manifest,self.requests[0])},
                    'finalization':{'request_identity_digest':expected_request_identity(self.manifest,self.requests[0])}},
                'delivery':{'statuses':[200],'completed':[True],'bodies_base64':[base64.b64encode(body).decode()]},
                'corpus_before':self.manifest['corpus_snapshot'],'corpus_after':self.manifest['corpus_snapshot'],
                'model_sha256':{n:sha256((directory/n).read_bytes()) for n in raw if n.startswith('model-')},
                'stage_sha256':{n:sha256((directory/n).read_bytes()) for n in raw if n.startswith('stage-')},
                'originals_sha256':{'document-0000.json':sha256((directory/'originals/document-0000.json').read_bytes())},
                'native_call_count':1,'elapsed_seconds':1.5})
        return directory

    def package(self,directory):return capture_result(directory,self.manifest,self.requests[0],0)

    def grade(self,directory):
        result=self.package(directory);self.write(directory/'result.json',result)
        digest=sha256((directory/'result.json').read_bytes())
        grade={'verdict':'pass','result_sha256':digest,'coverage_underreported_aspects':0,**{k:0 for k in FAILURE_COUNTS}}
        for axis in ('spec','standards'):self.write(directory/f'grade-{axis}.json',grade)
        self.write(directory/'review.json',{'result_sha256':digest,'spec':'pass','standards':'pass',
            'reviewers':['one','two'],'coverage_underreported_aspects':0,**{k:0 for k in FAILURE_COUNTS},
            'grade_sha256':{axis:sha256((directory/f'grade-{axis}.json').read_bytes()) for axis in ('spec','standards')}})

    def admit(self,index):return admit_case(self.manifest,index=index,output=self.output,inputs=self.inputs)

    def test_initial_admission_and_changed_prerequisites(self):
        self.assertEqual(self.admit(0)['request'],self.requests[0])
        with patch('scripts.live_query_admission.prepare_manifest',return_value={'changed':True}):
            with self.assertRaises(ValueError):self.admit(0)
        (self.output/'case-00').mkdir()
        with self.assertRaises(ValueError):self.admit(0)

    def test_exact_prior_case_and_independent_grades_allow_remaining_budget(self):
        directory=self.fixture();self.grade(directory)
        admitted=self.admit(1)
        self.assertEqual(admitted['max_calls'],299);self.assertEqual(admitted['seconds'],3598.5)
        (directory/'grade-spec.json').unlink()
        with self.assertRaises(FileNotFoundError):self.admit(1)

    def test_changed_raw_and_rewritten_result_cannot_reuse_prior_grade(self):
        directory=self.fixture();self.grade(directory)
        path=directory/'desktop.png';path.write_bytes(path.read_bytes()+b'changed')
        with self.assertRaises(ValueError):self.admit(1)
        browser=json.loads((directory/'browser.json').read_bytes());browser['screenshots_sha256']['desktop.png']=sha256(path.read_bytes())
        self.write(directory/'browser.json',browser);self.write(directory/'result.json',self.package(directory))
        with self.assertRaises(ValueError):self.admit(1)

    def test_native_truncation_is_not_completion_even_with_updated_hash(self):
        directory=self.fixture();path=directory/'model-000-output.json';raw=json.loads(path.read_bytes())
        raw['response']['choices'][0]['finish_reason']='length';self.write(path,raw)
        runtime=json.loads((directory/'runtime.json').read_bytes());runtime['model_sha256'][path.name]=sha256(path.read_bytes())
        self.write(directory/'runtime.json',runtime)
        with self.assertRaisesRegex(ValueError,'termination'):self.package(directory)

    def test_browser_delivery_tamper_or_duplicate_cannot_qualify(self):
        directory=self.fixture();path=directory/'browser.json';raw=json.loads(path.read_bytes())
        for changed in ({'sse_sha256':'a'*64},{'submission_count':2},{'completed':False},{'page_errors':['error']}):
            self.write(path,{**raw,**changed})
            with self.assertRaises(ValueError):self.package(directory)

    def test_corpus_drift_and_unconfirmed_shutdown_cannot_qualify(self):
        directory=self.fixture();path=directory/'runtime.json';raw=json.loads(path.read_bytes())
        self.write(path,{**raw,'corpus_after':{'generation':'redis:2'}})
        with self.assertRaises(ValueError):self.package(directory)
        self.write(path,raw);path=directory/'process.json';raw=json.loads(path.read_bytes())
        self.write(path,{**raw,'remote_cleanup_confirmed':False})
        with self.assertRaises(ValueError):self.package(directory)

    def test_aggregate_budget_exhaustion_does_not_start_a_new_attempt(self):
        directory=self.fixture();self.grade(directory)
        self.manifest['max_model_calls']=1
        # Re-freeze the synthetic admission and bind the existing run to it.
        path=directory/'process.json';raw=json.loads(path.read_bytes())
        digest=sha256(json.dumps(self.manifest,sort_keys=True,allow_nan=False).encode())
        raw['identity']['manifest_sha256']=digest
        raw['ready']['manifest_sha256']=raw['closed']['manifest_sha256']=digest
        self.write(path,raw);self.grade(directory)
        with patch('scripts.live_query_admission.prepare_manifest',return_value=self.manifest):
            with self.assertRaisesRegex(ValueError,'budget'):self.admit(1)
        self.assertFalse((self.output/'case-01').exists())

    def test_control_ack_mismatch_is_not_hidden_by_confirmed_flag(self):
        directory=self.fixture();path=directory/'process.json';original=json.loads(path.read_bytes())
        for key,value in (('nonce','c'*32),('reason','control_eof'),('event','failed')):
            raw=copy.deepcopy(original);raw['closed'][key]=value;self.write(path,raw)
            with self.assertRaises(ValueError):self.package(directory)

    def test_runtime_cannot_replace_actual_delivered_final(self):
        directory=self.fixture()
        with patch('scripts.live_query_delivery.DeliveredEvents.conserved_final',return_value={'different':True}):
            with self.assertRaisesRegex(ValueError,'actual engine'):self.package(directory)

    def test_boolean_counts_and_non_image_screenshot_receipts_reject(self):
        directory=self.fixture();path=directory/'browser.json';original=json.loads(path.read_bytes())
        self.write(path,{**original,'submission_count':True})
        with self.assertRaises(ValueError):self.package(directory)
        self.write(path,{**original,'screenshots_sha256':{'runtime.json':sha256((directory/'runtime.json').read_bytes())}})
        with self.assertRaises(ValueError):self.package(directory)

    def test_request_replacement_during_manifest_validation_rejects(self):
        def replace(**kwargs):
            changed=copy.deepcopy(self.requests);changed[0]['question']='Unreviewed replacement'
            self.write(self.inputs/'requests.json',{'version':1,'cases':changed})
            return self.manifest
        with patch('scripts.live_query_admission.prepare_manifest',side_effect=replace):
            with self.assertRaisesRegex(ValueError,'Request bytes changed'):self.admit(0)

    def test_actual_engine_question_mode_and_identity_are_bound(self):
        directory=self.fixture();path=directory/'runtime.json';original=json.loads(path.read_bytes())
        for key,value in (('question','Different question'),('mode','quick')):
            raw=copy.deepcopy(original);raw['engine_final'][key]=value;self.write(path,raw)
            with self.assertRaisesRegex(ValueError,'Actual engine request'):self.package(directory)
        for field in ('history','model'):
            raw=copy.deepcopy(original);request=copy.deepcopy(self.requests[0])
            request[field]=[{'role':'user','content':'Different'}] if field=='history' else 'other'
            digest=expected_request_identity(self.manifest,request)
            raw['engine_final']['query_plan']['request_identity_digest']=digest
            raw['engine_final']['finalization']['request_identity_digest']=digest
            self.write(path,raw)
            with self.assertRaisesRegex(ValueError,'Actual engine request'):self.package(directory)

    def test_unknown_kind_cannot_hide_truncation(self):
        directory=self.fixture();runtime=json.loads((directory/'runtime.json').read_bytes())
        for side in ('input','output'):
            path=directory/f'model-000-{side}.json';raw=json.loads(path.read_bytes())
            raw['kind']='query:unrecognized'
            if side=='output':raw['response']['choices'][0]['finish_reason']='length'
            self.write(path,raw);runtime['model_sha256'][path.name]=sha256(path.read_bytes())
        self.write(directory/'runtime.json',runtime)
        with self.assertRaisesRegex(ValueError,'Unknown native capture kind'):self.package(directory)
