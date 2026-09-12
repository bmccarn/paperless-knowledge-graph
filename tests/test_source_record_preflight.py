import hashlib
import json
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import patch
from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.source_record_preflight import prepare
from scripts.source_recovery_preflight import mock_sdk
from scripts.run_source_recovery import schedule
from tests.test_source_record_reader import question


class RecordPreflightTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self, root):
        text='Original fact.'; digest=hashlib.sha256(text.encode()).hexdigest()
        req={**question(),'source_documents':[{'document_id':1,'windows':[{'span':{
            'document_id':1,'span_id':'s1','start':0,'end':len(text),'content':text,'content_digest':digest}}]}]}
        for name,data in [('request.json',req),('original.json',{'id':1,'content':text})]:
            (root/name).write_text(json.dumps(data))
        cases={r['case'] for r in schedule()}
        manifest={'pairs':schedule(),'originals':{c:'original.json' for c in cases},
            'requests':{c:'request.json' for c in cases},'sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest()
                for name in ('request.json','original.json')}}
        (root/'manifest.json').write_text(json.dumps(manifest))
        return manifest,req

    async def test_changed_file_after_validation_cannot_change_serialized_request(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); _,req=self.fixture(root)
            @asynccontextmanager
            async def mutation_after_freeze(model,responder,*,raw_bodies):
                (root/'request.json').write_text(json.dumps({**req,'question':'Changed after validation'}))
                async with mock_sdk(model,responder,raw_bodies=raw_bodies) as bodies:
                    yield bodies
            with patch('scripts.source_record_preflight.mock_sdk',mutation_after_freeze):
                report=await prepare(root,root/'output','synthetic-model')
            self.assertEqual(report['requests'],12)
            for file in (root/'output').glob('case-*/wire-*.json'):
                body=json.loads(file.read_bytes());content=body['messages'][-1]['content']
                payload=json.loads(content if isinstance(content,str) else ''.join(p['text'] for p in content))
                self.assertEqual(payload['question'],req['question'])

    async def test_unadmitted_reference_fails_before_output_or_dispatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);manifest,req=self.fixture(root)
            (root/'other.json').write_text(json.dumps(req))
            manifest['requests'][schedule()[0]['case']]='other.json'
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'Unbound preflight source'):
                await prepare(root,root/'output','synthetic-model')
            self.assertFalse((root/'output').exists())
