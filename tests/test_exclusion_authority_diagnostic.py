"""The classification probe is frozen, bounded and separate from semantic grading."""
import asyncio
from contextlib import contextmanager
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()

from scripts import eval_exclusion_authority as probe


def source_payload():
    return {
        'original_question': 'What balance was recorded?', 'evaluated_at': '2026-09-09',
        'source_documents': [{'document_id': 1, 'title': 'Synthetic statement',
                              'spans': [{'span_id': 's1', 'text': 'Balance: 40 USD.'}]}],
        'observations': [{'id': identity, 'text': 'The recorded balance is 40 USD.',
                          'references': [{'span_id': 's1'}]} for identity in ('a', 'b')],
        'delivered_ids': ['a'], 'omitted_id': 'b',
    }


def inputs():
    return {'version': 2, 'partition': 'diagnostic-development',
            'experiment': 'exclusion-authority', 'calls': [
                {'id': f'case-{i}', 'payload': source_payload(),
                 'gold': {'decision': 'covered_by', 'target_ids': ['a']},
                 'origin': {'kind': 'synthetic', 'private_label': 'never enter model input'}}
                for i in range(9)]}


def decision(kind='covered_by', target='a', identity='b'):
    return {'decisions': [{'observation_id': identity, 'decision': kind, 'target_id': target}]}


class ProtocolTests(unittest.TestCase):
    def test_three_decisions_use_production_parser_and_direct_target(self):
        from app.answer_fact_selection import parse_exclusion
        for kind, target in [('outside_request', None), ('reject', None), ('covered_by', 'a')]:
            response = decision(kind, target)
            text = json.dumps(response)
            with patch('app.answer_fact_selection.parse_exclusion', wraps=parse_exclusion) as parser:
                self.assertEqual(probe.parse_classification(text, source_payload()), response)
            parser.assert_called_once_with(text, 'b', ['a'])

    def test_falsey_extra_duplicate_or_foreign_decisions_never_succeed(self):
        valid = json.dumps(decision())
        invalid = [None, '', ' ', 'null', 'false', '[]', '{}', '{"decisions":[]}',
                   'prefix ' + valid, valid + ' suffix', '```json\n' + valid + '\n```',
                   '{"decisions":[],"decisions":[]}',
                   json.dumps({'decisions': decision()['decisions'] * 2}),
                   json.dumps(decision(identity='a')), json.dumps(decision(identity=True)),
                   json.dumps(decision(target='b')), json.dumps(decision(target='foreign')),
                   json.dumps(decision(target=None)), json.dumps(decision(target=['a'])),
                   json.dumps(decision('outside_request', 'a')), json.dumps(decision('reject', 'a')),
                   json.dumps(decision('accept', None)),
                   json.dumps({'decisions': [decision()['decisions'][0] | {'reason': 'gold'}]})]
        for text in invalid:
            with self.subTest(text=text), self.assertRaises((ValueError, TypeError)):
                probe.parse_classification(text, source_payload())


class AdmissionTests(unittest.TestCase):
    def test_exact_nine_and_full_nested_schema_exclude_old_proposal_authority(self):
        self.assertEqual(len(probe.calls_for(json.dumps(inputs()))), 9)
        mutations = [
            lambda d: d.update(version=True),
            lambda d: d['calls'].pop(),
            lambda d: d['calls'].append(deepcopy(d['calls'][0])),
            lambda d: d['calls'][1].update(id=d['calls'][0]['id']),
            lambda d: d['calls'][0]['payload'].update(proposal={'status': 'outside_request'}),
            lambda d: d['calls'][0]['payload'].update(gold='covered_by'),
            lambda d: d['calls'][0]['payload']['source_documents'][0].update(gold='outside_request'),
            lambda d: d['calls'][0]['payload']['source_documents'][0]['spans'][0].update(gold='reject'),
            lambda d: d['calls'][0]['payload']['observations'][0].update(gold='covered_by'),
            lambda d: d['calls'][0]['payload']['observations'][0]['references'][0].update(gold=True),
        ]
        for mutate in mutations:
            data = inputs(); mutate(data)
            with self.subTest(data=data), self.assertRaises(ValueError):
                probe.calls_for(json.dumps(data))

    def test_source_and_identity_boundaries(self):
        for key, value in [('original_question', {}), ('evaluated_at', '20260909'),
                           ('evaluated_at', '2026-02-30'), ('evaluated_at', {}),
                           ('source_documents', []), ('observations', []),
                           ('delivered_ids', []), ('delivered_ids', ['a', 'a']),
                           ('delivered_ids', ['foreign']), ('delivered_ids', ['b']),
                           ('omitted_id', True), ('omitted_id', 'foreign'),
                           ('conversation_context', {}), ('conversation_context', 'x' * 12001)]:
            data = inputs(); data['calls'][0]['payload'][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                probe.calls_for(json.dumps(data))
        for identity in [0, -1, True, '1', None]:
            data = inputs(); data['calls'][0]['payload']['source_documents'][0]['document_id'] = identity
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                probe.calls_for(json.dumps(data))
        for mutate in [
            lambda p: p['observations'][1].update(id='a'),
            lambda p: p['observations'][0]['references'][0].update(span_id='foreign'),
            lambda p: p['observations'][0]['references'].append({'span_id': 's1'}),
            lambda p: p['source_documents'].append(deepcopy(p['source_documents'][0])),
            lambda p: p['source_documents'][0]['spans'].append({'span_id': 's1', 'text': 'Conflicting'}),
        ]:
            data = inputs(); mutate(data['calls'][0]['payload'])
            with self.assertRaises(ValueError): probe.calls_for(json.dumps(data))
        data = inputs(); p = data['calls'][0]['payload']
        p['source_documents'].append({'document_id': 2, 'title': 'Other',
                                      'spans': [{'span_id': 's2', 'text': 'Other document.'}]})
        p['observations'][0]['references'].append({'span_id': 's2'})
        with self.assertRaises(ValueError): probe.calls_for(json.dumps(data))

    def test_independent_gold_allows_equivalent_single_targets_not_target_sets(self):
        data = inputs(); p = data['calls'][0]['payload']
        p['observations'].append({**deepcopy(p['observations'][0]), 'id': 'c'})
        p['delivered_ids'].append('c'); data['calls'][0]['gold']['target_ids'].append('c')
        self.assertEqual(len(probe.calls_for(json.dumps(data))), 9)
        for gold in [{'decision': 'covered_by', 'target_ids': []},
                     {'decision': 'covered_by', 'target_ids': ['b']},
                     {'decision': 'covered_by', 'target_ids': ['a', 'a']},
                     {'decision': 'outside_request', 'target_ids': ['a']},
                     {'decision': 'reject', 'target_ids': ['a']},
                     {'decision': 'accept', 'target_ids': []}]:
            changed = inputs(); changed['calls'][0]['gold'] = gold
            with self.subTest(gold=gold), self.assertRaises(ValueError):
                probe.calls_for(json.dumps(changed))

    def test_manifest_binds_reviews_prompt_runtime_code_and_fixed_budgets(self):
        from app.answer_fact_selection import EXCLUSION_PROMPT
        from scripts.eval_source_audit import write_private
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); payload = json.dumps(inputs()).encode()
            (root / 'inputs.json').write_bytes(payload)
            for axis in ('spec', 'standards'):
                write_private(root / f'inputs-{axis}-review.json', {
                    'verdict': 'pass', 'input_sha256': probe.digest(payload), 'reviewer': axis})
            runtime = {'enabled': True, 'model': 'synthetic', 'packages': {'strands-agents': '1.55.0'}}
            with patch('scripts.eval_source_audit.runtime_snapshot', return_value=runtime):
                manifest = probe.manifest_for(root)
                self.assertEqual((manifest['max_calls'], manifest['active_seconds']), (9, 900))
                self.assertEqual(manifest['prompt_sha256'], probe.digest(EXCLUSION_PROMPT.encode()))
                self.assertIn('app/answer_fact_selection.py', manifest['code_sha256'])
                self.assertEqual(len(manifest['reviews']), 2)
                for value in [None, '', ' ', True, 'spec']:
                    (root / 'inputs-standards-review.json').write_text(json.dumps({
                        'verdict': 'pass', 'input_sha256': probe.digest(payload), 'reviewer': value}))
                    with self.subTest(value=value), self.assertRaises(ValueError): probe.manifest_for(root)


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.payload = json.dumps(inputs()).encode()
        (self.root / 'inputs.json').write_bytes(self.payload)
        self.orchestrator = AsyncMock()

    @contextmanager
    def stage_capture(self, orchestrator, capture, output):
        self.capture = capture
        yield {'attempts': [], 'hashes': {}}

    async def test_same_admitted_bytes_gold_isolation_and_no_retry_after_failure(self):
        from app.answer_fact_selection import EXCLUSION_PROMPT
        admitted = []
        def admit(directory, *, payload):
            admitted.append(payload)
            (directory / 'inputs.json').write_text('{"changed":true}')
            return {'frozen': True}
        self.orchestrator._text_agent.side_effect = [None] + [json.dumps(decision())] * 8
        with patch.object(probe, 'manifest_for', side_effect=admit), \
             patch.object(probe, 'validate_native_call'), \
             patch('scripts.live_query_stages.capture_stages', self.stage_capture), \
             patch('app.strands_orchestrator.StrandsQueryOrchestrator', return_value=self.orchestrator):
            await probe.execute(self.root, {'frozen': True})
        self.assertEqual(admitted, [self.payload])
        self.assertEqual(self.orchestrator._text_agent.await_count, 9)
        for call, original in zip(self.orchestrator._text_agent.await_args_list, inputs()['calls']):
            self.assertEqual(call.args, ('fact_exclusion', EXCLUSION_PROMPT,
                                        json.dumps(original['payload'], ensure_ascii=False)))
            self.assertNotIn('private_label', call.args[2])
        result = json.loads((self.root / 'results/result.json').read_bytes())
        self.assertFalse(result['complete_execution'])
        self.assertEqual(result['calls'][0]['status'], 'failed')
        self.assertTrue(all(r['status'] == 'valid_ungraded' for r in result['calls'][1:]))
        self.assertEqual(result['input_sha256'], probe.digest(self.payload))
        self.orchestrator.close.assert_awaited_once()
        with patch.object(probe, 'manifest_for', return_value={'frozen': True}), \
             patch.object(probe, 'calls_for', return_value=inputs()['calls']), \
             self.assertRaises(FileExistsError):
            await probe.execute(self.root, {'frozen': True})

    async def test_changed_identity_prevents_construction_or_calls(self):
        with patch.object(probe, 'manifest_for', return_value={'changed': True}), \
             patch('app.strands_orchestrator.StrandsQueryOrchestrator') as construct, \
             self.assertRaises(ValueError):
            await probe.execute(self.root, {'frozen': True})
        construct.assert_not_called(); self.assertFalse((self.root / 'results').exists())

    async def test_cancellation_retains_active_call_and_closes_before_propagating(self):
        self.orchestrator._text_agent.side_effect = asyncio.CancelledError
        with patch.object(probe, 'manifest_for', return_value={}), \
             patch('scripts.live_query_stages.capture_stages', self.stage_capture), \
             patch('app.strands_orchestrator.StrandsQueryOrchestrator', return_value=self.orchestrator), \
             self.assertRaises(asyncio.CancelledError):
            await probe.execute(self.root, {})
        result = json.loads((self.root / 'results/result.json').read_bytes())
        self.assertEqual(result['interrupted'], 'CancelledError')
        self.assertFalse(result['complete_execution']); self.assertEqual(len(result['calls']), 1)
        self.assertEqual(result['calls'][0]['exception_type'], 'CancelledError')
        self.orchestrator.close.assert_awaited_once()

    async def test_whole_budget_stops_remaining_calls_and_records_timeout(self):
        async def expire(*args):
            self.capture.deadline = asyncio.get_running_loop().time() - 1
            raise TimeoutError()
        self.orchestrator._text_agent.side_effect = expire
        with patch.object(probe, 'manifest_for', return_value={}), \
             patch('scripts.live_query_stages.capture_stages', self.stage_capture), \
             patch('app.strands_orchestrator.StrandsQueryOrchestrator', return_value=self.orchestrator):
            await probe.execute(self.root, {})
        result = json.loads((self.root / 'results/result.json').read_bytes())
        self.assertEqual(len(result['calls']), 1); self.assertEqual(result['not_started_calls'], 8)
        self.assertFalse(result['complete_execution']); self.orchestrator.close.assert_awaited_once()

    async def test_native_failure_cannot_turn_valid_classification_into_success(self):
        self.orchestrator._text_agent.return_value = json.dumps(decision())
        with patch.object(probe, 'manifest_for', return_value={}), \
             patch.object(probe, 'validate_native_call', side_effect=[ValueError('terminal')] + [None] * 8), \
             patch('scripts.live_query_stages.capture_stages', self.stage_capture), \
             patch('app.strands_orchestrator.StrandsQueryOrchestrator', return_value=self.orchestrator):
            await probe.execute(self.root, {})
        result = json.loads((self.root / 'results/result.json').read_bytes())
        self.assertFalse(result['complete_execution']); self.assertEqual(result['calls'][0]['status'], 'failed')
        self.assertEqual(self.orchestrator._text_agent.await_count, 9)

    async def test_actual_sdk_all_nine_captures_and_truncated_eof_preserve_failure(self):
        import httpx
        from strands import Agent
        from strands.models.openai import OpenAIModel
        from app import strands_orchestrator as native_module

        for truncated in (False, True):
            with self.subTest(truncated=truncated), tempfile.TemporaryDirectory() as temp:
                root = Path(temp); (root / 'inputs.json').write_bytes(self.payload)
                requests, clients = [], []
                def respond(request):
                    requests.append(json.loads(request.content))
                    chunks = [{
                        'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
                        'choices': [{'index': 0, 'delta': {'role': 'assistant',
                            'content': json.dumps(decision())}, 'finish_reason': None}],
                    }]
                    if not (truncated and len(requests) == 1):
                        chunks.append({
                            'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
                            'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}],
                            'usage': {'prompt_tokens': 2, 'completion_tokens': 1, 'total_tokens': 3},
                        })
                    return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                        content=''.join('data: ' + json.dumps(c) + '\n\n' for c in chunks)
                        + 'data: [DONE]\n\n')
                def model(instance, **kwargs):
                    client = httpx.AsyncClient(transport=httpx.MockTransport(respond)); clients.append(client)
                    return OpenAIModel(client_args={'api_key': 'synthetic-secret', 'max_retries': 0,
                        'base_url': 'http://synthetic.invalid/v1', 'http_client': client}, model_id='test')
                with patch.object(probe, 'manifest_for', return_value={'frozen': True}), \
                     patch.object(native_module, 'Agent', Agent), \
                     patch.object(native_module.StrandsQueryOrchestrator, '_model', model):
                    await probe.execute(root, {'frozen': True})
                result = json.loads((root / 'results/result.json').read_bytes())
                self.assertEqual(result['complete_execution'], not truncated)
                self.assertEqual(result['native_call_count'], 9)
                self.assertEqual(len(requests), 9); self.assertTrue(all(c.is_closed for c in clients))
                self.assertEqual(result['not_started_calls'], 0)
                self.assertEqual(len(result['capture_sha256']), 18)
                self.assertEqual(len(result['stage_sha256']), 18)
                self.assertEqual(len(result['call_sha256']), 9)
                for name, digest in {**result['capture_sha256'], **result['stage_sha256'],
                                     **result['call_sha256']}.items():
                    self.assertEqual(probe.digest((root / 'results' / name).read_bytes()), digest)
                for request in requests:
                    self.assertNotIn('target_ids', json.dumps(request))
                    self.assertNotIn('private_label', json.dumps(request))
                    self.assertEqual(request['cache'], {'no-cache': True, 'no-store': True})
                artifacts = ''.join(p.read_text() for p in (root / 'results').iterdir())
                self.assertNotIn('synthetic-secret', artifacts)
                if truncated:
                    self.assertEqual(result['calls'][0]['status'], 'failed')
                    self.assertTrue(all(r['status'] == 'valid_ungraded' for r in result['calls'][1:]))
                    raw = json.loads((root / 'results/model-000-output.json').read_bytes())
                    self.assertEqual(raw['exception_type'], 'MissingProviderTermination')
