import copy
import json
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.prepare_reader_retention import prepare_case
from app.evidence import build_evidence_pack
from app.source_text import bind_document_context
from app.source_reading import READER_PROMPT, response_format, group_sources
from app.answer_finalization import evidence_spans


class PreparationTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        original = {'id': 1, 'title': 'Station record', 'content': 'Measured pressure was 20 kPa. A future target is 15 kPa.', 'tags': []}
        pack = build_evidence_pack('What was measured?', {}, [{'document_id': 1, 'title': original['title'],
            'content': original['content'][:27], 'source_kind': 'ocr'}], [])
        bind_document_context(pack['items'][0], original['content'])
        documents = group_sources(evidence_spans(pack, citation_safe=True))
        payload = {'question': 'What was measured?', 'resolved_question': 'What was measured?',
            'evaluated_at': '2026-09-10', 'source_date_order': 'mdy', 'source_documents': documents,
            'requirements': [{'id': 'r1', 'aspect': 'Recorded measurement', 'temporal_scope': 'none', 'comparison_scope': 'none'}]}
        capture = {'name': 'source_reader', 'system_prompt': READER_PROMPT,
                   'prompt': json.dumps(payload), 'response_format': response_format(documents)}
        return original, capture

    async def test_matched_arms_preserve_request_and_restore_late_text(self):
        original, capture = self.fixture()
        case = await prepare_case('station', original, capture)
        r, f = case['arms']['R'], case['arms']['F']
        self.assertEqual(r['prompt'], capture['prompt'])
        self.assertEqual({k:v for k,v in r['payload'].items() if k != 'source_documents'},
                         {k:v for k,v in f['payload'].items() if k != 'source_documents'})
        self.assertTrue(r['coverage']['missing_intervals'])
        self.assertEqual(f['coverage']['missing_intervals'], [])
        self.assertFalse(case['same_semantic_extent'])

    async def test_changed_original_or_reader_prompt_cannot_enter_comparison(self):
        for change in ('original', 'prompt'):
            original, capture = self.fixture()
            if change == 'original': original['content'] = 'Different reading.'
            else: capture['system_prompt'] += ' Remember the target.'
            with self.assertRaises(ValueError): await prepare_case('station', original, capture)

    async def test_noncontiguous_retained_window_requires_exact_explicit_mapping(self):
        from scripts.prepare_reader_retention import retained_intervals
        original = {'id': 2, 'content': 'HEADER. Omitted middle. VALUE.'}
        payload = {'source_documents': [{'document_id': 2, 'windows': [{'ordinal': 0, 'span': {
            'span_id': 'old-table', 'document_id': 2, 'start': 0, 'end': 13, 'content': 'HEADER.VALUE.'}}]}]}
        with self.assertRaises(ValueError): retained_intervals(payload, original)
        mappings = {'old-table': [{'span_start': 0, 'span_end': 7, 'original_start': 0, 'original_end': 7},
                                  {'span_start': 7, 'span_end': 13, 'original_start': 24, 'original_end': 30}]}
        coverage = retained_intervals(payload, original, mappings)
        self.assertEqual(coverage['missing_intervals'], [[7, 24]])
        mappings['old-table'][1]['original_start'] = 22
        with self.assertRaises(ValueError): retained_intervals(payload, original, mappings)

    async def test_provider_measurement_uses_real_sdk_without_native_calls_or_caps(self):
        from scripts.prepare_reader_retention import provider_request_measurement
        original, capture = self.fixture()
        payload = json.loads(capture['prompt'])
        measured = await provider_request_measurement(payload, model='frozen-synthetic-model')
        self.assertEqual(measured['native_model_calls'], 0)
        self.assertEqual(measured['body']['model'], 'frozen-synthetic-model')
        self.assertTrue(measured['body']['extra_body']['cache']['no-cache'] if 'extra_body' in measured['body']
                        else measured['body']['cache']['no-cache'])
        self.assertFalse({'max_tokens', 'max_completion_tokens', 'max_output_tokens'} & measured['body'].keys())
        self.assertGreater(measured['bytes'], len(capture['prompt']))
