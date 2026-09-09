"""Reject changed qualification inputs before any live reader can be opened."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.live_query_manifest import parse_requests, private_inputs, prepare_manifest
from scripts.live_query_evaluation import sha256


class LiveManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root / 'originals').mkdir()
        self.requests = {'version': 1, 'cases': [
            {'id': str(i), 'question': f'Question {i}', 'mode': mode, 'model': 'frozen',
             'history': [{'role': 'assistant', 'content': 'Antecedent'}] if i == 4 else []}
            for i, mode in enumerate(('strict', 'quick', 'deep', 'timeline', 'deep', 'strict'))]}
        (self.root / 'requests.json').write_text(json.dumps(self.requests))
        (self.root / 'rubric.md').write_text('Independently reviewed original facts.')
        (self.root / 'inventory.json').write_text('{"documents": [{"id": 42}]}')
        original = b'{"id":42,"content":"Original source."}'
        (self.root / 'originals' / '42.json').write_bytes(original)
        self.originals = {'inventory_sha256': sha256((self.root / 'inventory.json').read_bytes()),
                          'originals_sha256': {'42.json': sha256(original)}}
        (self.root / 'originals-manifest.json').write_text(json.dumps(self.originals))

    def test_exact_private_inputs_are_bound_and_changed_original_or_inventory_rejects(self):
        hashes = private_inputs(self.root)
        self.assertEqual(len(hashes), 5)
        for path in (self.root / 'originals' / '42.json', self.root / 'inventory.json'):
            original = path.read_bytes(); path.write_bytes(original + b' ')
            with self.assertRaises(ValueError): private_inputs(self.root)
            path.write_bytes(original)
        self.assertEqual(private_inputs(self.root), hashes)

    def test_questions_models_history_and_mode_coverage_are_explicit(self):
        parse_requests(json.dumps(self.requests))
        mutations = [lambda d: d['cases'][0].pop('model'),
                     lambda d: d['cases'][0].update(originals=['Rubric injection']),
                     lambda d: d['cases'][0].update(id='1'),
                     lambda d: d['cases'][3].update(mode='strict'),
                     lambda d: d['cases'][4].update(history=[]),
                     lambda d: d['cases'][4]['history'][0].update(role='system')]
        for mutate in mutations:
            data = copy.deepcopy(self.requests); mutate(data)
            with self.assertRaises(ValueError): parse_requests(json.dumps(data))

    def test_failed_all_mode_admission_does_not_read_private_inputs(self):
        with patch('scripts.live_query_manifest.admit_all_modes', side_effect=ValueError('Not qualified')), \
             patch('scripts.live_query_manifest.private_inputs') as read_inputs:
            with self.assertRaisesRegex(ValueError, 'Not qualified'):
                prepare_manifest(dataset='unused', initial_output='unused', all_mode_output='unused',
                                 inputs=self.root, configuration={}, corpus_snapshot={}, evaluated_at='2026-09-09')
            read_inputs.assert_not_called()

    def test_manifest_binds_admission_helpers_and_private_question_bytes(self):
        with patch('scripts.live_query_manifest.admit_all_modes', return_value={'reviewed': 'synthetic'}):
            args = dict(dataset='unused', initial_output='unused', all_mode_output='unused', inputs=self.root,
                        configuration={'model': 'frozen'}, corpus_snapshot={'generation': 'test'}, evaluated_at='2026-09-09')
            first = prepare_manifest(**args)
            self.requests['cases'][0]['question'] += '?'
            (self.root / 'requests.json').write_text(json.dumps(self.requests))
            second = prepare_manifest(**args)
        self.assertNotEqual(first['private_inputs_sha256'], second['private_inputs_sha256'])
        self.assertIn('scripts/live_query_manifest.py', first['live_code_sha256'])
        self.assertEqual(first['all_mode_admission'], {'reviewed': 'synthetic'})

    def test_mutating_nested_runtime_inputs_cannot_rewrite_the_frozen_snapshot(self):
        configuration = {'models': ['frozen'], 'timeouts': {'audit': 120}}
        corpus = {'generation': 'g1', 'documents': [42]}
        with patch('scripts.live_query_manifest.admit_all_modes', return_value={'reviewed': 'synthetic'}):
            args = dict(dataset='unused', initial_output='unused', all_mode_output='unused', inputs=self.root,
                        configuration=configuration, corpus_snapshot=corpus, evaluated_at='2026-09-09')
            first = prepare_manifest(**args)
            configuration['models'].append('changed'); configuration['timeouts']['audit'] = 999
            corpus['generation'] = 'g2'; corpus['documents'].append(43)
            second = prepare_manifest(**args)
            self.assertEqual(first['configuration'], {'models': ['frozen'], 'timeouts': {'audit': 120}})
            self.assertEqual(first['corpus_snapshot'], {'generation': 'g1', 'documents': [42]})
            self.assertNotEqual(first, second)
            configuration['timeouts']['audit'] = float('nan')
            with self.assertRaises(ValueError): prepare_manifest(**args)
