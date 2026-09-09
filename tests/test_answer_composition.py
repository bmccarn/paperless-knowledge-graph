"""Composition proposes observations; originals still decide delivery."""
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_composition import AnswerComposition
from app.answer_finalization import AnswerFinalizer
from app.question_evidence import QuestionEvidence, QuestionEvidenceError
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests import test_source_reading as reading_fixtures
from tests.test_question_evidence import REQUIREMENTS
from tests.source_audit_fixtures import decision


class AnswerCompositionTests(unittest.IsolatedAsyncioTestCase):
    setUp = reading_fixtures.SourceReadingTests.setUp

    async def prepared(self):
        class Reader:
            async def read_question_sources(inner, payload): return self.reading
        return await QuestionEvidence.prepare(Reader(), 'What do the records establish?',
            REQUIREMENTS, self.pack, evaluated_at='2026-09-09')

    def proposal(self):
        return {'observations': ['Vendor completed a refund of $125.'],
            'requirement_mapping': [{'requirement_id': 'r1', 'observation_ids': ['u1'], 'status': 'proposed'}],
            'source_references': [{'observation_id': 'u1', 'span_ids': [self.spans[0]['span_id']]}]}

    async def test_composer_receives_originals_and_notes_without_old_answer(self):
        evidence = await self.prepared()
        auditor = StrandsQueryOrchestrator(); auditor.enabled = True
        calls = []
        async def model(**kwargs):
            calls.append(kwargs)
            return json.dumps(self.proposal())
        with patch.object(auditor, '_text_agent', side_effect=model):
            composed = await auditor.compose_question_answer(evidence)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['name'], 'answer_composer')
        payload = json.loads(calls[0]['prompt'])
        self.assertEqual(payload, evidence.composition_input)
        self.assertNotIn('draft_answer', payload)
        self.assertEqual(composed.snapshot_digest, evidence.digest)
        self.assertEqual(composed.candidate.units()[0]['id'], 'u1')

    async def test_foreign_missing_duplicate_and_unmapped_ids_reject_whole_composition(self):
        evidence = await self.prepared()
        changes = [lambda x: x['requirement_mapping'].clear(),
            lambda x: x['requirement_mapping'].append(copy.deepcopy(x['requirement_mapping'][0])),
            lambda x: x['requirement_mapping'][0].update(requirement_id='r999'),
            lambda x: x['requirement_mapping'][0].update(observation_ids=['u999']),
            lambda x: x['requirement_mapping'][0].update(observation_ids=['u1', 'u1']),
            lambda x: x['requirement_mapping'][0].update(status='unresolved'),
            lambda x: x['source_references'][0].update(span_ids=['foreign']),
            lambda x: x['source_references'][0].update(observation_id='u999'),
            lambda x: x['source_references'].clear(),
            lambda x: x['observations'].append('Unrequested adjacent fact.'),
            lambda x: x.update(answer='Do not fall back to this draft.')]
        for change in changes:
            raw = self.proposal(); change(raw)
            with self.assertRaises(QuestionEvidenceError):
                AnswerComposition.parse(json.dumps(raw), evidence)
        raw = json.dumps(self.proposal())
        for invalid in ('prefix ' + raw, raw + ' trailing', raw.replace('"observations":', '"observations": [], "observations":')):
            with self.assertRaises(QuestionEvidenceError): AnswerComposition.parse(invalid, evidence)

    async def test_reader_and_composer_false_value_cannot_override_originals(self):
        self.reading['documents'][0]['observations'][0]['text'] = 'MALICIOUS NOTE: the refund was $999.'
        evidence = await self.prepared()
        raw = self.proposal(); raw['observations'] = ['Vendor completed a refund of $999.']
        composed = AnswerComposition.parse(json.dumps(raw), evidence)
        auditor = StrandsQueryOrchestrator(); auditor.enabled = True
        async def model(**kwargs):
            return json.dumps({'assessments': [decision(references=[{'span_id': self.spans[0]['span_id']}])]})
        with patch.object(auditor, '_text_agent', side_effect=model):
            final = await AnswerFinalizer(evidence.auditor(auditor)).finalize(
                'What do the records establish?', composed.candidate, self.pack, evaluated_at='2026-09-09')
        self.assertFalse(final['finalization']['answer_verified'])
        self.assertNotIn('MALICIOUS NOTE', json.dumps(final))
