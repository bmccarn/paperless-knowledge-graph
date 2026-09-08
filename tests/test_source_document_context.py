"""Continuation chunks inherit only independently checked original context."""
import copy
import hashlib
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.source_text import bind_document_context
from tests.test_source_dates import ExactAuditor, pack


class SourceDocumentContextTests(unittest.IsolatedAsyncioTestCase):
    def test_full_document_structure_is_parsed_once_for_multiple_chunks(self):
        from app import answer_finalization as module
        fields = [f'**RECORDED DOSE** - - - **{i} mg**' for i in range(1, 21)]
        source = '\n\n'.join(fields)
        items = []
        for index, field in enumerate(fields):
            item = pack(field)['items'][0]
            item.update(id=f'chunk-{index}', chunk_index=index)
            bind_document_context(item, source)
            items.append(item)
        with patch.object(module._MARKDOWN, 'parse', wraps=module._MARKDOWN.parse) as parse:
            self.assertEqual(len(evidence_spans({'items':items})), 20)
        self.assertEqual(sum(call.args[0] == source for call in parse.call_args_list), 1)

    async def verify(self, source, chunk, claim, *, mutate=None):
        evidence = pack(chunk)
        item = evidence['items'][0]
        item['chunk_index'] = 1
        bind_document_context(item, source)
        if mutate:
            mutate(item)
        return await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?', claim, evidence)

    async def test_later_fields_and_list_markers_keep_original_reference_coordinates(self):
        for chunk, claim in [('**ANNUAL CHARGE** - - - **$500**', 'The annual charge is $500.'),
                             ('**RECORDED DOSE** - - - **5** mg', 'The recorded dose is 5 mg.'),
                             ('- 5 mg', 'The recorded dose is 5 mg.')]:
            source = 'Earlier original page.\n\n' + chunk + '\n\nEnd of original page.'
            result = await self.verify(source, chunk, claim)
            self.assertTrue(result['finalization']['answer_verified'], chunk)
            ref = result['claim_ledger']['claims'][0]['references'][0]
            self.assertEqual(ref['quote'], chunk)
            self.assertEqual((ref['start'], ref['end']), (0, len(chunk)))
            self.assertEqual(ref['content_digest'], hashlib.sha256(chunk.encode()).hexdigest())
            self.assertEqual(ref['source_context']['start'], source.index(chunk))
            self.assertEqual(ref['source_context']['digest'], hashlib.sha256(source.encode()).hexdigest())

    async def test_enclosing_literal_and_adjacent_tokens_cannot_grant_authority(self):
        field = '**ANNUAL CHARGE** - - - **$500**'
        for source, chunk, claim in [
            ('Earlier\n\n```\n'+field+'\n```', field, 'The annual charge is $500.'),
            ('Earlier\n\n<pre>\n'+field+'\n</pre>', field, 'The annual charge is $500.'),
            ('Earlier\n\n`'+field+'`', field, 'The annual charge is $500.'),
            ('Recorded dose: -5 mg', '5 mg', 'The recorded dose is 5 mg.'),
            ('Recorded dose: 1500 mg', '500 mg', 'The recorded dose is 500 mg.'),
            ('Charge: $500.25', '$500', 'The charge is $500.'),
        ]:
            result = await self.verify(source, chunk, claim)
            self.assertFalse(result['finalization']['answer_verified'], source)

    async def test_ambiguous_stale_and_forged_bindings_never_fall_back_to_chunk_zero(self):
        chunk = '**ANNUAL CHARGE** - - - **$500**'
        for source, mutation in [
            (chunk+'\n\n'+chunk, None),
            ('Different source', None),
            (chunk, lambda i: i['source_context'].update(digest='0'*64)),
            (chunk, lambda i: i['source_context'].update(start=True)),
            (chunk, lambda i: i['source_context'].update(document_id=999)),
            (chunk, lambda i: i.pop('_source_document_content')),
        ]:
            def mutate(item):
                item['chunk_index'] = 0
                if mutation:
                    mutation(item)
            result = await self.verify(source, chunk, 'The annual charge is $500.', mutate=mutate)
            self.assertFalse(result['finalization']['answer_verified'], (source, mutation))

    async def test_query_packing_binds_indexed_chunks_without_exposing_full_source(self):
        from app.query import QueryEngine
        from app.embeddings import chunk_text
        for field, claim in [('**ANNUAL CHARGE** - - - **$500**', 'The annual charge is $500.'),
                             ('**RECORDED DOSE** - - - **5 mg**', 'The recorded dose is 5 mg.')]:
            source = ('Unrelated original background text.\n\n'*110) + field + '\n\nEnd.'
            chunks = chunk_text(source)
            index = next(i for i,c in enumerate(chunks) if field in c)
            self.assertGreater(index, 0)
            records = [{'document_id':101, 'chunk_index':index, 'source_kind':'ocr', 'content':chunks[index],
                        'title':'Recorded values', 'doc_type':'invoice', 'similarity':1.0}]
            engine = QueryEngine()
            with patch('app.query.paperless_client.get_document', AsyncMock(return_value={'content':source,'title':'Recorded values'})), \
                    patch('app.query.embeddings_store.get_chunks_for_documents', AsyncMock(return_value=records)), \
                    patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
                evidence = await engine._build_evidence_pack('What values are recorded?', {'vector_results':records},
                    [{'document_id':101}], {}, 'strict')
            item = next(i for i in evidence['items'] if i['chunk_index']==index)
            self.assertIsNotNone(item['source_context']['start'])
            class FieldAuditor:
                async def audit_answer_units(self, question, units, spans, plan):
                    span = next(s for s in spans if field in s['content'])
                    return {'assessments': [{'unit_id':u['id'], 'status':'supported', 'temporal_scope':'historical',
                        'references':[{**{k:span[k] for k in ('span_id','evidence_id','document_id')},'quote':field}]} for u in units]}
            result = await AnswerFinalizer(FieldAuditor()).finalize('What is recorded?', claim, {'items':[item]})
            self.assertTrue(result['finalization']['answer_verified'])
            public = engine._public_evidence_pack(evidence)
            self.assertNotIn('_source_document_content', json.dumps(public))
            self.assertNotIn(source, json.dumps(evidence_spans(evidence)))
            self.assertEqual(next(i for i in public['items'] if i['id']==item['id'])['source_context'], item['source_context'])
            # Same evidence identity cannot silently select a different context.
            conflicting = copy.deepcopy(item)
            conflicting['source_context']['digest']='0'*64
            with self.assertRaises(ValueError):
                evidence_spans({'items':[item,conflicting]})
