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
    def test_header_companions_follow_their_original_table_for_both_chunk_origins(self):
        from app.embeddings import chunk_text, table_header_chunks
        header = '| Record | Debit | Credit |\n| --- | --- | --- |\n'
        source = 'First ledger\n\n' + header + ''.join(f'| Aster {i} | ${i+10} | $1 |\n' for i in range(350))
        second_start = len(source) + len('\nSecond ledger\n\n')
        source += '\nSecond ledger\n\n' + header + ''.join(f'| Orion {i} | ${i+20} | $2 |\n' for i in range(350))
        for params in ({'chunk_size':4000,'overlap':800}, {'chunk_size':3600,'overlap':500}):
            represented = chunk_text(source, **params)
            raw = chunk_text(source, **params, include_table_headers=False)
            self.assertEqual(len(raw), len(represented))
            headers = table_header_chunks(source, raw)
            target = next(i for i,text in enumerate(raw) if '| Orion 250 |' in text)
            self.assertIn(target, headers)
            companion = raw[headers[target]]
            self.assertIn(header, companion)
            start = source.index(companion)
            self.assertTrue(start <= second_start < start+len(companion))
            self.assertIn('Orion', companion)
            self.assertTrue(all(text in source for text in raw))
            # Identical original intervals have no independently unique binding.
            ambiguous = 'Repeated body.\n' * 500
            self.assertEqual(table_header_chunks(ambiguous, ['Repeated body.\n']), {})

    async def test_table_continuations_are_rebound_before_source_certification(self):
        from app.query import QueryEngine
        from app.embeddings import chunk_text
        header = '| Record | Charge |\n| --- | --- |\n'
        source = 'Service invoices\n\n' + header + ''.join(
            f'| Account {i} recorded service | ${i+100} |\n' for i in range(190)) + '\nEnd of table.'
        represented = chunk_text(source)
        index = next(i for i, text in enumerate(represented) if text not in source)
        record = {'document_id':101, 'chunk_index':index, 'source_kind':'ocr', 'content':represented[index],
                  'title':'Service invoices', 'doc_type':'invoice', 'similarity':1.0}
        engine = QueryEngine()
        async def build(records):
            with patch('app.query.paperless_client.get_document', AsyncMock(return_value={'content':source,'title':'Service invoices'})) as fetch, \
                    patch('app.query.embeddings_store.get_chunks_for_documents', AsyncMock(return_value=records)), \
                    patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
                result = await engine._build_evidence_pack('What invoice charges are recorded?', {'vector_results':records},
                    [{'document_id':101}], {}, 'strict')
            self.assertEqual(fetch.await_count, 1)
            return result
        evidence = await build([record])
        item = next(i for i in evidence['items'] if i['chunk_index'] == index)
        self.assertIn(item['content'], source)
        self.assertNotEqual(item['content'], represented[index])
        self.assertTrue(all(s['content'] in source for s in evidence_spans(evidence, citation_safe=True)))
        self.assertTrue(all(i.get('source_context', {}).get('start') is not None for i in evidence['items']))
        self.assertEqual(chunk_text(source), represented)  # Indexed representation remains unchanged.
        altered = {**record, 'content':'Invented prefix. ' + record['content']}
        rejected = await build([altered])
        self.assertFalse(any(i['chunk_index'] == index for i in rejected['items']))
        generated = {**record, 'source_kind':'summary'}
        excluded = await build([generated])
        self.assertFalse(any(i['chunk_index'] == index for i in excluded['items']))

    def test_unbound_full_source_cannot_certify_an_inserted_header(self):
        source = '| Field | Value |\n| --- | --- |\n| Charge | $500 |'
        transformed = '| Field | Value |\n| --- | --- |\n' + 'Value |\n| --- | --- |\n| Charge | $500 |'
        evidence = pack(transformed)
        bind_document_context(evidence['items'][0], source)
        diagnostics = {}
        self.assertEqual(evidence_spans(evidence, citation_safe=True, diagnostics=diagnostics), [])
        self.assertEqual(diagnostics['unbound_source_chunks'], 1)

    async def test_long_table_keeps_separate_original_header_through_audit_selection(self):
        from app.query import QueryEngine
        from app.embeddings import chunk_text
        header = '| Record | Invoiced | Refunded |\n| --- | --- | --- |\n'
        source = header + ''.join(f'| {"Aster" if i<500 else "Orion"} record {i} | ${i+100} | $25 |\n'
                                  for i in range(1600))
        row = '| Orion record 1000 | $1100 | $25 |'
        chunks = chunk_text(source)
        index = next(i for i,c in enumerate(chunks) if row in c)
        records = [{'document_id':101,'chunk_index':index,'source_kind':'ocr','content':chunks[index],
                    'title':'Service ledger','doc_type':'invoice','similarity':1.0}]
        engine = QueryEngine()
        with patch('app.query.paperless_client.get_document', AsyncMock(return_value={'content':source,'title':'Service ledger'})), \
                patch('app.query.embeddings_store.get_chunks_for_documents', AsyncMock(return_value=records)), \
                patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
            evidence = await engine._build_evidence_pack('What was invoiced for Orion record 1000?',
                {'vector_results':records}, [{'document_id':101}], {}, 'strict')
        self.assertTrue(any(header in i['content'] for i in evidence['items']))
        self.assertTrue(all(i['content'] in source for i in evidence['items']))
        seen = []
        class TableAuditor:
            async def audit_answer_units(self, question, units, spans, plan):
                chosen = [next((s for s in spans if text in s['content']), None) for text in (header,row)]
                seen.append(chosen)
                return {'assessments':[{'unit_id':u['id'],'status':'supported' if all(chosen) else 'missing',
                    'temporal_scope':'historical','references':[{'span_id':s['span_id']} for s in chosen if s]} for u in units]}
        result = await AnswerFinalizer(TableAuditor()).finalize('What was invoiced for Orion record 1000?',
            'Orion record 1000 was invoiced $1100 and refunded $25.', evidence)
        self.assertTrue(result['finalization']['answer_verified'])
        refs = result['claim_ledger']['claims'][0]['references']
        self.assertEqual(len({r['span_id'] for r in refs}), 2)
        self.assertTrue(all(r['quote'] in source for r in refs))

    async def test_adjacent_tables_keep_the_correct_column_order_in_the_public_pack(self):
        from app.query import QueryEngine
        from app.embeddings import chunk_text, table_header_chunks
        first = '| Record | Debit | Credit |\n| --- | --- | --- |\n'
        second = '| Record | Credit | Debit |\n| --- | --- | --- |\n'
        source = first + ''.join(f'| Aster {i} | ${i+10} | $2 |\n' for i in range(350)) + '\n'
        second_start = len(source)
        source += second + ''.join(f'| Orion revised balance {i} | ${i+20} | $3 |\n' for i in range(1600))
        row = '| Orion revised balance 1250 | $1270 | $3 |'
        for params in ({'chunk_size':4000,'overlap':800}, {'chunk_size':3600,'overlap':500}):
            raw = chunk_text(source, **params, include_table_headers=False)
            target = next(i for i,text in enumerate(raw) if row in text)
            companion = raw[table_header_chunks(source, raw)[target]]
            self.assertIn(second, companion)
            self.assertTrue(source.index(companion) <= second_start < source.index(companion)+len(companion))
        chunks = chunk_text(source)
        index = next(i for i,c in enumerate(chunks) if row in c)
        records = [{'document_id':101,'chunk_index':index,'source_kind':'ocr','content':chunks[index],
                    'title':'Ledger','doc_type':'invoice','similarity':1.0}]
        with patch('app.query.paperless_client.get_document', AsyncMock(return_value={'content':source,'title':'Ledger'})), \
                patch('app.query.embeddings_store.get_chunks_for_documents', AsyncMock(return_value=records)), \
                patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
            evidence = await QueryEngine()._build_evidence_pack('What is the Orion revised balance 1250?',
                {'vector_results':records}, [{'document_id':101}], {}, 'strict')
        self.assertTrue(any(second in i['content'] for i in evidence['items']))
        self.assertTrue(any(row in i['content'] for i in evidence['items']))
        self.assertTrue(all(i['content'] in source for i in evidence['items']))

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
