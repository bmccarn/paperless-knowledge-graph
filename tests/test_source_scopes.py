"""Original supply and selected reference scope remain distinct from fact truth."""
import copy
import hashlib
import unittest

from app.source_scopes import SourceScope, SourceScopeError


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def original(doc_id, text):
    return {'document_id': doc_id, 'content': text, 'content_digest': digest(text), 'extent': len(text)}


def span(record, start, end, handle):
    return {'span_id': handle, 'document_id': record['document_id'],
            'start': start, 'end': end, 'content': record['content'][start:end],
            'content_digest': record['content_digest'], 'feedback_open': False}


class SourceScopeTests(unittest.TestCase):
    def setUp(self):
        self.a = original(42, 'Service record\nNo attendance recorded.\n  ')
        self.b = original(43, 'Separate record\nAttendance recorded.\n')
        self.spans = [span(self.a, 0, 23, 'a-first'), span(self.a, 20, self.a['extent'], 'a-tail'),
                      span(self.b, 0, self.b['extent'], 'b-full')]
        self.scope = SourceScope.bind([self.a, self.b], self.spans)

    def test_actual_view_and_selected_scope_are_independent(self):
        view = self.scope.view(self.spans[:2])
        doc = view.documents[0]
        self.assertEqual(doc['source_scope']['coverage'], 'complete_original')
        whole = doc['source_scope']['complete_original_reference']
        selected = view.resolve([{'kind': 'passage', 'handle': 'a-first'}])
        self.assertEqual(selected.references, [{'span_id': 'a-first'}])
        resolved = view.resolve([whole, {'kind': 'passage', 'handle': 'a-first'}, whole])
        self.assertEqual(resolved.references, [{'span_id': 'a-first'}, {'span_id': 'a-tail'}])
        self.assertEqual(len(resolved.receipt['selections']), 3)
        self.assertEqual(resolved.receipt['view_digest'], view.digest)
        self.assertNotIn('supported', resolved.receipt)
        self.assertEqual([w['span'] for w in doc['windows']], self.spans[:2])

    def test_partial_unknown_and_unoffered_scopes_cannot_be_completed(self):
        complete = self.scope.view(self.spans[:2])
        whole = complete.documents[0]['source_scope']['complete_original_reference']
        partial = self.scope.view(self.spans[:1])
        metadata = partial.documents[0]['source_scope']
        self.assertEqual(metadata['coverage'], 'partial_original')
        self.assertEqual(metadata['missing_intervals'], [[23, self.a['extent']]])
        self.assertIsNone(metadata['complete_original_reference'])
        with self.assertRaises(SourceScopeError): partial.resolve([whole])
        unknown = SourceScope.bind([], self.spans[:2]).view(self.spans[:2])
        self.assertEqual(unknown.documents[0]['source_scope']['coverage'], 'unknown_original')
        self.assertIsNone(unknown.documents[0]['source_scope']['original_identity'])
        with self.assertRaises(SourceScopeError): unknown.resolve([whole])
        self.assertEqual(unknown.resolve([{'kind': 'passage', 'handle': 'a-first'}]).references,
                         [{'span_id': 'a-first'}])

    def test_whitespace_and_middle_gaps_are_real_missing_source(self):
        for ranges in ([(0, self.a['extent'] - 2)], [(0, 14), (15, self.a['extent'])]):
            pieces = [span(self.a, a, b, f'p{i}') for i, (a, b) in enumerate(ranges)]
            view = SourceScope.bind([self.a], pieces).view(pieces)
            self.assertEqual(view.documents[0]['source_scope']['coverage'], 'partial_original')

    def test_original_and_window_identity_cannot_be_forged(self):
        for key, value in [('content', self.a['content'] + 'x'), ('extent', self.a['extent'] + 1),
                           ('content_digest', '0' * 64), ('document_id', True)]:
            with self.subTest(key=key), self.assertRaises(SourceScopeError):
                SourceScope.bind([{**self.a, key: value}], self.spans[:2])
        for key, value in [('content', 'Altered'), ('start', True), ('end', 999),
                           ('content_digest', '0' * 64), ('feedback_open', True)]:
            changed = [{**self.spans[0], key: value}, self.spans[1]]
            with self.subTest(key=key), self.assertRaises(SourceScopeError): SourceScope.bind([self.a], changed)
        with self.assertRaises(SourceScopeError): SourceScope.bind([self.a, self.a], self.spans[:2])
        with self.assertRaises(SourceScopeError): SourceScope.bind([self.a], [self.spans[0], self.spans[0]])
        with self.assertRaises(SourceScopeError): self.scope.view([{**self.spans[0], 'content': 'Altered'}])
        with self.assertRaises(SourceScopeError): self.scope.view([{**self.spans[0], 'feedback_open': 0}])

    def test_partition_membership_and_immutable_copies(self):
        originals, supplied = copy.deepcopy([self.a, self.b]), copy.deepcopy(self.spans)
        scope = SourceScope.bind(originals, supplied)
        view = scope.view(supplied[:2])
        originals[0]['content'] = 'Mutated'; supplied[0]['content'] = 'Mutated'
        document_copy = view.documents; document_copy[0]['windows'].clear()
        self.assertEqual([d['document_id'] for d in view.documents], [42])
        self.assertEqual(len(view.documents[0]['windows']), 2)
        sibling = scope.view(self.spans[2:]).documents[0]['source_scope']['complete_original_reference']
        for ref in ({'kind': 'passage', 'handle': 'b-full'}, sibling,
                    {'kind': 'complete_original', 'handle': 'a-first'},
                    {'kind': 'passage', 'handle': 'a-first', 'complete': True}):
            with self.assertRaises(SourceScopeError): view.resolve([ref])
        resolution = view.resolve([{'kind': 'passage', 'handle': 'a-first'}])
        resolution.references.clear(); resolution.receipt['selections'].clear()
        self.assertEqual(len(resolution.references), 1)
        self.assertEqual(len(resolution.receipt['selections']), 1)

    def test_certified_chunk_offsets_are_not_original_offsets(self):
        chunk = self.a['content'][15:]
        piece = {'span_id': 'chunk', 'document_id': 42, 'start': 0, 'end': len(chunk),
                 'content': chunk, 'content_digest': digest(chunk), 'source_context': {
                     'document_id': 42, 'digest': self.a['content_digest'], 'start': 15, 'end': self.a['extent']}}
        view = SourceScope.bind([self.a], [piece]).view([piece])
        self.assertEqual(view.documents[0]['source_scope']['missing_intervals'], [[0, 15]])
        for changed in ({k: v for k, v in piece.items() if k != 'source_context'},
                        {**piece, 'source_context': {**piece['source_context'], 'start': 0}}):
            with self.assertRaises(SourceScopeError): SourceScope.bind([self.a], [changed])
