"""Immutable original supply and typed citation scope, never factual approval."""
from dataclasses import dataclass
import hashlib
import json
import re

from app.source_text import missing_intervals

VERSION = 'source-scope-v1'
_SHA = re.compile(r'[0-9a-f]{64}')
MODEL_NOTE = (
    'Use typed references with exactly kind and handle. A passage handle is an original '
    'window span_id in this document. A complete_original handle is the explicitly offered '
    'complete_original_reference in this document source_scope. It selects every supplied '
    'original window for that document; the application resolves those windows without '
    'inventing a quotation. Source_scope describes supplied OCR extent only, never factual '
    'truth, accurate OCR, complete physical records, a complete archive or current-world status. '
    'For a claim that the entire supplied original lacks a record or field, independently '
    'inspect that whole original and select its complete_original reference. Merely selecting '
    'one positive field does not cite a whole-original absence. Complete-original coverage '
    'does not require an affirmative written declaration that an absent field is absent. '
    'Partial or unknown coverage cannot establish whole-original absence, but an explicit '
    'unchecked field may support a narrower nonselection observation. No scope establishes '
    'that an event never happened or records elsewhere do not exist. Check all assertions '
    'against original text, conditions and alternatives; scope selection does not approve them. '
)


class SourceScopeError(ValueError):
    """Content-free original, view or reference binding failure."""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _require(condition):
    if not condition: raise SourceScopeError('invalid_source_scope')


def _identity(value):
    return type(value) is int and value > 0


def _span_position(span, original, chunk=None):
    """Return original coordinates only after exact chunk and slice binding."""
    content = original['content']
    context = span.get('source_context')
    if context is None:
        if chunk is None:
            _require(span['content_digest'] == original['content_digest'])
            offset, end = 0, len(content)
        else:
            offset, end = chunk['start'], chunk['end']
    else:
        _require(chunk is None and isinstance(context, dict) and _identity(context.get('document_id'))
                 and context['document_id'] == original['document_id']
                 and context.get('digest') == original['content_digest'])
        offset, end = context.get('start'), context.get('end')
        _require(type(offset) is int and type(end) is int and 0 <= offset < end <= len(content))
        _require(_digest(content[offset:end]) == span['content_digest'])
    first, last = span['start'], span['end']
    _require(0 <= first < last <= end - offset and content[offset + first:offset + last] == span['content'])
    return {'start': offset + first, 'end': offset + last}


@dataclass(frozen=True, repr=False)
class SourceScope:
    _snapshot: str

    @classmethod
    def bind(cls, originals, spans, *, chunks=()):
        """Admitted expected identities come from acquisition/diagnostic owners."""
        try:
            originals, spans, chunks = json.loads(_json([originals, spans, chunks]))
            _require(isinstance(originals, list) and isinstance(spans, list) and isinstance(chunks, list))
            sources, identities = {}, {}
            for original in originals:
                _require(isinstance(original, dict) and set(original) == {
                    'document_id', 'content', 'content_digest', 'extent'})
                doc_id, content = original['document_id'], original['content']
                _require(_identity(doc_id) and doc_id not in sources and isinstance(content, str)
                         and bool(content.strip()) and type(original['extent']) is int
                         and original['extent'] == len(content)
                         and original['content_digest'] == _digest(content))
                sources[doc_id] = original
                identities[doc_id] = {k: original[k] for k in ('document_id', 'content_digest', 'extent')}
            bindings, used = {}, set()
            for chunk in chunks:
                _require(isinstance(chunk, dict) and set(chunk) == {
                    'document_id', 'content_digest', 'start', 'end'}
                    and _identity(chunk['document_id']) and chunk['document_id'] in sources
                    and isinstance(chunk['content_digest'], str))
                key = (chunk['document_id'], chunk['content_digest'])
                content = sources[chunk['document_id']]['content']
                start, end = chunk['start'], chunk['end']
                _require(key not in bindings and type(start) is int and type(end) is int
                         and 0 <= start < end <= len(content)
                         and _digest(content[start:end]) == chunk['content_digest'])
                bindings[key] = chunk
            entries, handles = [], set()
            for ordinal, span in enumerate(spans):
                _require(isinstance(span, dict) and _identity(span.get('document_id'))
                         and isinstance(span.get('span_id'), str) and bool(span['span_id'].strip())
                         and span['span_id'] not in handles and span.get('feedback_open') is not True
                         and isinstance(span.get('content'), str) and bool(span['content'])
                         and isinstance(span.get('content_digest'), str)
                         and _SHA.fullmatch(span['content_digest']) is not None
                         and type(span.get('start')) is int and type(span.get('end')) is int
                         and 0 <= span['start'] < span['end']
                         and span['end'] - span['start'] == len(span['content']))
                handles.add(span['span_id'])
                original = sources.get(span['document_id'])
                # A claimed original context cannot be quietly demoted to legacy.
                _require(original is not None or span.get('source_context') is None)
                key = (span['document_id'], span['content_digest'])
                chunk = bindings.get(key)
                if chunk is not None: used.add(key)
                position = _span_position(span, original, chunk) if original else None
                entries.append({'ordinal': ordinal, 'span': span, 'original_interval': position})
            _require(used == set(bindings))
            return cls(_json({'version': VERSION, 'originals': list(identities.values()), 'entries': entries}))
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            raise SourceScopeError('invalid_source_scope') from None

    def view(self, supplied_spans):
        """Only these exact supplied windows contribute coverage or handles."""
        try:
            bound = json.loads(self._snapshot)
            supplied = json.loads(_json(supplied_spans))
            _require(isinstance(supplied, list))
            available = {e['span']['span_id']: e for e in bound['entries']}
            selected, seen = [], set()
            for span in supplied:
                _require(isinstance(span, dict) and isinstance(span.get('span_id'), str))
                handle = span['span_id']
                _require(handle in available and handle not in seen and _json(span) == _json(available[handle]['span']))
                seen.add(handle); selected.append(available[handle])
            ids = {e['span']['document_id'] for e in selected}
            return SourceScopeView(_json({'version': VERSION,
                'originals': [o for o in bound['originals'] if o['document_id'] in ids], 'entries': selected}))
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            raise SourceScopeError('invalid_source_view') from None


@dataclass(frozen=True, repr=False)
class ResolvedScope:
    _snapshot: str

    @property
    def references(self): return json.loads(self._snapshot)['references']

    @property
    def receipt(self): return json.loads(self._snapshot)['receipt']


@dataclass(frozen=True, repr=False)
class ScopedReading:
    _snapshot: str

    @classmethod
    def create(cls, reading, resolutions, view_digest):
        return cls(_json({'reading': reading, 'receipt': {'version': VERSION,
            'view_digest': view_digest, 'reading_digest': _digest(_json(reading)),
            'resolutions': resolutions}}))

    @property
    def reading(self): return json.loads(self._snapshot)['reading']

    @property
    def receipt(self): return json.loads(self._snapshot)['receipt']


    def validate(self, scope, documents):
        """Replay each selected scope against its document-local original view."""
        try:
            reading, receipt = self.reading, self.receipt
            windows = sorted((w for d in documents for w in d['windows']), key=lambda w: w['ordinal'])
            root = scope.view([w['span'] for w in windows])
            _require(set(receipt) == {'version', 'view_digest', 'reading_digest', 'resolutions'}
                     and receipt['version'] == VERSION and receipt['view_digest'] == root.digest
                     and receipt['reading_digest'] == _digest(_json(reading)))
            views = {d['document_id']: scope.view([w['span'] for w in d['windows']]) for d in documents}
            expected = []
            for document in reading['documents']:
                view = views[document['document_id']]
                for ordinal, observation in enumerate(document['observations']):
                    expected.append((document['document_id'], ordinal, view, observation['references']))
            _require(isinstance(receipt['resolutions'], list) and len(receipt['resolutions']) == len(expected))
            for row, (doc_id, ordinal, view, references) in zip(receipt['resolutions'], expected):
                _require(set(row) == {'document_id', 'observation_ordinal', 'resolution'}
                         and type(row['document_id']) is int and row['document_id'] == doc_id
                         and type(row['observation_ordinal']) is int and row['observation_ordinal'] == ordinal)
                replay = view.resolve([s['reference'] for s in row['resolution']['selections']])
                _require(_json(replay.receipt) == _json(row['resolution'])
                         and _json(replay.references) == _json(references))
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            raise SourceScopeError('invalid_scoped_reading_receipt') from None


@dataclass(frozen=True, repr=False)
class SourceScopeView:
    _snapshot: str

    @property
    def digest(self): return _digest(self._snapshot)

    @property
    def reference_schema(self):
        return {'type': 'object', 'additionalProperties': False, 'required': ['kind', 'handle'],
                'properties': {'kind': {'type': 'string', 'enum': ['passage', 'complete_original']},
                               'handle': {'type': 'string', 'minLength': 1}}}

    @property
    def documents(self):
        record = json.loads(self._snapshot)
        originals = {o['document_id']: o for o in record['originals']}
        documents = {}
        for entry in record['entries']:
            doc_id = entry['span']['document_id']
            documents.setdefault(doc_id, {'document_id': doc_id, 'windows': []})['windows'].append({
                'ordinal': entry['ordinal'], 'span': entry['span']})
        for doc_id, document in documents.items():
            identity = originals.get(doc_id)
            positions = [e['original_interval'] for e in record['entries'] if e['span']['document_id'] == doc_id]
            missing = missing_intervals(identity['extent'], positions) if identity else None
            complete = identity is not None and not missing
            handle = 'original:' + _digest(_json({'identity': identity, 'windows': document['windows']})) if complete else None
            # The typed namespace is distinct, but avoid ambiguous catalog text.
            _require(handle is None or handle not in {w['span']['span_id'] for w in document['windows']})
            document['source_scope'] = {'coverage': 'complete_original' if complete else
                ('partial_original' if identity else 'unknown_original'), 'original_identity': identity,
                'supplied_intervals': positions if identity else None, 'missing_intervals': missing,
                'complete_original_reference': {'kind': 'complete_original', 'handle': handle} if complete else None}
        return list(documents.values())

    def resolve(self, references):
        """Preserve selections; expand whole-original scope only within this view."""
        try:
            references = json.loads(_json(references))
            _require(isinstance(references, list))
            record = json.loads(self._snapshot)
            passages = {e['span']['span_id']: e for e in record['entries']}
            originals = {d['source_scope']['complete_original_reference']['handle']: d
                         for d in self.documents if d['source_scope']['complete_original_reference']}
            selections, expanded = [], {}
            for reference in references:
                _require(isinstance(reference, dict) and set(reference) == {'kind', 'handle'}
                         and isinstance(reference['handle'], str))
                kind, handle = reference['kind'], reference['handle']
                if kind == 'passage':
                    _require(handle in passages)
                    entry = passages[handle]
                    ids = [handle]
                    selected = {'document_id': entry['span']['document_id'],
                                'original_interval': entry['original_interval']}
                else:
                    _require(kind == 'complete_original' and handle in originals)
                    document = originals[handle]
                    ordered = sorted((passages[w['span']['span_id']] for w in document['windows']),
                                     key=lambda e: (e['original_interval']['start'], e['original_interval']['end'], e['ordinal']))
                    ids = [e['span']['span_id'] for e in ordered]
                    selected = {'document_id': document['document_id'], 'supply_scope': document['source_scope']}
                selections.append({'reference': reference, **selected, 'expanded_span_ids': ids})
                for span_id in ids: expanded.setdefault(span_id, {'span_id': span_id})
            receipt = {'version': VERSION, 'view_digest': self.digest, 'selections': selections,
                       'expanded_span_ids': list(expanded)}
            receipt['digest'] = _digest(_json(receipt))
            return ResolvedScope(_json({'references': list(expanded.values()), 'receipt': receipt}))
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            raise SourceScopeError('invalid_scope_reference') from None
