"""Exact source-block accounting for diagnostics; never semantic certification."""
from dataclasses import dataclass
import hashlib
import json
import re

from markdown_it import MarkdownIt

VERSION = 'source-record-inventory-v1'
_MARKDOWN = MarkdownIt('commonmark').enable('table')


class SourceRecordError(ValueError):
    """Content-free input, identity and record-binding failure."""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _hash(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _require(condition):
    if not condition:
        raise SourceRecordError('invalid_source_record')


def _nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def _ranges(text):
    # Markdown maps physical CR/LF lines. Descendant tokens cannot subdivide a
    # top-level table/list and discard the header or its enclosing conditions.
    lines = [m.group() for m in re.finditer(r'[^\r\n]*(?:\r\n|\r|\n|$)', text) if m.group()]
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    starts = sorted({offsets[t.map[0]] for t in _MARKDOWN.parse(text)
                     if t.level == 0 and t.map is not None})
    # Whitespace and syntax outside parser maps are retained, never filtered.
    cuts = [0, *starts[1:], len(text)]
    return [(first, last) for first, last in zip(cuts, cuts[1:]) if first < last]


@dataclass(frozen=True)
class BoundSourceRecords:
    _snapshot: str

    @property
    def record(self):
        return json.loads(self._snapshot)


@dataclass(frozen=True)
class SourceRecordInventory:
    _snapshot: str

    @classmethod
    def build(cls, originals):
        """Freeze exact complete originals; expected digests come from the caller."""
        try:
            _require(isinstance(originals, list) and bool(originals))
            documents, seen = [], set()
            for original in originals:
                _require(isinstance(original, dict) and set(original) == {'document_id', 'content', 'content_digest'})
                doc_id, content, digest = (original[k] for k in ('document_id', 'content', 'content_digest'))
                _require(type(doc_id) is int and doc_id > 0 and doc_id not in seen)
                _require(_nonempty(content) and isinstance(digest, str) and _hash(content) == digest)
                seen.add(doc_id)
                blocks = [{'block_id': f'{doc_id}:{digest}:{ordinal}:{first}:{last}',
                           'ordinal': ordinal, 'start': first, 'end': last, 'content': content[first:last]}
                          for ordinal, (first, last) in enumerate(_ranges(content))]
                _require(''.join(b['content'] for b in blocks) == content)
                documents.append({'document_id': doc_id, 'content_digest': digest,
                                  'extent': len(content), 'blocks': blocks})
            return cls(_json({'version': VERSION, 'documents': documents}))
        except (TypeError, ValueError, KeyError, UnicodeError, RecursionError):
            raise SourceRecordError('invalid_source_inventory') from None

    @property
    def record(self):
        return json.loads(self._snapshot)

    @property
    def digest(self):
        return _hash(self._snapshot)

    def bind_reading(self, text):
        """Account every inventory block. Owned references remain unverified claims."""
        def unique(pairs):
            result = {}
            for key, value in pairs:
                _require(key not in result)
                result[key] = value
            return result

        def invalid_constant(value):
            raise SourceRecordError('invalid_source_record')

        try:
            _require(isinstance(text, str))
            raw = json.loads(text, object_pairs_hook=unique, parse_constant=invalid_constant)
            _require(isinstance(raw, dict) and set(raw) == {'blocks'} and isinstance(raw['blocks'], list))
            owned = {b['block_id']: d['document_id'] for d in self.record['documents'] for b in d['blocks']}
            rows = {}
            for row in raw['blocks']:
                _require(isinstance(row, dict) and set(row) == {'block_id', 'status', 'observations', 'reason'})
                block_id = row['block_id']
                _require(isinstance(block_id, str) and block_id in owned and block_id not in rows)
                _require(row['status'] in ('interpreted', 'unresolved') and isinstance(row['observations'], list))
                if row['status'] == 'unresolved':
                    _require(not row['observations'] and _nonempty(row['reason']))
                else:
                    _require(bool(row['observations']) and row['reason'] is None)
                    for observation in row['observations']:
                        _require(isinstance(observation, dict) and set(observation) == {'text', 'references'})
                        _require(_nonempty(observation['text']) and isinstance(observation['references'], list))
                        refs = []
                        for ref in observation['references']:
                            _require(isinstance(ref, dict) and set(ref) == {'block_id'})
                            target = ref['block_id']
                            _require(isinstance(target, str) and target in owned and owned[target] == owned[block_id])
                            _require(target not in refs)
                            refs.append(target)
                        _require(block_id in refs)
                rows[block_id] = row
            _require(set(rows) == set(owned))
            ordered = [rows[block_id] for block_id in owned]
            receipt = {'version': VERSION, 'inventory_digest': self.digest,
                       'reading_digest': _hash(_json(ordered)), 'accounted_blocks': len(rows),
                       'inventory_blocks': len(owned), 'accounting_complete': True,
                       'unresolved_blocks': sum(row['status'] == 'unresolved' for row in ordered),
                       'semantic_support': 'not_evaluated', 'question_coverage': 'not_evaluated',
                       'archive_completeness': 'not_established'}
            return BoundSourceRecords(_json({'inventory': self.record, 'blocks': ordered, 'receipt': receipt}))
        except (TypeError, ValueError, KeyError, UnicodeError, RecursionError):
            raise SourceRecordError('invalid_source_reading_binding') from None
