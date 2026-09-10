import copy
import hashlib
import json
import unittest

from app.source_records import SourceRecordError, SourceRecordInventory


def original(text, doc_id=1):
    return {'document_id': doc_id, 'content': text,
            'content_digest': hashlib.sha256(text.encode()).hexdigest()}


def unresolved(inventory):
    return {'blocks': [{'block_id': b['block_id'], 'status': 'unresolved',
                        'observations': [], 'reason': 'Not interpreted yet'}
                       for d in inventory.record['documents'] for b in d['blocks']]}


class SourceRecordTests(unittest.TestCase):
    def test_exact_unicode_physical_coverage_and_structural_blocks(self):
        text = '\r\n# Rêcord\r\n\r\nFirst clause.\r\n\r\n| Item | Value |\r\n| --- | --- |\r\n| X | 12 |\r\n\r\n- Conditional list\r\n  - Nested item\r\n\r\n```\r\nopaque\r\n\r\ncode\r\n```\r\n\r\n'
        inventory = SourceRecordInventory.build([original(text)])
        blocks = inventory.record['documents'][0]['blocks']
        self.assertEqual(''.join(b['content'] for b in blocks), text)
        self.assertEqual(blocks[0]['start'], 0)
        self.assertEqual(blocks[-1]['end'], len(text))
        for left, right in zip(blocks, blocks[1:]):
            self.assertEqual(left['end'], right['start'])
        self.assertTrue(any('| Item | Value |' in b['content'] and '| X | 12 |' in b['content'] for b in blocks))
        self.assertTrue(any('- Conditional list' in b['content'] and '- Nested item' in b['content'] for b in blocks))
        self.assertTrue(any('opaque\r\n\r\ncode' in b['content'] for b in blocks))
        self.assertEqual(SourceRecordInventory.build([original(text)]).digest, inventory.digest)

    def test_unknown_syntax_and_large_block_are_never_dropped_or_truncated(self):
        for text in ('<!-- source comment -->\n\nraw\u2028separator', 'x' * 100000):
            record = SourceRecordInventory.build([original(text)]).record
            self.assertEqual(''.join(b['content'] for b in record['documents'][0]['blocks']), text)

    def test_rejects_source_identity_and_digest_drift(self):
        for sources in ([], [original('')], [original(' \r\n')], [original('valid', True)],
                        [original('valid'), original('another')], [{**original('valid'), 'content': 'changed'}],
                        [{**original('valid'), 'extra': 'metadata'}]):
            with self.subTest(sources=sources), self.assertRaises(SourceRecordError):
                SourceRecordInventory.build(sources)

    def test_unresolved_accounting_is_not_semantic_completeness_and_is_immutable(self):
        sources = [original('Heading\n\nMaterial condition.')]
        inventory = SourceRecordInventory.build(sources)
        before = inventory.digest
        sources[0]['content'] = 'mutated'
        view = inventory.record
        view['documents'].clear()
        payload = unresolved(inventory)
        bound = inventory.bind_reading(json.dumps(payload))
        payload['blocks'].clear()
        result = bound.record
        self.assertEqual(result['receipt']['inventory_digest'], before)
        self.assertTrue(result['receipt']['accounting_complete'])
        self.assertEqual(result['receipt']['unresolved_blocks'], 2)
        self.assertEqual(result['receipt']['semantic_support'], 'not_evaluated')
        self.assertEqual(result['receipt']['question_coverage'], 'not_evaluated')
        result['inventory']['documents'].clear()
        self.assertTrue(bound.record['inventory']['documents'])

    def test_own_block_and_same_document_qualification_references_required(self):
        inventory = SourceRecordInventory.build([original('Subject.\n\nCondition.'), original('Other.', 2)])
        payload = unresolved(inventory)
        own, qualifier, foreign = [r['block_id'] for r in payload['blocks']]
        row = payload['blocks'][0]
        row.update(status='interpreted', reason=None, observations=[{'text': 'Source-qualified assertion.',
            'references': [{'block_id': own}, {'block_id': qualifier}]}])
        self.assertEqual(inventory.bind_reading(json.dumps(payload)).record['blocks'][0], row)
        for refs in ([], [qualifier], [own, foreign], [own, own], [own, 'fabricated']):
            bad = copy.deepcopy(payload)
            bad['blocks'][0]['observations'][0]['references'] = [{'block_id': r} for r in refs]
            with self.subTest(refs=refs), self.assertRaises(SourceRecordError):
                inventory.bind_reading(json.dumps(bad))

    def test_missing_duplicate_extra_and_inconsistent_records_fail_closed(self):
        inventory = SourceRecordInventory.build([original('A.\n\nB.')])
        payload = unresolved(inventory)
        bads = [dict(blocks=payload['blocks'][:1]), dict(blocks=payload['blocks'] * 2), {**payload, 'extra': 1}]
        for changes in ({'status': 'interpreted'}, {'reason': ''}, {'status': 'irrelevant'}, {'extra': True},
                        {'observations': [{'text': 'unresolved cannot assert', 'references': []}]}):
            bad = copy.deepcopy(payload); bad['blocks'][0].update(changes); bads.append(bad)
        for bad in bads:
            with self.subTest(bad=bad), self.assertRaises(SourceRecordError):
                inventory.bind_reading(json.dumps(bad))
        for text in ('{"blocks":[],"blocks":[]}', '{"blocks":NaN}', '{"blocks":Infinity}', 'not json'):
            with self.subTest(text=text), self.assertRaises(SourceRecordError):
                inventory.bind_reading(text)
