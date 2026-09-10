"""Exact context edges without repeatedly normalizing whole originals."""
import random
import unittest
from unittest.mock import patch
from app import answer_finalization as final
from app import source_dates as dates

class BoundaryContextCostTests(unittest.TestCase):
    def test_value_edges_equal_full_transform(self):
        rng = random.Random(183)
        texts = ['a' * 100000, 'ID' + '*' * 9000 + '  123',
                 'foo' + ' \t' * 8000 + 'bar', 'a\r\n\u00a0**[x]** b']
        texts += [''.join(rng.choices('ab  \t\n*_`[]123\u00a0', k=500)) for _ in range(30)]
        for text in texts:
            markers = [[12, min(100, len(text))]] if len(text) > 100 else []
            for start, end in [(0,len(text)), (0,len(text)//2), (len(text)//3,len(text))]:
                local = final._slice_markers(markers,start,end)
                expected = final._value_context(text[start:end],local)
                for tail, width in [(True,16),(False,2)]:
                    actual = final._value_edge(text, markers, start, end, width=width, tail=tail)
                    self.assertEqual(actual, expected[-width:] if tail else expected[:width])

    def test_overlapping_marker_positional_behavior_is_preserved(self):
        text = 'x' * 300
        markers = [[0,200], [100,200]]
        expected = final._value_context(text, markers)
        self.assertEqual(final._value_edge(text, markers, 0, len(text), width=16, tail=True), expected[-16:])

    def test_ordinary_edge_work_is_independent_of_document_length(self):
        original = final._value_context
        examined = []
        def measured(text, markers=()):
            examined.append(len(text)); return original(text, markers)
        text = 'Station pressure remains normal. ' * 40000
        with patch.object(final, '_value_context', measured):
            final._value_edge(text, [], 0, len(text), width=16, tail=True)
            final._value_edge(text, [], 0, len(text), width=2, tail=False)
        self.assertLess(sum(examined), 1000)

    def test_date_edges_equal_full_transform(self):
        import re
        rng = random.Random(852)
        texts = ['ID' + '*' * 9000 + ' 123', 'abc' + ' ' * 20000,
                 'x' * 100000, 'a\u00a0b\r\n']
        texts += [''.join(rng.choices('ab  \t\n*_`[]123\u00a0', k=800)) for _ in range(30)]
        for text in texts:
            for end in [0,len(text)//2,len(text)]:
                prefix = text[:end]
                expected = ' '.join(re.sub(r'[*_`]', '', prefix).split())[-80:] + (' ' if prefix and prefix[-1].isspace() else '')
                self.assertEqual(dates.date_context(text, end=end), expected)
