"""Clock abbreviations neither demand nor supply metre evidence."""
import unittest
from app.answer_finalization import value_mismatches
from app.source_quantities import unit_names, prose_quantities


class MeridiemUnitTests(unittest.TestCase):
    def test_clock_abbreviation_does_not_require_metres(self):
        for marker, source_marker in [('a.m.', 'AM'), ('p.m.', 'PM'), ('A.M.', 'AM'), ('P.M.', 'PM')]:
            with self.subTest(marker=marker):
                self.assertEqual(value_mismatches(f'Time: 12:01 {marker}',
                                 [{'quote': f'Time: 12:01 {source_marker}'}]), {})
                self.assertNotIn('m', unit_names(f'Time: 12:01 {marker}'))

    def test_clock_abbreviation_cannot_supply_measurement(self):
        for marker in ('a.m.', 'p.m.'):
            with self.subTest(marker=marker):
                result = value_mismatches('Length: 12 m.', [{'quote': f'Time: 12 {marker}'}])
                self.assertIn('m', result['units'])
                self.assertIn('12 m', result['quantities'])
                self.assertEqual(prose_quantities(f'Time: 12 {marker}'), set())

    def test_real_metres_and_changed_times_keep_checks(self):
        self.assertEqual(value_mismatches('Length: 12 m.', [{'quote': 'Length: 12 m.'}]), {})
        self.assertTrue(value_mismatches('Length: 13 m.', [{'quote': 'Length: 12 m.'}]))
        self.assertTrue(value_mismatches('Time: 13:01 a.m.', [{'quote': 'Time: 12:01 AM.'}]))
        self.assertEqual(unit_names('Time: 12:01 a.m.; length: 3 m and speed: 4 m/s.'), {'m', 'm/s'})

    def test_complete_standalone_marker_boundaries(self):
        for text in ('(a.m.)', 'p.m.;', 'A.m.', 'P.m.'):
            with self.subTest(text=text):
                self.assertNotIn('m', unit_names(text))
        for text in ('xa.m.', 'a.m.x', 'a.m./s', 'kg/a.m.', 'a.m'):
            with self.subTest(text=text):
                self.assertIn('m', unit_names(text))
