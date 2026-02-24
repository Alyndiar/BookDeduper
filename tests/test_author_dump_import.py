import os
import tempfile
import unittest

from app.util import discover_latest_author_dump_file, parse_author_dump_line


class TestAuthorDumpHelpers(unittest.TestCase):
    def test_discover_latest_author_dump_file(self):
        with tempfile.TemporaryDirectory() as td:
            open(os.path.join(td, 'ol_dump_authors_2026-01-31.txt'), 'w').close()
            open(os.path.join(td, 'ol_dump_authors_2025-12-31.txt'), 'w').close()
            open(os.path.join(td, 'other.txt'), 'w').close()
            path, date = discover_latest_author_dump_file(td)
            self.assertTrue(path.endswith('ol_dump_authors_2026-01-31.txt'))
            self.assertEqual(date, '2026-01-31')

    def test_parse_author_dump_line(self):
        line = '/type/author\t/authors/OL1A\t1\t2021-01-01T00:00:00\t{"key":"/authors/OL1A","name":"Alain P\\u00e9chereau","personal_name":"Alain P.","alternate_names":["A. Pechereau","Alain Pechereau"],"fuller_name":"Alain Pechereau"}\n'
        rec = parse_author_dump_line(line)
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec['ol_key'], '/authors/OL1A')
        self.assertEqual(rec['canonical_name'], 'Alain Péchereau')
        self.assertEqual(rec['canonical_norm'], 'alain pechereau')
        self.assertIn('a pechereau', rec['aliases_norm'])
        self.assertIn('alain pechereau', rec['aliases_norm'])

    def test_parse_author_dump_line_uses_tab_last_modified_when_missing_obj(self):
        line = '/type/author\t/authors/OL1A\t1\t2021-01-01T00:00:00\t{"key":"/authors/OL1A","name":"Name Only"}\n'
        rec = parse_author_dump_line(line)
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec['last_modified'], '2021-01-01T00:00:00')


if __name__ == '__main__':
    unittest.main()
