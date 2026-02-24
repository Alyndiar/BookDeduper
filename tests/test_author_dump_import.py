import os
import tempfile
import unittest

from app.util import discover_latest_author_dump_file, extract_author_name_from_dump_line, is_single_token_name


class TestAuthorDumpHelpers(unittest.TestCase):
    def testdiscover_latest_author_dump_file(self):
        with tempfile.TemporaryDirectory() as td:
            open(os.path.join(td, 'ol_dump_authors_2026-01-31.txt'), 'w').close()
            open(os.path.join(td, 'ol_dump_authors_2025-12-31.txt'), 'w').close()
            open(os.path.join(td, 'other.txt'), 'w').close()
            path, date = discover_latest_author_dump_file(td)
            self.assertTrue(path.endswith('ol_dump_authors_2026-01-31.txt'))
            self.assertEqual(date, '2026-01-31')

    def test_extract_name_from_dump_line(self):
        line = '/type/author\t/authors/OL1A\t1\t2021-01-01T00:00:00\t{"name":"Alain P\\u00e9chereau"}\n'
        self.assertEqual(extract_author_name_from_dump_line(line), 'Alain Péchereau')

    def test_single_token(self):
        self.assertTrue(is_single_token_name('barringer'))
        self.assertFalse(is_single_token_name('karen elizabeth gordon'))


if __name__ == '__main__':
    unittest.main()
