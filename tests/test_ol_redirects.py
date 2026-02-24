import os
import tempfile
import unittest

from app.author_db import AuthorDB
from app.ol_redirects import (
    discover_latest_redirect_dump_file,
    import_latest_redirect_dump,
    parse_redirect_dump_line,
    resolve_redirect_target,
    migrate_redirect_aliases,
)


class RedirectDumpTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.db_path = os.path.join(self.td.name, 'authors.sqlite')
        self.db = AuthorDB(self.db_path)
        self.addCleanup(lambda: self.db.close())

    def _write(self, name: str, lines: list[str]):
        with open(os.path.join(self.td.name, name), 'w', encoding='utf-8') as f:
            f.write(''.join(lines))

    def test_parse_redirect_line(self):
        line = '/type/redirect\t/authors/OL1A\t2\t2024-01-01T00:00:00\t{"type":{"key":"/type/redirect"},"key":"/authors/OL1A","location":"/authors/OL2A"}\n'
        rec = parse_redirect_dump_line(line)
        self.assertEqual(rec, ('/authors/OL1A', '/authors/OL2A', 2, '2024-01-01T00:00:00'))

    def test_latest_dump_selection(self):
        self._write('ol_dump_redirects_2026-01-31.txt', [])
        self._write('ol_dump_redirects_2025-12-31.txt', [])
        p, d = discover_latest_redirect_dump_file(self.td.name)
        self.assertTrue(str(p).endswith('ol_dump_redirects_2026-01-31.txt'))
        self.assertEqual(d, '2026-01-31')

    def test_chain_and_loop_resolution(self):
        m = {'A': 'B', 'B': 'C'}
        self.assertEqual(resolve_redirect_target(m, 'A')[0], 'C')
        loop_m = {'A': 'B', 'B': 'A'}
        target, loop = resolve_redirect_target(loop_m, 'A')
        self.assertTrue(loop)
        self.assertIn(target, ('A', 'B'))

    def test_resume_behavior_no_duplicates(self):
        lines = [
            '/type/redirect\t/authors/OL1A\t1\t2024-01-01\t{"location":"/authors/OL2A"}\n',
            '/type/redirect\t/authors/OL3A\t1\t2024-01-01\t{"location":"/authors/OL4A"}\n',
        ]
        self._write('ol_dump_redirects_2026-01-31.txt', lines)

        self.db.execute("INSERT INTO ol_import_runs(import_type,dump_date,dump_filename,status,started_at,updated_at,progress_line,rows_processed,redirects_stored,aliases_added,errors_count) VALUES('ol_redirects','2026-01-31','ol_dump_redirects_2026-01-31.txt','running',1,1,1,1,1,0,0)")
        self.db.execute("INSERT INTO ol_author_redirects(from_key,to_key,to_key_resolved,dump_date,last_modified,revision,updated_at) VALUES('/authors/OL1A','/authors/OL2A',NULL,'2026-01-31','2024-01-01',1,1)")

        res = import_latest_redirect_dump(self.db, self.td.name, checkpoint_every=1)
        self.assertTrue(res['ok'])
        cnt = self.db.query_one("SELECT COUNT(*) AS c FROM ol_author_redirects")['c']
        self.assertEqual(int(cnt), 2)

    def test_alias_migration_idempotent(self):
        self.db.execute("INSERT INTO author_dump_records(ol_key,last_modified,author_norm,canonical_name,dump_date,updated_at) VALUES('/authors/OL1A','2024','old norm','Old Name','2026-01-31',1)")
        self.db.execute("INSERT INTO author_dump_records(ol_key,last_modified,author_norm,canonical_name,dump_date,updated_at) VALUES('/authors/OL2A','2024','new norm','New Name','2026-01-31',1)")
        self.db.execute("INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES('old norm','Old Name','Old Name',1,1,1)")
        self.db.execute("INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES('new norm','New Name','New Name',1,1,1)")
        self.db.execute("INSERT INTO ol_author_redirects(from_key,to_key,to_key_resolved,dump_date,last_modified,revision,updated_at) VALUES('/authors/OL1A','/authors/OL2A',NULL,'2026-01-31','2024',1,1)")

        migrate_redirect_aliases(self.db, '2026-01-31')
        migrate_redirect_aliases(self.db, '2026-01-31')
        row = self.db.query_one("SELECT author_norm,source FROM author_aliases WHERE alias_norm='old norm'")
        self.assertEqual(str(row['author_norm']), 'new norm')
        self.assertEqual(str(row['source']), 'openlibrary_redirect')

    def test_new_version_marks_old_obsolete(self):
        self._write('ol_dump_redirects_2026-01-31.txt', ['/type/redirect\t/authors/OL1A\t1\t2024\t{"location":"/authors/OL2A"}\n'])
        self._write('ol_dump_redirects_2026-02-28.txt', ['/type/redirect\t/authors/OL3A\t1\t2024\t{"location":"/authors/OL4A"}\n'])

        self.db.execute("INSERT INTO ol_import_runs(import_type,dump_date,dump_filename,status,started_at,updated_at,progress_line,rows_processed,redirects_stored,aliases_added,errors_count) VALUES('ol_redirects','2026-01-31','ol_dump_redirects_2026-01-31.txt','completed',1,1,1,1,1,0,0)")
        res = import_latest_redirect_dump(self.db, self.td.name, checkpoint_every=1)
        self.assertTrue(res['ok'])
        old = self.db.query_one("SELECT status FROM ol_import_runs WHERE import_type='ol_redirects' AND dump_date='2026-01-31' ORDER BY id DESC LIMIT 1")
        self.assertEqual(str(old['status']), 'obsolete')


if __name__ == '__main__':
    unittest.main()
