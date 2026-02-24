import os
import tempfile
import unittest

from app.db import DB
from app.author_db import AuthorDB
import app.analyzer as analyzer_mod


class _Parsed:
    def __init__(self, author: str, norm: str):
        self.author = author
        self.author_norm = norm
        self.author_confidence = 0.95


class AuthorSingleTokenPolicyTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix='.sqlite')
        os.close(fd)
        fd2, self.author_path = tempfile.mkstemp(suffix='.sqlite')
        os.close(fd2)
        self.db = DB(self.path)
        self.author_db = AuthorDB(self.author_path)
        self.db.set_state('scan_completed', '1')
        self.db.execute(
            "INSERT INTO files(root_id,folder_path,path,name,ext,size,mtime_ns,ctime_ns,is_archive,inner_ext_guess,author,series,series_index,title,tags,author_norm,series_norm,title_norm,work_key,last_seen_scan_id) VALUES(1,'r','/r/a.epub','a.epub','epub',1,1,1,0,NULL,'','','', '', '', '', '', '', '', 1)"
        )

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        try:
            self.author_db.close()
        except Exception:
            pass
        for p in (self.path, self.author_path):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    def test_single_token_accepted_if_already_approved(self):
        self.author_db.execute(
            "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES('barringer','BARRINGER','BARRINGER',1,1,1)"
        )
        old = analyzer_mod.parse_filename
        analyzer_mod.parse_filename = lambda _name, invalid_authors=None: _Parsed('BARRINGER', 'barringer')
        try:
            w = analyzer_mod.AnalyzeWorker(self.db, phase='authors_seed', author_db=self.author_db)
            ok, _ = w._run_preseed_authors()
            self.assertTrue(ok)
        finally:
            analyzer_mod.parse_filename = old

        row = self.db.query_one("SELECT frequency FROM author_variants WHERE normalized_name='barringer' AND variant_text='BARRINGER'")
        self.assertIsNotNone(row)

    def test_single_token_rejected_if_not_approved(self):
        old = analyzer_mod.parse_filename
        analyzer_mod.parse_filename = lambda _name, invalid_authors=None: _Parsed('BARRINGER', 'barringer')
        try:
            w = analyzer_mod.AnalyzeWorker(self.db, phase='authors_seed', author_db=self.author_db)
            ok, _ = w._run_preseed_authors()
            self.assertTrue(ok)
        finally:
            analyzer_mod.parse_filename = old

        row = self.db.query_one("SELECT 1 FROM tentative_authors WHERE normalized_name='barringer'")
        self.assertIsNone(row)


if __name__ == '__main__':
    unittest.main()
