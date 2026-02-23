import os
import tempfile
import unittest

from app.db import DB
from app.analyzer import AnalyzeWorker


class AnalyzeResumeTests(unittest.TestCase):
    def _make_db(self, profile: str) -> DB:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        db = DB(path)
        db.set_state("scan_completed", "1")
        db.set_state("memory_profile", profile)
        return db

    def _seed_duplicate_data(self, db: DB):
        # two duplicate works: a and b
        rows = [
            (1, "r", "/r/a1.epub", "a1.epub", "epub", 100, 1, 1, 0, None, "A", None, None, "T", "", "a", "", "t", "a", 1),
            (1, "r", "/r/a2.epub", "a2.epub", "epub", 90, 2, 2, 0, None, "A", None, None, "T", "", "a", "", "t", "a", 1),
            (1, "r", "/r/b1.epub", "b1.epub", "epub", 120, 3, 3, 0, None, "B", None, None, "T", "", "b", "", "t", "b", 1),
            (1, "r", "/r/b2.epub", "b2.epub", "epub", 110, 4, 4, 0, None, "B", None, None, "T", "", "b", "", "t", "b", 1),
        ]
        db.executemany(
            """
            INSERT INTO files(
              root_id,folder_path,path,name,ext,size,mtime_ns,ctime_ns,is_archive,inner_ext_guess,
              author,series,series_index,title,tags,author_norm,series_norm,title_norm,work_key,last_seen_scan_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        db.executemany(
            "INSERT INTO works(work_key,author_norm,series_norm,series_index_norm,title_norm,display_author,display_series,display_series_index,display_title) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                ("a", "a", "", "", "t", "A", "", "", "T"),
                ("b", "b", "", "", "t", "B", "", "", "T"),
            ],
        )

    def test_duplicates_resume_keeps_existing_queue_balanced(self):
        db = self._make_db("balanced")
        self._seed_duplicate_data(db)
        db.execute("INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)", ("a", 1, 0, "KEEP (best)", 1))
        db.execute("INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)", ("a", 2, 1, "Lower rank", 1))
        db.set_state("analyze_last_work_key", "a")
        db.set_state("analyze_duplicates_completed", "0")

        worker = AnalyzeWorker(db, phase="duplicates", commit_every=100)
        ok, _msg = worker._run_analyze_duplicates()
        self.assertTrue(ok)

        rows_a = db.query_all("SELECT 1 FROM deletion_queue WHERE work_key='a'")
        rows_b = db.query_all("SELECT 1 FROM deletion_queue WHERE work_key='b'")
        self.assertEqual(len(rows_a), 2)
        self.assertEqual(len(rows_b), 2)

    def test_duplicates_resume_keeps_existing_queue_extreme(self):
        db = self._make_db("extreme")
        self._seed_duplicate_data(db)
        db.execute("INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)", ("a", 1, 0, "KEEP (best)", 1))
        db.execute("INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)", ("a", 2, 1, "Lower rank", 1))
        db.set_state("analyze_last_work_key", "a")
        db.set_state("analyze_duplicates_completed", "0")

        worker = AnalyzeWorker(db, phase="duplicates", commit_every=100)
        ok, _msg = worker._run_analyze_duplicates()
        self.assertTrue(ok)

        rows_a = db.query_all("SELECT 1 FROM deletion_queue WHERE work_key='a'")
        rows_b = db.query_all("SELECT 1 FROM deletion_queue WHERE work_key='b'")
        self.assertEqual(len(rows_a), 2)
        self.assertEqual(len(rows_b), 2)


if __name__ == "__main__":
    unittest.main()
