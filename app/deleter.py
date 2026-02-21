from __future__ import annotations
from typing import Tuple
from send2trash import send2trash
from .util import norm_path

def delete_checked(db) -> Tuple[int, int]:
    rows = db.query_all(
        """
        SELECT dq.id AS dqid, dq.file_id, f.path, f.folder_path
        FROM deletion_queue dq
        JOIN files f ON f.id = dq.file_id
        WHERE dq.checked=1
        """
    )
    deleted = 0
    failed = 0
    affected_folders = set()

    db.begin()
    try:
        for r in rows:
            p = r["path"]
            folder = r["folder_path"]
            try:
                send2trash(p)
                db.execute("DELETE FROM files WHERE id=?", (int(r["file_id"]),))
                db.execute("DELETE FROM deletion_queue WHERE id=?", (int(r["dqid"]),))
                deleted += 1
                affected_folders.add(norm_path(folder))
            except Exception:
                failed += 1

        db.execute(
            """
            DELETE FROM works
            WHERE work_key IN (
              SELECT w.work_key FROM works w
              LEFT JOIN files f ON f.work_key = w.work_key
              GROUP BY w.work_key
              HAVING COUNT(f.id)=0
            )
            """
        )

        for folder in affected_folders:
            db.execute("UPDATE folders SET force_rescan=1 WHERE path=?", (folder,))

        db.commit()
    except Exception:
        db.rollback()
        raise

    return deleted, failed
