from __future__ import annotations
from typing import Callable, Optional, Tuple
from send2trash import send2trash
from .util import norm_path

def delete_checked(
    db,
    progress_cb: Optional[Callable[[str, float], None]] = None,
    stop_fn: Optional[Callable[[], bool]] = None,
) -> Tuple[int, int]:
    """Send all checked files in the deletion_queue to the Recycle Bin.

    Args:
        db: The project DB instance.
        progress_cb: Optional callable(msg, pct) called every ~5 files. pct in [0, 100].
        stop_fn: Optional callable() -> bool; if it returns True the loop stops early.

    Returns:
        (deleted, failed) counts.
    """
    rows = db.query_all(
        """
        SELECT dq.id AS dqid, dq.file_id, f.path, f.folder_path
        FROM deletion_queue dq
        JOIN files f ON f.id = dq.file_id
        WHERE dq.checked=1
        """
    )
    total = max(1, len(rows))
    deleted = 0
    failed = 0
    affected_folders = set()

    db.begin()
    try:
        for i, r in enumerate(rows):
            if stop_fn and stop_fn():
                break
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
            if progress_cb and (i % 5 == 0 or i == len(rows) - 1):
                remaining = total - i - 1
                pct = 100.0 * (i + 1) / total
                progress_cb(
                    f"Sending to Recycle Bin: {deleted} done, {failed} failed, {remaining} left",
                    pct,
                )

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
