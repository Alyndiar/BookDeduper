# BookDeduper

BookDeduper is a desktop app for finding and reviewing duplicate ebook files across one or more folders.
It uses a local SQLite project database to track scan results, then ranks duplicates and prepares a review queue before deletion.

## Features

- **Project-based workflow** backed by a single SQLite file.
- **Root folder management** (add/remove/enable/disable scan roots).
- **Incremental scanning** with pause/resume/stop and checkpoint state.
- **Filename parsing** for author/series/title normalization and work-key grouping.
- **Duplicate analysis** that ranks candidate files and marks lower-ranked entries for deletion.
- **Review UI** to inspect each work group, toggle checkmarks, and save selection.
- **Safe deletion** via recycle bin/trash (`send2trash`), not permanent delete.
- Optional **7-Zip integration** to inspect archive contents and infer inner file format.

## Requirements

- Python 3.10+
- `pip` for dependency installation
- Optional: `7z` / 7-Zip executable (for archive introspection)

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Typical workflow

1. Open the app.
2. In **1) Project**, open or create a `.sqlite` project file.
3. In **2) Roots**, add one or more library folders.
4. In **3) Scan**, run scan (pause/resume supported).
5. In **4) Analyze**, optionally run **Pre-seed Authors** to build tentative/variant author data first, then run **Analyze Authors** to finalize merge suggestions, and **Analyze Duplicates** for deletion recommendations.
6. In **5) Review/Delete**, inspect per-work decisions and delete checked files to recycle bin.

## Notes

- The app stores operational state (last actions, scan checkpoints, settings) inside the project DB.
- Analyze requires a completed scan. Author pre-seeding is a separate Analyze action you can run before full author analysis.
- Review/Delete requires completed analysis.
- If 7-Zip is unavailable, archive inner format guessing falls back to filename tags only.
- Authors tab supports importing approved authors from local Open Library dump files named `ol_dump_authors_YYYY-MM-DD.txt` stored next to the project DB, with resumable progress tracking.

## Project structure

- `main.py` — Qt app entrypoint.
- `app/ui_*.py` — tabs and UI workflow.
- `app/scanner.py` — filesystem scan worker.
- `app/analyzer.py` — duplicate grouping/ranking queue builder.
- `app/deleter.py` — checked-file trash workflow.
- `app/db.py` — SQLite schema and DB helper.
- `app/parser.py`, `app/ranker.py`, `app/util.py`, `app/sevenzip.py` — core helpers.

## Troubleshooting

- To profile which JSON fields are present in a large Open Library dump before parser changes, run:

  ```bash
  python tools/analyze_ol_dump_fields.py /path/to/ol_dump_authors_YYYY-MM-DD.txt --progress-every 500000
  ```

- If startup fails, verify dependencies are installed and run:

  ```bash
  python -m py_compile main.py app/*.py
  ```

- If archive detection does not work, install 7-Zip and ensure `7z` is in PATH (or use the app's re-detect button after opening a project).
