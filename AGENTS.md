# AGENTS.md – BookDeduper Engineering Rules

## Core Principle
BookDeduper is a deterministic, resumable, project-scoped deduplication engine.
Stability and data safety > feature expansion.

---

## Repository Structure Rules

- main.py → entry point only
- app/db.py → database schema & access only
- app/parser.py → filename parsing logic only
- app/ranker.py → duplicate scoring logic only
- app/scanner.py → filesystem traversal only
- app/ui_* → UI only

Never mix responsibilities.

---

## Database Rules

- The SQLite file is the project boundary.
- Schema changes require:
  1. Increment schema version in state table.
  2. Add forward-compatible migration.
- No destructive ALTER without fallback.

---

## Thread Safety Rules

- Workers must emit Qt signals.
- No direct UI mutation from worker threads.
- DB writes must occur inside explicit transactions.

---

## Archive Handling Policy

- 7z listing must timeout gracefully.
- Archive inspection must never crash scan worker.
- No nested archive recursion.

---

## Ranking Rules

Preferred formats:
EPUB > AZW3 > AZW > MOBI > PDF > Others

Quality modifiers:
Retail > v5 > v4 > v3 > v2 > none > raw/unproofed

Never auto-delete best-ranked file.

---

## Deletion Safety

- Deletion uses send2trash only.
- Never use permanent delete.
- Deletions must flag folders for rescan.

---

## UI Constraints

- Tabs enabled sequentially.
- Analyze cannot run before scan completed.
- Review cannot run before analyze completed.

---

## When Adding Features

- Must preserve resumable state model.
- Must not break existing .sqlite projects.
- Must remain Windows-first.
