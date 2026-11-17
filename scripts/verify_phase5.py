"""Phase 5 verification for the DuckDB job listing store.

Checks:
  * the table exists and has the expected columns;
  * every job_id has exactly one current version;
  * current rows have no duplicate URLs;
  * description / url / company / technologies coverage;
  * nullable salary fields are accepted;
  * re-ingesting the saved scrape adds no SCD versions;
  * the Jobby smoke query works.

Usage:
    python -m scripts.verify_phase5
    python -m scripts.verify_phase5 --db job_listings.duckdb --raw job_data_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

from storage import DB_FILE, TABLE, ingest_listings

DEFAULT_RAW = "job_data_raw.json"

REQUIRED_COLUMNS = {
    "surrogate_key",
    "job_id",
    "job_board",
    "url",
    "company",
    "title",
    "city",
    "description",
    "technologies",
    "salary_month",
    "salary_hour",
    "currency",
    "salary_type",
    "employment_types",
    "seniority",
    "work_mode",
    "application_url",
    "benefits",
    "date_posted",
    "scraped_at",
    "hash_diff",
    "effective_from",
    "effective_to",
    "is_current",
}

# Advisory thresholds. Missing descriptions are the only ones that directly
# block Jobby's determinator, so they are reported first.
COVERAGE_THRESHOLDS = {
    "url": 99.0,
    "title": 99.0,
    "description": 90.0,
    "company": 50.0,
    "technologies": 30.0,
}

SMOKE_SQL = """
SELECT url, title, company, description, technologies
FROM dim_job_listings
WHERE is_current
ORDER BY scraped_at DESC
LIMIT 5
"""


def percent(part: int, total: int) -> float:
    return round((part / total) * 100, 1) if total else 0.0


def flatten_raw(raw_data) -> list[dict]:
    if not isinstance(raw_data, dict):
        return []
    listings = []
    for board_listings in raw_data.values():
        if isinstance(board_listings, list):
            listings.extend(board_listings)
    return listings


def load_raw(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as file:
        return flatten_raw(json.load(file))


def check_schema(conn: duckdb.DuckDBPyConnection) -> list[str]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    if TABLE not in tables:
        return [f"table {TABLE} does not exist"]

    columns = {row[0] for row in conn.execute(f"DESCRIBE {TABLE}").fetchall()}
    missing = REQUIRED_COLUMNS - columns
    if missing:
        return [f"missing columns: {', '.join(sorted(missing))}"]
    return []


def check_invariants(conn: duckdb.DuckDBPyConnection) -> list[str]:
    problems = []

    total, current = conn.execute(
        f"""
        SELECT count(*), count(*) FILTER (WHERE is_current)
        FROM {TABLE}
        """
    ).fetchone()

    if total == 0:
        problems.append("table is empty; nothing has been ingested yet")
        return problems
    if current == 0:
        problems.append("no current rows")
        return problems

    duplicate_job_ids = conn.execute(
        f"""
        SELECT count(*)
        FROM (
            SELECT job_id
            FROM {TABLE}
            WHERE is_current
            GROUP BY job_id
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_job_ids:
        problems.append(
            f"{duplicate_job_ids} job_id(s) have more than one current version"
        )

    duplicate_urls = conn.execute(
        f"""
        SELECT count(*)
        FROM (
            SELECT url
            FROM {TABLE}
            WHERE is_current AND url IS NOT NULL
            GROUP BY url
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_urls:
        problems.append(f"{duplicate_urls} URL(s) appear in multiple current rows")

    orphan_currents = conn.execute(
        f"""
        SELECT count(*)
        FROM {TABLE}
        WHERE is_current AND (effective_from IS NULL OR scraped_at IS NULL)
        """
    ).fetchone()[0]
    if orphan_currents:
        problems.append(f"{orphan_currents} current row(s) lack effective_from/scraped_at")

    return problems


def board_breakdown(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, int]]:
    return conn.execute(
        f"""
        SELECT coalesce(job_board, 'unknown'), count(*)
        FROM {TABLE}
        WHERE is_current
        GROUP BY 1
        ORDER BY 2 DESC, 1
        """
    ).fetchall()


def coverage(conn: duckdb.DuckDBPyConnection) -> dict[str, tuple[int, int, float]]:
    total = conn.execute(
        f"SELECT count(*) FROM {TABLE} WHERE is_current"
    ).fetchone()[0]
    row = conn.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE url IS NOT NULL AND url <> ''),
            count(*) FILTER (WHERE title IS NOT NULL AND title <> ''),
            count(*) FILTER (WHERE description IS NOT NULL AND description <> ''),
            count(*) FILTER (WHERE company IS NOT NULL AND company <> ''),
            count(*) FILTER (WHERE technologies IS NOT NULL
                              AND array_length(technologies) > 0),
            count(*) FILTER (WHERE salary_month IS NOT NULL
                              OR salary_hour IS NOT NULL),
            count(*) FILTER (WHERE currency IS NOT NULL),
            count(*) FILTER (WHERE date_posted IS NOT NULL)
        FROM {TABLE}
        WHERE is_current
        """
    ).fetchone()

    names = (
        "url",
        "title",
        "description",
        "company",
        "technologies",
        "salary",
        "currency",
        "date_posted",
    )
    return {
        name: (count, total, percent(count, total))
        for name, count in zip(names, row)
    }


def check_idempotence(db_path: str, raw_path: str) -> tuple[bool, str, dict[str, int]]:
    listings = load_raw(raw_path)
    if not listings:
        return True, "skipped (no raw scrape file)", {}

    with tempfile.TemporaryDirectory() as tmp:
        copy_path = os.path.join(tmp, "phase5_copy.duckdb")
        shutil.copy2(db_path, copy_path)

        conn = duckdb.connect(copy_path, read_only=True)
        try:
            before = conn.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        finally:
            conn.close()

        summary = ingest_listings(copy_path, listings)

        conn = duckdb.connect(copy_path, read_only=True)
        try:
            after = conn.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        finally:
            conn.close()

    ok = after == before and summary["new"] == 0 and summary["updated"] == 0
    detail = (
        f"rows {before} -> {after}; new={summary['new']}, "
        f"updated={summary['updated']}, unchanged={summary['unchanged']}"
    )
    return ok, detail, summary


def print_smoke(conn: duckdb.DuckDBPyConnection) -> None:
    print("\nSmoke query (current listings):")
    rows = conn.execute(SMOKE_SQL).fetchall()
    if not rows:
        print("  no current rows")
        return
    for url, title, company, description, technologies in rows:
        desc = (description or "").replace("\n", " ").strip()
        print(f"  - {title!r} @ {company!r}")
        print(f"    url: {url}")
        print(f"    technologies: {technologies}")
        print(f"    description: {desc[:120]}{'...' if len(desc) > 120 else ''}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the Phase 5 DuckDB store")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--raw", default=DEFAULT_RAW)
    parser.add_argument(
        "--skip-idempotence",
        action="store_true",
        help="skip re-ingesting the saved scrape",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"FAIL: database not found: {args.db}")
        return 1

    conn = duckdb.connect(args.db, read_only=True)
    try:
        problems = check_schema(conn)
        if problems:
            for problem in problems:
                print(f"FAIL: {problem}")
            return 1

        problems = check_invariants(conn)
        stats = conn.execute(
            f"""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE is_current) AS current,
                   count(DISTINCT job_id) FILTER (WHERE is_current) AS current_ids
            FROM {TABLE}
            """
        ).fetchone()
        coverage_stats = coverage(conn)
        boards = board_breakdown(conn)
        print_smoke(conn)
    finally:
        conn.close()

    print("\nSCD state:")
    print(f"  total rows:      {stats[0]}")
    print(f"  current rows:    {stats[1]}")
    print(f"  current job_ids: {stats[2]}")

    print("\nCurrent rows by board:")
    for board, count in boards:
        print(f"  {board:12} {count:>7}")

    print("\nCurrent-row coverage:")
    for name, (count, total, pct) in coverage_stats.items():
        threshold = COVERAGE_THRESHOLDS.get(name)
        if threshold is None:
            print(f"  {name:12} {count:>7}/{total:<7} {pct:>5.1f}%")
        else:
            marker = "OK" if pct >= threshold else "LOW"
            print(
                f"  {name:12} {count:>7}/{total:<7} {pct:>5.1f}%  "
                f"{marker} (target {threshold:.0f}%)"
            )

    if not args.skip_idempotence:
        ok, detail, _ = check_idempotence(args.db, args.raw)
        print("\nIdempotence (re-ingest saved scrape):")
        print(f"  {'OK' if ok else 'FAIL'}: {detail}")
        if not ok:
            problems.append("re-ingesting unchanged data created SCD versions")

    # Description coverage is the hard requirement for Jobby's determinator.
    desc_pct = coverage_stats["description"][2]
    if desc_pct < COVERAGE_THRESHOLDS["description"]:
        problems.append(
            f"description coverage is {desc_pct}% "
            f"(target {COVERAGE_THRESHOLDS['description']:.0f}%)"
        )

    print("\nResult:")
    if problems:
        for problem in problems:
            print(f"  FAIL: {problem}")
        return 1

    print("  PASS: database is ready for Jobby")
    return 0


if __name__ == "__main__":
    sys.exit(main())
