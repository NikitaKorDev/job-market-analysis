"""Per-board field coverage of the current rows in ``dim_job_listings``.

Prints, for each job_board, the share of current rows that have:
technologies, technologies_required (not NULL), a non-empty technologies_nice,
seniority (board text), seniority_levels (not NULL),
work_mode (plus its distribution) and description. Columns that do not exist
yet (an old database) are reported as ``n/a``.

For justjoin it also reports which raw path each current row came from, by
looking the offer up in ``scrapers/justjoin/justjoin_state.json``:
``api`` (detail API record), ``api_listing`` (listing API record merged over
the cached JSON-LD), ``page_offer`` (offer record embedded in the HTML page),
``jsonld`` (schema.org JobPosting only) or ``not_in_state``.

Usage:
    python -m scripts.coverage
    python -m scripts.coverage --db job_listings.duckdb --no-justjoin-source
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

import duckdb

from scrapers.justjoin.justjoin import raw_source
from storage import DB_FILE, TABLE, canonical_url

JUSTJOIN_STATE = os.path.join("scrapers", "justjoin", "justjoin_state.json")


def _pct(part: int, total: int) -> str:
    return f"{(part / total * 100) if total else 0:5.1f}%"


def _columns(conn) -> set[str]:
    return {row[0] for row in conn.execute(f"DESCRIBE {TABLE}").fetchall()}


def board_coverage(conn) -> list[dict]:
    cols = _columns(conn)

    def metric(expr: str, needs: str) -> str:
        return f"count(*) FILTER (WHERE {expr})" if needs in cols else "NULL"

    rows = conn.execute(
        f"""
        SELECT
            job_board,
            count(*),
            count(*) FILTER (WHERE technologies IS NOT NULL AND len(technologies) > 0),
            {metric("technologies_required IS NOT NULL", "technologies_required")},
            {metric("len(technologies_nice) > 0", "technologies_nice")},
            count(*) FILTER (WHERE seniority IS NOT NULL),
            {metric("seniority_levels IS NOT NULL", "seniority_levels")},
            count(*) FILTER (WHERE work_mode IS NOT NULL),
            count(*) FILTER (WHERE description IS NOT NULL AND description <> '')
        FROM {TABLE}
        WHERE is_current
        GROUP BY 1
        ORDER BY 2 DESC
        """
    ).fetchall()
    modes = conn.execute(
        f"""
        SELECT job_board, coalesce(work_mode, 'NULL'), count(*)
        FROM {TABLE}
        WHERE is_current
        GROUP BY 1, 2
        """
    ).fetchall()
    by_board: dict[str, Counter] = {}
    for board, mode, count in modes:
        by_board.setdefault(board, Counter())[mode] = count

    out = []
    for board, total, tech, req, nice, senior, levels, mode, desc in rows:
        out.append(
            {
                "board": board,
                "total": total,
                "technologies": tech,
                "technologies_required": req,
                "technologies_nice_nonempty": nice,
                "seniority": senior,
                "seniority_levels": levels,
                "work_mode": mode,
                "description": desc,
                "work_mode_dist": by_board.get(board, Counter()),
            }
        )
    return out


def justjoin_sources(conn, state_path: str) -> Counter | None:
    if not os.path.exists(state_path):
        return None
    with open(state_path, encoding="utf-8") as file:
        offers = json.load(file).get("offers", {})
    by_url = {canonical_url(url): entry for url, entry in offers.items()}
    urls = conn.execute(
        f"SELECT url FROM {TABLE} WHERE is_current AND job_board = 'justjoin'"
    ).fetchall()
    sources = Counter()
    for (url,) in urls:
        entry = by_url.get(canonical_url(url))
        sources[(raw_source(entry.get("raw")) or "invalid") if entry else "not_in_state"] += 1
    return sources


def main() -> int:
    parser = argparse.ArgumentParser(description="Per-board coverage report")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--state", default=JUSTJOIN_STATE)
    parser.add_argument("--no-justjoin-source", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"database not found: {args.db}")
        return 1

    conn = duckdb.connect(args.db, read_only=True)
    try:
        stats = board_coverage(conn)
        sources = None if args.no_justjoin_source else justjoin_sources(conn, args.state)
    finally:
        conn.close()

    def cell(value, total):
        return "    n/a" if value is None else _pct(value, total).rjust(7)

    header = (
        f"{'board':12} {'rows':>6} {'tech':>7} {'req!=null':>9} {'nice>0':>7} "
        f"{'senior':>7} {'levels':>7} {'w_mode':>7} {'descr':>7}"
    )
    print(f"Current-row coverage ({args.db})")
    print(header)
    print("-" * len(header))
    for s in stats:
        t = s["total"]
        print(
            f"{s['board']:12} {t:>6} {cell(s['technologies'], t)} "
            f"{cell(s['technologies_required'], t):>9} "
            f"{cell(s['technologies_nice_nonempty'], t)} "
            f"{cell(s['seniority'], t)} {cell(s['seniority_levels'], t)} "
            f"{cell(s['work_mode'], t)} "
            f"{cell(s['description'], t)}"
        )

    print("\nwork_mode distribution:")
    for s in stats:
        dist = ", ".join(
            f"{mode} {count}" for mode, count in sorted(s["work_mode_dist"].items())
        )
        print(f"  {s['board']:12} {dist}")

    if sources is not None:
        print(f"\njustjoin current rows by raw source ({args.state}):")
        for name, count in sources.most_common():
            print(f"  {name:14} {count:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
