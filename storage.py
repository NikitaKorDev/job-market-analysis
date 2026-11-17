"""DuckDB SCD2 storage for flat job listings.

Scrapers emit flat dictionaries (see ``scrapers/common.flat_listing``). This
module maps those dictionaries onto ``dim_job_listings`` and keeps a slowly
changing dimension (SCD type 2) history.

Intervals are half-open: ``effective_from <= day < effective_to``. Current rows
have ``effective_to = NULL`` and ``is_current = TRUE``.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import duckdb

from scrapers.common import (
    as_list,
    int_or_none,
    normalize_salary_type,
    normalize_work_mode,
    str_or_none,
    text_list,
)

DB_FILE = "job_listings.duckdb"
TABLE = "dim_job_listings"
STAGE_TABLE = "stage_job_listings"

# Query parameters that only track the click, never identify the offer.
_TRACKING_PARAMS = {
    "_ga",
    "_gl",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "yclid",
}
_TRACKING_PREFIXES = ("utm_", "pk_", "mtm_")

# Fields tracked by hash_diff. ``description`` is deliberately excluded
# (decision 3), as is ``scraped_at``: re-running an unchanged scrape must not
# create a new version.
_HASH_FIELDS = (
    "job_board",
    "url",
    "company",
    "title",
    "city",
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
)

# Columns written to the staging table and then to dim_job_listings.
_ROW_FIELDS = (
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
)

_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    surrogate_key    VARCHAR PRIMARY KEY,
    job_id           VARCHAR NOT NULL,
    job_board        VARCHAR,
    url              VARCHAR,
    company          VARCHAR,
    title            VARCHAR,
    city             VARCHAR,
    description      VARCHAR,
    technologies     VARCHAR[],
    salary_month     INTEGER,
    salary_hour      INTEGER,
    currency         VARCHAR,
    salary_type      VARCHAR,
    employment_types VARCHAR[],
    seniority        VARCHAR,
    work_mode        VARCHAR,
    application_url  VARCHAR,
    benefits         VARCHAR[],
    date_posted      DATE,
    scraped_at       TIMESTAMP,
    hash_diff        VARCHAR,
    effective_from   DATE NOT NULL,
    effective_to     DATE,
    is_current       BOOLEAN NOT NULL
);
"""

_STAGE_SQL = f"""
CREATE OR REPLACE TEMP TABLE {STAGE_TABLE} (
    job_id           VARCHAR,
    job_board        VARCHAR,
    url              VARCHAR,
    company          VARCHAR,
    title            VARCHAR,
    city             VARCHAR,
    description      VARCHAR,
    technologies     VARCHAR[],
    salary_month     INTEGER,
    salary_hour      INTEGER,
    currency         VARCHAR,
    salary_type      VARCHAR,
    employment_types VARCHAR[],
    seniority        VARCHAR,
    work_mode        VARCHAR,
    application_url  VARCHAR,
    benefits         VARCHAR[],
    date_posted      DATE,
    scraped_at       TIMESTAMP,
    hash_diff        VARCHAR,
    effective_from   DATE
);
"""


def _get(listing: dict, *keys: str) -> Any:
    """Return the first non-empty value for any of the given keys."""
    for key in keys:
        value = listing.get(key)
        if value is not None and value != "":
            return value
    return None


def _is_tracking_param(key: str) -> bool:
    lowered = key.lower()
    return lowered in _TRACKING_PARAMS or lowered.startswith(_TRACKING_PREFIXES)


def canonical_url(value: Any) -> str:
    """Normalise a URL so the same offer always produces the same identity.

    Lowercases the scheme/host, removes fragments, default ports, trailing
    slashes and tracking parameters. Meaningful query parameters are kept and
    sorted. Invalid or relative URLs are returned unchanged (stripped).
    """
    text = str_or_none(value)
    if not text:
        return ""

    try:
        parts = urlsplit(text)
        host = parts.hostname
    except ValueError:
        return text

    if not parts.scheme or not host:
        return text

    scheme = parts.scheme.lower()
    host = host.lower()

    try:
        port = parts.port
    except ValueError:
        port = None

    if port and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{host}:{port}"
    else:
        netloc = host

    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")

    query_pairs = [
        (key, val)
        for key, val in parse_qsl(parts.query, keep_blank_values=True)
        if not _is_tracking_param(key)
    ]
    query = urlencode(sorted(query_pairs))

    return urlunsplit((scheme, netloc, path, query, ""))


def stable_job_id(listing: dict) -> str | None:
    """Stable identity: ``board:canonical_url``, else ``board:offer_id``.

    Never falls back to title-only identity.
    """
    board = str_or_none(listing.get("board")) or "unknown"

    url = canonical_url(listing.get("url"))
    if url:
        return f"{board}:{url}"

    offer_id = str_or_none(listing.get("offer_id"))
    if offer_id:
        return f"{board}:{offer_id}"
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str_or_none(value)
    if not text:
        return None

    try:
        return date.fromisoformat(text)
    except ValueError:
        pass

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        text = str_or_none(value)
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalise_row(listing: dict) -> dict | None:
    """Map one flat listing onto the ``dim_job_listings`` columns."""
    job_id = stable_job_id(listing)
    if not job_id:
        return None

    return {
        "job_id": job_id,
        "job_board": str_or_none(listing.get("board")) or "",
        "url": str_or_none(listing.get("url")),
        "company": str_or_none(listing.get("company")),
        "title": str_or_none(listing.get("title")),
        "city": str_or_none(listing.get("city")),
        "description": str_or_none(listing.get("description")),
        "technologies": text_list(
            _get(listing, "technologies_list", "technologies")
        ),
        "salary_month": int_or_none(
            _get(listing, "salary-month", "salary_month")
        ),
        "salary_hour": int_or_none(_get(listing, "salary-hour", "salary_hour")),
        "currency": str_or_none(listing.get("currency")),
        "salary_type": normalize_salary_type(listing.get("salary_type")),
        "employment_types": as_list(listing.get("employment_types")),
        "seniority": str_or_none(listing.get("seniority")),
        "work_mode": normalize_work_mode(listing.get("work_mode")),
        "application_url": str_or_none(listing.get("application_url")),
        "benefits": as_list(listing.get("benefits")),
        "date_posted": _parse_date(_get(listing, "date", "date_posted")),
        "scraped_at": _parse_timestamp(listing.get("scraped_at"))
        or _utc_now_naive(),
    }


def _hash_value(field: str, value: Any) -> str:
    if value is None:
        return "\x00"
    if field in ("url", "application_url"):
        return canonical_url(value) or "\x00"
    if isinstance(value, (list, tuple, set)):
        return "\x1e".join(sorted(str(item) for item in value))
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def hash_diff(row: dict) -> str:
    """Hash all tracked fields, excluding description and scraped_at."""
    payload = "\x1f".join(_hash_value(field, row.get(field)) for field in _HASH_FIELDS)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the Phase 3 table and reject an incompatible pre-existing one."""
    conn.execute(_SCHEMA_SQL)

    existing = {row[0] for row in conn.execute(f"DESCRIBE {TABLE}").fetchall()}
    expected = {"surrogate_key", *_ROW_FIELDS, "effective_to", "is_current"}
    missing = expected - existing
    if missing:
        raise RuntimeError(
            f"{TABLE} has an incompatible schema; missing columns: "
            f"{', '.join(sorted(missing))}. Drop the table and ingest again."
        )


def _dedupe(rows: Iterable[dict]) -> list[dict]:
    """Keep the latest row per job_id so a batch can never create two currents."""
    latest: dict[str, dict] = {}
    floor = datetime.min
    for row in rows:
        existing = latest.get(row["job_id"])
        if existing is None or (row["scraped_at"] or floor) >= (
            existing["scraped_at"] or floor
        ):
            latest[row["job_id"]] = row
    return list(latest.values())


def _count_changes(conn: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    rows = conn.execute(
        f"""
        SELECT s.job_id, s.hash_diff, t.hash_diff
        FROM {STAGE_TABLE} AS s
        LEFT JOIN {TABLE} AS t
          ON s.job_id = t.job_id AND t.is_current
        """
    ).fetchall()

    new = updated = unchanged = 0
    for _, staged_hash, current_hash in rows:
        if current_hash is None:
            new += 1
        elif staged_hash != current_hash:
            updated += 1
        else:
            unchanged += 1
    return new, updated, unchanged


def ingest_listings(
    db_path: str = DB_FILE, listings: Iterable[dict] | None = None
) -> dict[str, int]:
    """Ingest flat listings with SCD2 semantics and return a summary."""
    listings = list(listings or [])

    rows = []
    skipped = 0
    for listing in listings:
        if not isinstance(listing, dict):
            skipped += 1
            continue
        row = normalise_row(listing)
        if row is None:
            skipped += 1
            continue
        row["hash_diff"] = hash_diff(row)
        rows.append(row)

    rows = _dedupe(rows)
    summary = {
        "seen": len(listings),
        "valid": len(rows),
        "skipped": skipped,
        "new": 0,
        "updated": 0,
        "unchanged": 0,
    }
    if not rows:
        return summary

    today = date.today()
    for row in rows:
        row["effective_from"] = today

    conn = duckdb.connect(str(db_path))
    try:
        ensure_schema(conn)
        conn.execute("BEGIN")
        conn.execute(_STAGE_SQL)

        placeholders = ", ".join("?" for _ in _ROW_FIELDS)
        conn.executemany(
            f"INSERT INTO {STAGE_TABLE} VALUES ({placeholders})",
            [tuple(row[field] for field in _ROW_FIELDS) for row in rows],
        )

        new, updated, unchanged = _count_changes(conn)
        summary.update(new=new, updated=updated, unchanged=unchanged)

        # Expire versions whose tracked attributes changed. Intervals are
        # half-open, so an old version replaced today ends at today.
        conn.execute(
            f"""
            UPDATE {TABLE}
            SET effective_to = ?, is_current = FALSE
            FROM {STAGE_TABLE} AS s
            WHERE {TABLE}.job_id = s.job_id
              AND {TABLE}.is_current
              AND {TABLE}.hash_diff <> s.hash_diff
            """,
            [today],
        )

        # Insert brand-new listings and changed versions. Unchanged listings
        # already have a current row with the same hash, so they are skipped.
        columns = ", ".join(_ROW_FIELDS)
        select_columns = ", ".join(f"s.{field}" for field in _ROW_FIELDS)
        conn.execute(
            f"""
            INSERT INTO {TABLE}
                (surrogate_key, {columns}, effective_to, is_current)
            SELECT
                uuid()::VARCHAR,
                {select_columns},
                NULL,
                TRUE
            FROM {STAGE_TABLE} AS s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {TABLE} AS t
                WHERE t.job_id = s.job_id
                  AND t.is_current
                  AND t.hash_diff = s.hash_diff
            )
            """
        )

        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass  # schema failure happens before BEGIN
        raise
    finally:
        conn.close()

    return summary
