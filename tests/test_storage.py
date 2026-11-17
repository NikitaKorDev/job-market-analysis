"""Tests for the DuckDB SCD2 ingestion layer."""

import os
import tempfile
import unittest
from datetime import date, datetime

import duckdb

from scrapers.common import flat_listing
from storage import (
    canonical_url,
    hash_diff,
    ingest_listings,
    normalise_row,
    stable_job_id,
)


def make_listing(**overrides):
    data = {
        "board": "test",
        "url": "https://example.com/job/1",
        "company": "Acme",
        "offer_id": "42",
        "title": "Backend Developer",
        "city": "Kraków",
        "description": "Build things.",
        "technologies": ["Python", "SQL"],
        "salary_month": 10000,
        "salary_hour": 60,
        "currency": "PLN",
        "salary_type": "brutto",
        "employment_types": ["B2B"],
        "seniority": "mid",
        "work_mode": "remote",
        "application_url": "https://example.com/apply",
        "benefits": ["Lunch"],
        "date": "2026-11-01",
    }
    data.update(overrides)
    return flat_listing(**data)


class CanonicalUrlTests(unittest.TestCase):
    def test_strips_tracking_fragment_default_port_and_trailing_slash(self):
        self.assertEqual(
            canonical_url(
                " HTTPS://Example.COM:443/path/?utm_source=x&b=2&a=1#frag "
            ),
            "https://example.com/path?a=1&b=2",
        )

    def test_keeps_root_path(self):
        self.assertEqual(canonical_url("https://example.com/"), "https://example.com/")

    def test_invalid_or_relative_url_is_returned_stripped(self):
        self.assertEqual(canonical_url("  not a url  "), "not a url")
        self.assertEqual(canonical_url(None), "")


class StableJobIdTests(unittest.TestCase):
    def test_prefers_canonical_url(self):
        listing = make_listing(
            board="justjoin",
            url="https://justjoin.it/job-offer/dev?utm_medium=email",
            offer_id="ignored",
        )
        self.assertEqual(
            stable_job_id(listing),
            "justjoin:https://justjoin.it/job-offer/dev",
        )

    def test_falls_back_to_offer_id(self):
        listing = make_listing(board="olx", url="", offer_id="123")
        self.assertEqual(stable_job_id(listing), "olx:123")

    def test_never_uses_title_alone(self):
        listing = make_listing(board="olx", url="", offer_id="")
        self.assertIsNone(stable_job_id(listing))


class HashDiffTests(unittest.TestCase):
    def test_description_and_scraped_at_are_excluded(self):
        first = normalise_row(make_listing(description="First text"))
        second = normalise_row(
            make_listing(
                description="Completely different text",
                scraped_at="2027-01-01T00:00:00+00:00",
            )
        )
        self.assertEqual(hash_diff(first), hash_diff(second))

    def test_array_order_does_not_change_hash(self):
        first = normalise_row(make_listing(technologies=["Python", "SQL"]))
        second = normalise_row(make_listing(technologies=["SQL", "Python"]))
        self.assertEqual(hash_diff(first), hash_diff(second))

    def test_tracked_change_changes_hash(self):
        first = normalise_row(make_listing())
        second = normalise_row(make_listing(title="Senior Backend Developer"))
        self.assertNotEqual(hash_diff(first), hash_diff(second))

    def test_tracking_query_params_do_not_change_hash(self):
        first = normalise_row(make_listing(url="https://example.com/job/1"))
        second = normalise_row(
            make_listing(url="https://example.com/job/1?utm_source=newsletter")
        )
        self.assertEqual(hash_diff(first), hash_diff(second))


class IngestTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = os.path.join(self._tmp.name, "jobs.duckdb")

    def query(self, sql, params=None):
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            return conn.execute(sql, params or []).fetchall()
        finally:
            conn.close()

    def test_inserts_all_fields(self):
        summary = ingest_listings(self.db_path, [make_listing()])

        self.assertEqual(summary["new"], 1)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(summary["unchanged"], 0)

        row = self.query(
            """
            SELECT job_board, url, company, title, city, description,
                   technologies, salary_month, salary_hour, currency,
                   salary_type, employment_types, seniority, work_mode,
                   application_url, benefits, date_posted, scraped_at,
                   effective_to, is_current
            FROM dim_job_listings
            """
        )[0]

        self.assertEqual(row[0], "test")
        self.assertEqual(row[1], "https://example.com/job/1")
        self.assertEqual(row[2], "Acme")
        self.assertEqual(row[3], "Backend Developer")
        self.assertEqual(row[4], "Kraków")
        self.assertEqual(row[5], "Build things.")
        self.assertEqual(row[6], ["Python", "SQL"])
        self.assertEqual(row[7], 10000)
        self.assertEqual(row[8], 60)
        self.assertEqual(row[9], "PLN")
        self.assertEqual(row[10], "brutto")
        self.assertEqual(row[11], ["B2B"])
        self.assertEqual(row[12], "mid")
        self.assertEqual(row[13], "remote")
        self.assertEqual(row[14], "https://example.com/apply")
        self.assertEqual(row[15], ["Lunch"])
        self.assertEqual(row[16], date(2026, 11, 1))
        self.assertIsInstance(row[17], datetime)
        self.assertIsNone(row[18])
        self.assertTrue(row[19])

    def test_undisclosed_salary_is_null(self):
        ingest_listings(
            self.db_path,
            [make_listing(salary_month=0, salary_hour="", currency="")],
        )
        row = self.query(
            "SELECT salary_month, salary_hour, currency FROM dim_job_listings"
        )[0]
        self.assertEqual(row, (None, None, None))

    def test_rerun_unchanged_adds_no_version(self):
        listing = make_listing()
        ingest_listings(self.db_path, [listing])

        summary = ingest_listings(self.db_path, [listing])

        self.assertEqual(summary["new"], 0)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(summary["unchanged"], 1)
        self.assertEqual(
            self.query("SELECT count(*) FROM dim_job_listings")[0][0], 1
        )

    def test_description_only_change_adds_no_version(self):
        ingest_listings(self.db_path, [make_listing(description="Old text")])

        summary = ingest_listings(
            self.db_path, [make_listing(description="New text")]
        )

        self.assertEqual(summary["updated"], 0)
        self.assertEqual(summary["unchanged"], 1)
        self.assertEqual(
            self.query("SELECT count(*) FROM dim_job_listings")[0][0], 1
        )

    def test_changed_title_expires_old_version(self):
        ingest_listings(self.db_path, [make_listing()])

        summary = ingest_listings(
            self.db_path, [make_listing(title="Senior Backend Developer")]
        )

        self.assertEqual(summary["new"], 0)
        self.assertEqual(summary["updated"], 1)

        rows = self.query(
            """
            SELECT title, effective_from, effective_to, is_current
            FROM dim_job_listings
            ORDER BY effective_from, is_current
            """
        )
        self.assertEqual(len(rows), 2)

        old = next(row for row in rows if not row[3])
        current = next(row for row in rows if row[3])
        today = date.today()

        self.assertEqual(old[0], "Backend Developer")
        self.assertEqual(old[1], today)
        self.assertEqual(old[2], today)
        self.assertEqual(current[0], "Senior Backend Developer")
        self.assertEqual(current[1], today)
        self.assertIsNone(current[2])

    def test_same_day_changes_get_unique_surrogate_keys(self):
        ingest_listings(self.db_path, [make_listing()])
        ingest_listings(self.db_path, [make_listing(title="Version 2")])
        ingest_listings(self.db_path, [make_listing(title="Version 3")])

        total, distinct_keys, current = self.query(
            """
            SELECT count(*), count(DISTINCT surrogate_key),
                   count(*) FILTER (WHERE is_current)
            FROM dim_job_listings
            """
        )[0]

        self.assertEqual(total, 3)
        self.assertEqual(distinct_keys, 3)
        self.assertEqual(current, 1)

    def test_duplicate_job_id_in_one_batch_keeps_latest(self):
        older = make_listing(
            title="Older", scraped_at="2026-11-01T00:00:00+00:00"
        )
        newer = make_listing(
            title="Newer", scraped_at="2026-11-02T00:00:00+00:00"
        )

        summary = ingest_listings(self.db_path, [older, newer])

        self.assertEqual(summary["valid"], 1)
        row = self.query("SELECT title FROM dim_job_listings")[0]
        self.assertEqual(row[0], "Newer")

    def test_only_current_versions_are_returned(self):
        listing = make_listing()
        ingest_listings(self.db_path, [listing])
        ingest_listings(self.db_path, [make_listing(title="Changed")])

        rows = self.query(
            "SELECT job_id FROM dim_job_listings WHERE is_current"
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], stable_job_id(listing))

    def test_unknown_work_mode_is_null(self):
        ingest_listings(self.db_path, [make_listing(work_mode="warszawa")])
        row = self.query("SELECT work_mode FROM dim_job_listings")[0]
        self.assertIsNone(row[0])

    def test_missing_url_and_offer_id_is_skipped(self):
        listing = make_listing(url="", offer_id="")
        summary = ingest_listings(self.db_path, [listing])
        self.assertEqual(summary["valid"], 0)
        self.assertEqual(summary["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
