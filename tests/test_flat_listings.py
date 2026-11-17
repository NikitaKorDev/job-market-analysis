"""Tests for flat scraper listings (no envelope, no custom schema validator)."""

import unittest

from scrapers.bulldogjobs.bulldogjobs import _string_list, parse_salary
from scrapers.common import flat_listing
from scrapers.justjoin.justjoin import raw_to_flat
from scrapers.pracuj.pracuj import build_listings
from scrapers.scraper_hub import _as_listings

CORE_FIELDS = {
    "board",
    "url",
    "company",
    "offer_id",
    "title",
    "city",
    "description",
    "technologies",
    "technologies_list",
    "salary-month",
    "salary-hour",
    "currency",
    "salary_type",
    "employment_types",
    "seniority",
    "work_mode",
    "application_url",
    "benefits",
    "date",
    "scraped_at",
}


class FlatListingTests(unittest.TestCase):
    def test_flat_listing_has_all_core_fields(self):
        listing = flat_listing(board="test", url="https://example.com/1", title="Dev")
        self.assertEqual(set(listing), CORE_FIELDS)

    def test_undisclosed_salary_is_none(self):
        listing = flat_listing(
            board="test",
            url="https://example.com/1",
            title="Dev",
            salary_month=0,
            salary_hour="",
            currency="",
        )
        self.assertIsNone(listing["salary-month"])
        self.assertIsNone(listing["salary-hour"])
        self.assertIsNone(listing["currency"])

    def test_negative_salary_is_none(self):
        listing = flat_listing(
            board="test", url="https://example.com/1", title="Dev", salary_month=-500
        )
        self.assertIsNone(listing["salary-month"])

    def test_normalization(self):
        listing = flat_listing(
            board="test",
            url="https://example.com/1",
            title="Dev",
            technologies=["Python", "SQL", "Python"],
            salary_type="netto (+ VAT)",
            work_mode="praca zdalna",
            employment_types=["B2B"],
            benefits=["Lunch"],
        )
        self.assertEqual(listing["technologies"], "Python, SQL")
        self.assertEqual(listing["technologies_list"], ["Python", "SQL"])
        self.assertEqual(listing["salary_type"], "netto")
        self.assertEqual(listing["work_mode"], "remote")
        self.assertEqual(listing["employment_types"], ["B2B"])
        self.assertEqual(listing["benefits"], ["Lunch"])

    def test_b2b_salary_type(self):
        listing = flat_listing(
            board="test", url="https://example.com/1", title="Dev", salary_type="B2B"
        )
        self.assertEqual(listing["salary_type"], "netto")

    def test_technologies_split_and_dedupe(self):
        listing = flat_listing(
            board="test",
            url="https://example.com/1",
            title="Dev",
            technologies=["Python", "SQL, Docker", "Python"],
        )
        self.assertEqual(listing["technologies_list"], ["Python", "SQL", "Docker"])

    def test_scraped_at_present(self):
        listing = flat_listing(board="test", url="https://example.com/1", title="Dev")
        self.assertTrue(listing["scraped_at"])


class HubSanityGuardTests(unittest.TestCase):
    def test_drops_broken_scrapes(self):
        result = _as_listings(
            "board",
            [
                {"url": "https://example.com/1", "title": "A"},
                {"url": "", "title": "B"},
                {"url": "https://example.com/3", "title": ""},
                "not a dict",
                None,
            ],
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["url"], "https://example.com/1")
        self.assertEqual(result[0]["board"], "board")
        self.assertIn("scraped_at", result[0])

    def test_keeps_existing_board(self):
        result = _as_listings(
            "hub-name",
            [{"board": "scraper-name", "url": "https://example.com/1", "title": "A"}],
        )
        self.assertEqual(result[0]["board"], "scraper-name")


class JustJoinFlattenTests(unittest.TestCase):
    def test_api_raw_becomes_flat_listing(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/example",
            "date": "2026-11-13",
            "api": {
                "title": "Backend Developer",
                "companyName": "Acme",
                "body": "<p>Build things.</p>",
                "applyUrl": "https://acme.example/apply",
                "workplaceType": "hybrid",
                "experienceLevel": "senior",
                "publishedAt": "2026-11-13T21:00:13Z",
                "requiredSkills": [{"name": "Python"}, {"name": "SQL"}],
                "niceToHaveSkills": [{"name": "Docker"}],
                "employmentTypes": [
                    {
                        "fromPerUnit": 10000,
                        "toPerUnit": 14000,
                        "currency": "PLN",
                        "currencySource": "original",
                        "type": "permanent",
                        "unit": "Month",
                        "gross": True,
                    },
                    {
                        "fromPerUnit": 9000,
                        "toPerUnit": 12000,
                        "currency": "PLN",
                        "currencySource": "conversion",
                        "type": "b2b",
                        "unit": "Month",
                        "gross": False,
                    },
                ],
                "multilocation": [{"city": "Warszawa"}],
            },
        }
        listing = raw_to_flat(raw)

        self.assertEqual(set(listing), CORE_FIELDS)
        self.assertEqual(listing["board"], "justjoin")
        self.assertEqual(listing["url"], "https://justjoin.it/job-offer/example")
        self.assertEqual(listing["company"], "Acme")
        self.assertEqual(listing["title"], "Backend Developer")
        self.assertEqual(listing["city"], "Warszawa")
        self.assertEqual(listing["description"], "Build things.")
        self.assertEqual(listing["technologies_list"], ["Python", "SQL", "Docker"])
        self.assertEqual(listing["salary-month"], 12000)
        self.assertEqual(listing["salary-hour"], 71)
        self.assertEqual(listing["currency"], "PLN")
        self.assertEqual(listing["salary_type"], "brutto")
        self.assertEqual(listing["employment_types"], ["Umowa o pracę", "B2B"])
        self.assertEqual(listing["seniority"], "senior")
        self.assertEqual(listing["work_mode"], "hybrid")
        self.assertEqual(listing["application_url"], "https://acme.example/apply")
        self.assertEqual(listing["date"], "2026-11-13")

    def test_html_raw_becomes_flat_listing(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/html-example",
            "date": "2026-09-29",
            "skills": ["Python", "AWS"],
            "jobposting": {
                "@type": "JobPosting",
                "title": "Data Engineer",
                "description": "&lt;p&gt;Data pipelines.&lt;/p&gt;",
                "datePosted": "2026-09-29T12:45:12Z",
                "employmentType": "FULL_TIME",
                "jobLocationType": "TELECOMMUTE",
                "baseSalary": {
                    "currency": "PLN",
                    "value": {
                        "unitText": "MONTH",
                        "minValue": 10000,
                        "maxValue": 14000,
                    },
                },
                "hiringOrganization": {"name": "N-iX"},
                "jobLocation": [{"address": {"addressLocality": "Kraków"}}],
            },
        }
        listing = raw_to_flat(raw)

        self.assertEqual(set(listing), CORE_FIELDS)
        self.assertEqual(listing["company"], "N-iX")
        self.assertEqual(listing["city"], "Kraków")
        self.assertEqual(listing["description"], "Data pipelines.")
        self.assertEqual(listing["technologies_list"], ["Python", "AWS"])
        self.assertEqual(listing["salary-month"], 12000)
        self.assertEqual(listing["salary-hour"], 71)
        self.assertEqual(listing["employment_types"], ["Pełny etat"])
        self.assertEqual(listing["work_mode"], "remote")
        self.assertIsNone(listing["salary_type"])

    def test_undisclosed_api_salary_is_none(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/no-salary",
            "api": {
                "title": "Intern",
                "companyName": "Acme",
                "body": "<p>Learn.</p>",
                "employmentTypes": [],
            },
        }
        listing = raw_to_flat(raw)
        self.assertIsNone(listing["salary-month"])
        self.assertIsNone(listing["salary-hour"])
        self.assertIsNone(listing["currency"])


class BulldogFlattenTests(unittest.TestCase):
    def test_string_list_splits_and_dedupes(self):
        self.assertEqual(
            _string_list(["Python", "SQL, Docker", "Python"]),
            ["Python", "SQL", "Docker"],
        )

    def test_monthly_gross_salary(self):
        month, hour, currency, salary_type = parse_salary("15 000 - 20 000 PLN brutto")
        self.assertEqual((month, hour, currency, salary_type), (17500, 104, "PLN", "brutto"))

    def test_hourly_b2b_salary(self):
        month, hour, currency, salary_type = parse_salary("100 - 120 PLN/h B2B")
        self.assertEqual((month, hour, currency, salary_type), (18480, 110, "PLN", "netto"))


class PracujFlattenTests(unittest.TestCase):
    def test_build_listings_emits_flat_dicts(self):
        parsed = [
            (
                {
                    "url": "https://www.pracuj.pl/praca/dev,oferta,1",
                    "company": "Acme",
                    "offer_id": "1",
                },
                {
                    "title": "Developer",
                    "city": "Kraków",
                    "description": "Build things.",
                    "technologies": "Python, SQL",
                    "salary-month": 10000,
                    "salary-hour": 60,
                    "currency": "PLN",
                    "salary_type": "brutto",
                    "date": "2026-11-01",
                },
            )
        ]
        listing = build_listings(parsed)[0]

        self.assertEqual(set(listing), CORE_FIELDS)
        self.assertEqual(listing["board"], "pracuj")
        self.assertEqual(listing["title"], "Developer")
        self.assertEqual(listing["company"], "Acme")
        self.assertEqual(listing["technologies_list"], ["Python", "SQL"])
        self.assertEqual(listing["salary-month"], 10000)
        self.assertEqual(listing["salary_type"], "brutto")


if __name__ == "__main__":
    unittest.main()
