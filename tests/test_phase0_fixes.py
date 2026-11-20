"""Phase 0 fixes: typed technologies, seniority levels and work mode per board.

All raw records below are trimmed copies of real cached / live records
(justjoin_state.json, nofluff /api/posting, bulldogjob and pracuj pages,
OLX API items) reduced to the fields the code reads. No network access.
"""

import json
import os
import tempfile
import unittest

import duckdb

from scrapers.bulldogjobs.bulldogjobs import parse_detail_html, work_mode_from_job
from scrapers.common import (
    flat_listing,
    normalize_seniority_levels,
    normalize_work_mode,
)
from scrapers.justjoin.justjoin import (
    attach_listing_record,
    extract_page_offer,
    parse_offer,
    raw_source,
    raw_to_flat,
)
from scrapers.nofluffjobs.nofluffjobs import _to_listing
from scrapers.olx.olx import extract_work_mode
from scrapers.pracuj.pracuj import _detail_from_html
from storage import TABLE, hash_diff, ingest_listings, normalise_row


# --------------------------------------------------------------------------- #
# common
# --------------------------------------------------------------------------- #
class WorkModeTests(unittest.TestCase):
    def test_hybrid_with_remote_possible_is_hybrid(self):
        self.assertEqual(normalize_work_mode("hybrydowa, możliwa praca zdalna"), "hybrid")
        self.assertEqual(normalize_work_mode("Praca hybrydowa"), "hybrid")

    def test_remote_plus_office_is_hybrid(self):
        self.assertEqual(normalize_work_mode("W siedzibie firmy, Praca zdalna"), "hybrid")
        self.assertEqual(normalize_work_mode(["praca stacjonarna", "praca zdalna"]), "hybrid")
        self.assertEqual(normalize_work_mode("remote possible"), "hybrid")

    def test_single_modes(self):
        self.assertEqual(normalize_work_mode("praca zdalna"), "remote")
        self.assertEqual(normalize_work_mode("home office"), "remote")
        self.assertEqual(normalize_work_mode("Praca stacjonarna"), "office")
        self.assertEqual(normalize_work_mode("on-site"), "office")
        self.assertEqual(normalize_work_mode("remote"), "remote")
        self.assertIsNone(normalize_work_mode("praca mobilna"))
        self.assertIsNone(normalize_work_mode(""))


class SeniorityLevelTests(unittest.TestCase):
    def test_board_values(self):
        cases = {
            "Senior": ["senior"],
            "senior": ["senior"],
            "Mid, Senior": ["mid", "senior"],
            "Trainee": ["intern"],
            "praktykant / stażysta": ["intern"],
            "regular": ["mid"],
            "medium": ["mid"],
            "c_level": ["manager"],
            "Head": ["manager"],
            "Expert": ["expert"],
            "lead": ["lead"],
            "specjalista (Mid / Regular)": ["mid"],
            "młodszy specjalista / młodsza specjalistka (junior)": ["junior"],
            "starsza specjalistka": ["senior"],
            "kierownik / kierowniczka - koordynator / koordynatorka": ["manager"],
        }
        for text, expected in cases.items():
            self.assertEqual(normalize_seniority_levels(text), expected, text)

    def test_ordered_and_deduplicated(self):
        self.assertEqual(
            normalize_seniority_levels(["Senior", "Junior", "senior", "Lead"]),
            ["junior", "senior", "lead"],
        )

    def test_unknown_is_none(self):
        self.assertIsNone(normalize_seniority_levels(None))
        self.assertIsNone(normalize_seniority_levels(""))
        self.assertIsNone(normalize_seniority_levels("Wymagane doświadczenie"))


class FlatListingTypedTechTests(unittest.TestCase):
    def test_union_and_typed_lists(self):
        listing = flat_listing(
            board="t",
            url="https://e.com/1",
            title="Dev",
            technologies=["Python"],
            technologies_required=["Python", "SQL"],
            technologies_nice=["Docker"],
            seniority="Senior",
            work_mode_raw="praca hybrydowa",
        )
        self.assertEqual(listing["technologies_list"], ["Python", "SQL", "Docker"])
        self.assertEqual(listing["technologies_required"], ["Python", "SQL"])
        self.assertEqual(listing["technologies_nice"], ["Docker"])
        self.assertEqual(listing["seniority"], "Senior")
        self.assertEqual(listing["seniority_levels"], ["senior"])
        self.assertEqual(listing["work_mode_raw"], "praca hybrydowa")

    def test_untyped_board_keeps_null(self):
        listing = flat_listing(board="t", url="https://e.com/1", title="Dev", technologies="Go")
        self.assertIsNone(listing["technologies_required"])
        self.assertIsNone(listing["technologies_nice"])
        self.assertIsNone(listing["seniority_levels"])

    def test_typed_board_without_items_is_empty_list(self):
        listing = flat_listing(
            board="t", url="https://e.com/1", title="Dev",
            technologies_required=[], technologies_nice=[],
        )
        self.assertEqual(listing["technologies_required"], [])
        self.assertEqual(listing["technologies_nice"], [])


# --------------------------------------------------------------------------- #
# justjoin
# --------------------------------------------------------------------------- #
# Real cached HTML-era entry (N-iX, fetched 2026-09-30), trimmed.
JJ_JSONLD = {
    "@type": "JobPosting",
    "datePosted": "2026-09-29T12:45:12.677514Z",
    "description": "&lt;p&gt;We are looking for Senior DataOps Engineer.&lt;/p&gt;",
    "title": "Senior DataOps Engineer",
    "employmentType": "FULL_TIME",
    "jobLocationType": "TELECOMMUTE",
    "baseSalary": {
        "currency": "PLN",
        "value": {"unitText": "MONTH", "minValue": 17315.1, "maxValue": 21547.68},
    },
    "hiringOrganization": {"name": "N-iX"},
    "jobLocation": {"address": {"addressLocality": "Kraków"}},
}

# Shape of a candidate-api listing record (the same keys the live listing returns).
JJ_LISTING_RECORD = {
    "slug": "n-ix-senior-dataops-engineer-krakow-data",
    "title": "Senior DataOps Engineer",
    "workplaceType": "remote",
    "experienceLevel": "senior",
    "city": "Kraków",
    "companyName": "N-iX",
    "requiredSkills": [
        {"name": "AWS", "level": 4},
        {"name": "Apache Spark", "level": 4},
        {"name": "Apache Iceberg", "level": 3},
    ],
    "niceToHaveSkills": [{"name": "Kubernetes", "level": 2}],
    "employmentTypes": [
        {"from": 17315.1, "to": 21547.68, "currency": "PLN", "currencySource": "original",
         "type": "b2b", "unit": "Month", "gross": False},
    ],
    "multilocation": [{"city": "Kraków"}],
}


def _rsc_page(records, slug, jsonld):
    """A minimal justjoin offer page: JSON-LD + one RSC flight chunk."""
    flight = "5:" + json.dumps(["$", "div", None, {"lookalikes": records}], separators=(",", ":")) + "\n"
    return (
        "<html><head>"
        f'<script type="application/ld+json">{json.dumps(jsonld)}</script>'
        "</head><body>"
        f"<script>self.__next_f.push({json.dumps([1, flight])})</script>"
        "</body></html>"
    )


class JustJoinTests(unittest.TestCase):
    def test_api_record_keeps_required_and_nice(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/rublon-full-stack-developer-java",
            "api": {
                "title": "Full-Stack Developer (Java EE, Quarkus, Angular)",
                "companyName": "Rublon",
                "body": "<p>Join Rublon.</p>",
                "workplaceType": "hybrid",
                "experienceLevel": "c_level",
                "requiredSkills": [{"name": "Java EE"}, {"name": "Quarkus"}],
                "niceToHaveSkills": [{"name": "Docker"}],
                "employmentTypes": [],
            },
        }
        listing = raw_to_flat(raw)
        self.assertEqual(listing["technologies_required"], ["Java EE", "Quarkus"])
        self.assertEqual(listing["technologies_nice"], ["Docker"])
        self.assertEqual(listing["technologies_list"], ["Java EE", "Quarkus", "Docker"])
        self.assertEqual(listing["seniority"], "c_level")
        self.assertEqual(listing["seniority_levels"], ["manager"])
        self.assertEqual(listing["work_mode"], "hybrid")
        self.assertEqual(listing["work_mode_raw"], "hybrid")

    def test_api_record_without_nice_has_empty_list(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/x",
            "api": {"title": "Dev", "body": "<p>x</p>", "requiredSkills": [{"name": "Go"}]},
        }
        listing = raw_to_flat(raw)
        self.assertEqual(listing["technologies_required"], ["Go"])
        self.assertEqual(listing["technologies_nice"], [])

    def test_jsonld_only_entry(self):
        raw = {
            "offer_url": "https://justjoin.it/job-offer/n-ix-senior-dataops-engineer-krakow-data",
            "published_raw": "",
            "jobposting": JJ_JSONLD,
        }
        self.assertEqual(raw_source(raw), "jsonld")
        listing = raw_to_flat(raw)
        self.assertEqual(listing["company"], "N-iX")
        self.assertEqual(listing["work_mode"], "remote")
        self.assertEqual(listing["work_mode_raw"], "TELECOMMUTE")
        self.assertIsNone(listing["technologies_required"])
        self.assertIsNone(listing["seniority_levels"])

    def test_cached_jsonld_entry_is_enriched_from_listing_record(self):
        entry = {
            "raw": {
                "offer_url": "https://justjoin.it/job-offer/n-ix-senior-dataops-engineer-krakow-data",
                "jobposting": dict(JJ_JSONLD, jobLocationType=None),
            },
            "v": 2,
        }
        self.assertTrue(attach_listing_record(entry, JJ_LISTING_RECORD))
        self.assertEqual(raw_source(entry["raw"]), "api_listing")
        listing = raw_to_flat(entry["raw"])
        self.assertEqual(listing["technologies_required"], ["AWS", "Apache Spark", "Apache Iceberg"])
        self.assertEqual(listing["technologies_nice"], ["Kubernetes"])
        self.assertEqual(listing["seniority"], "senior")
        self.assertEqual(listing["seniority_levels"], ["senior"])
        self.assertEqual(listing["work_mode"], "remote")
        # description, company and salary still come from the JSON-LD
        self.assertEqual(listing["description"], "We are looking for Senior DataOps Engineer.")
        self.assertEqual(listing["company"], "N-iX")
        self.assertEqual(listing["salary-month"], 19431)
        self.assertEqual(listing["employment_types"], ["B2B"])

    def test_api_entries_are_not_touched(self):
        entry = {"raw": {"offer_url": "u", "api": {"title": "x"}}}
        self.assertFalse(attach_listing_record(entry, JJ_LISTING_RECORD))
        self.assertNotIn("api_listing", entry["raw"])
        self.assertFalse(attach_listing_record({"raw": {"jobposting": {}}}, None))

    def test_page_offer_is_read_from_rsc_payload(self):
        slug = "rublon-full-stack-developer-java-ee-quarkus-angular--zielona-gora-java"
        lookalike = {
            "slug": "adform-software-engineer-big-data-warszawa-java",
            "experienceLevel": "mid",
            "workplaceType": "office",
            "requiredSkills": [{"name": "Scala"}],
            "niceToHaveSkills": [],
        }
        offer = {
            "guid": "3c1006dd",
            "slug": slug,
            "title": "Full-Stack Developer (Java EE, Quarkus, Angular)",
            "experienceLevel": {"label": "mid", "value": "mid"},
            "body": "$b4",
            "workplaceType": {"label": "hybrid", "value": "hybrid"},
            "requiredSkills": [{"id": "Java EE", "name": "Java EE", "level": 3}],
            "niceToHaveSkills": [{"id": "Docker", "name": "Docker", "level": 2}],
            "multilocation": [{"city": "Zielona Góra"}, {"city": "Kraków"}],
        }
        page = _rsc_page([lookalike, offer], slug, dict(JJ_JSONLD, jobLocationType=None))

        found = extract_page_offer(page, slug)
        self.assertEqual(found["slug"], slug)
        self.assertNotIn("body", found)

        raw = parse_offer("https://justjoin.it/job-offer/" + slug, page)
        self.assertEqual(raw_source(raw), "page_offer")
        listing = raw_to_flat(raw)
        self.assertEqual(listing["technologies_required"], ["Java EE"])
        self.assertEqual(listing["technologies_nice"], ["Docker"])
        self.assertEqual(listing["seniority_levels"], ["mid"])
        self.assertEqual(listing["work_mode"], "hybrid")
        self.assertEqual(listing["city"], "Zielona Góra, Kraków")

    def test_page_without_payload(self):
        self.assertIsNone(extract_page_offer("<html></html>"))


# --------------------------------------------------------------------------- #
# nofluff
# --------------------------------------------------------------------------- #
def _nofluff_detail(remote_days, places, seniority=("Senior",)):
    return {
        "basics": {"category": "backend", "seniority": list(seniority)},
        "location": {
            "places": [{"city": c} for c in places],
            "remote": remote_days,
            "remoteFlexible": False,
            "fieldwork": False,
            "hybridDesc": "",
        },
        "requirements": {
            "musts": [
                {"value": "Salesforce", "type": "main"},
                {"value": "Apex", "type": "main"},
                {"value": "Communication skills", "type": "other"},
            ],
            "nices": [{"value": "Git", "type": "main"}, {"value": "SAP", "type": "main"}],
        },
    }


POSTING = {
    "id": "regular-senior-salesforce-developer-agentforce-emea-spyrosoft-remote",
    "url": "regular-senior-salesforce-developer-agentforce-emea-spyrosoft-remote",
    "title": "Regular/Senior Salesforce Developer",
    "technology": "Salesforce",
}


class NoFluffTests(unittest.TestCase):
    def test_nices_are_kept_separately(self):
        listing = _to_listing(POSTING, _nofluff_detail(5, ["Remote", "Wrocław"]))
        self.assertEqual(
            listing["technologies_required"], ["Salesforce", "Apex", "Communication skills"]
        )
        self.assertEqual(listing["technologies_nice"], ["Git", "SAP"])
        self.assertEqual(
            listing["technologies_list"],
            ["Salesforce", "Apex", "Communication skills", "Git", "SAP"],
        )

    def test_remote_days_decide_work_mode(self):
        fully = _to_listing(POSTING, _nofluff_detail(5, ["Remote", "Warszawa"]))
        self.assertEqual(fully["work_mode"], "remote")
        self.assertIn("remote days/week: 5", fully["work_mode_raw"])
        hybrid = _to_listing(POSTING, _nofluff_detail(2, ["Kraków"]))
        self.assertEqual(hybrid["work_mode"], "hybrid")
        office = _to_listing(POSTING, _nofluff_detail(0, ["Kraków"]))
        self.assertEqual(office["work_mode"], "office")

    def test_several_levels(self):
        listing = _to_listing(POSTING, _nofluff_detail(5, ["Remote"], ("Mid", "Senior")))
        self.assertEqual(listing["seniority"], "Mid, Senior")
        self.assertEqual(listing["seniority_levels"], ["mid", "senior"])
        trainee = _to_listing(POSTING, _nofluff_detail(5, ["Remote"], ("Trainee",)))
        self.assertEqual(trainee["seniority_levels"], ["intern"])

    def test_without_detail(self):
        posting = dict(POSTING, location={"places": [{"city": "Warszawa"}], "fullyRemote": False})
        listing = _to_listing(posting, None)
        self.assertIsNone(listing["technologies_required"])
        self.assertIsNone(listing["technologies_nice"])
        self.assertIsNone(listing["work_mode"])
        self.assertEqual(listing["technologies_list"], ["Salesforce"])
        remote = _to_listing(dict(POSTING, location={"fullyRemote": True, "places": []}), None)
        self.assertEqual(remote["work_mode"], "remote")


# --------------------------------------------------------------------------- #
# bulldogjobs
# --------------------------------------------------------------------------- #
def _bulldog_page(job, posting=None):
    posting = posting or {
        "@context": "http://schema.org",
        "@type": "JobPosting",
        "datePosted": "2026-11-02",
        "title": job.get("position", "Engineer"),
        "employmentType": "FULL_TIME",
        "experienceRequirements": {"monthsOfExperience": 60},
        "skills": ", ".join(job.get("technologyTags", [])),
        "hiringOrganization": {"name": "SimCorp"},
        "description": "<p>Run SRE.</p>",
    }
    next_data = {"props": {"pageProps": {"data": {"job": job}}}}
    return (
        '<html><head><meta name="description" content="meta text">'
        f'<script type="application/ld+json">{json.dumps(posting)}</script>'
        f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(next_data)}</script>'
        "</head></html>"
    )


class BulldogTests(unittest.TestCase):
    def test_on_site_and_hybrid(self):
        job = {
            "id": "260469-lead-site-reliability-engineer-warsaw-simcorp",
            "experienceLevel": "lead",
            "remote": False,
            "workModes": ["on-site", "hybrid"],
            "environment": {"remotePossible": 60},
            "technologyTags": ["Terraform", "Kubernetes"],
        }
        detail = parse_detail_html(_bulldog_page(job))
        self.assertEqual(detail["work_mode"], "hybrid")
        self.assertIn("workModes: on-site, hybrid", detail["work_mode_raw"])
        self.assertEqual(detail["seniority"], "lead")
        self.assertEqual(detail["seniority_levels"], ["lead"])
        self.assertEqual(detail["skills"], ["Terraform", "Kubernetes"])
        self.assertEqual(detail["company"], "SimCorp")
        self.assertEqual(detail["description"], "Run SRE.")

    def test_full_remote_and_medium(self):
        job = {"experienceLevel": "medium", "remote": True, "workModes": ["full-remote"]}
        detail = parse_detail_html(_bulldog_page(job))
        self.assertEqual(detail["work_mode"], "remote")
        self.assertEqual(detail["seniority"], "medium")
        self.assertEqual(detail["seniority_levels"], ["mid"])

    def test_mode_rules(self):
        self.assertEqual(work_mode_from_job({"workModes": ["on-site"]})[0], "office")
        self.assertEqual(
            work_mode_from_job({"workModes": ["on-site", "hybrid", "full-remote"], "remote": True})[0],
            "hybrid",
        )
        self.assertEqual(work_mode_from_job({"workModes": ["on-site", "full-remote"]})[0], "hybrid")
        # Accenture-style: no workModes, remote False, remotePossible 100 -> a mix, not remote
        self.assertEqual(
            work_mode_from_job({"workModes": [], "remote": False,
                                "environment": {"remotePossible": 100}})[0],
            "hybrid",
        )
        self.assertEqual(
            work_mode_from_job({"remote": False, "environment": {"remotePossible": 0}})[0],
            "office",
        )
        self.assertEqual(work_mode_from_job({}, {"jobLocationType": "TELECOMMUTE"})[0], "remote")
        self.assertEqual(work_mode_from_job({}), (None, None))

    def test_without_next_data_falls_back_to_months(self):
        detail = parse_detail_html(
            '<script type="application/ld+json">'
            + json.dumps({"@type": "JobPosting", "experienceRequirements": {"monthsOfExperience": 30}})
            + "</script>"
        )
        self.assertEqual(detail["seniority"], "mid")
        self.assertIsNone(detail["work_mode"])


# --------------------------------------------------------------------------- #
# olx
# --------------------------------------------------------------------------- #
def _olx_item(workplace_keys, label, remote_recruitment="1"):
    return {
        "params": [
            {"key": "workplace", "value": {"key": workplace_keys, "label": label}},
            {"key": "remote_recruitment",
             "value": {"key": remote_recruitment, "label": "Rekrutacja online"}},
        ]
    }


class OlxTests(unittest.TestCase):
    def test_workplace_labels(self):
        self.assertEqual(
            extract_work_mode(_olx_item(["on_site"], "W siedzibie firmy")),
            ("office", "W siedzibie firmy"),
        )
        self.assertEqual(
            extract_work_mode(_olx_item(["on_site", "remote_work_possibility"],
                                        "W siedzibie firmy, Praca zdalna"))[0],
            "hybrid",
        )
        self.assertEqual(
            extract_work_mode(_olx_item(["remote_work_possibility"], "Praca zdalna"))[0],
            "remote",
        )

    def test_remote_recruitment_is_not_remote_work(self):
        item = {"params": [{"key": "remote_recruitment",
                            "value": {"key": "1", "label": "Rekrutacja online"}}]}
        self.assertEqual(extract_work_mode(item), (None, None))


# --------------------------------------------------------------------------- #
# pracuj
# --------------------------------------------------------------------------- #
def _pracuj_page(levels, modes, remote=False):
    data = {
        "attributes": {
            "description": "Analityk",
            "displayEmployerName": "Acme",
            "employment": {
                "positionLevels": [{"name": n} for n in levels],
                "workModes": [{"name": n} for n in modes],
                "entirelyRemoteWork": remote,
                "typesOfContracts": [],
                "workSchedules": [],
            },
        },
        "textSections": [
            {"sectionType": "technologies-expected", "textElements": ["SQL", "BPMN"]},
            {"sectionType": "technologies-optional", "textElements": ["UML", "Jira"]},
        ],
    }
    payload = {"props": {"pageProps": {"dehydratedState": {"queries": [{"state": {"data": data}}]}}}}
    return f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'


class PracujTests(unittest.TestCase):
    def test_expected_and_optional_are_split(self):
        detail = _detail_from_html(_pracuj_page(["kierownik / kierowniczka"], ["praca hybrydowa"]))
        self.assertEqual(detail["technologies_required"], ["SQL", "BPMN"])
        self.assertEqual(detail["technologies_nice"], ["UML", "Jira"])
        self.assertEqual(detail["technologies"], ["SQL", "BPMN", "UML", "Jira"])
        self.assertEqual(detail["seniority_levels"], ["manager"])
        self.assertEqual(detail["work_mode"], "hybrid")
        self.assertEqual(detail["work_mode_raw"], "praca hybrydowa")

    def test_all_levels_and_mixed_modes(self):
        detail = _detail_from_html(_pracuj_page(
            ["starszy specjalista / starsza specjalistka (senior)", "ekspert / ekspertka"],
            ["praca stacjonarna", "praca zdalna"],
        ))
        self.assertEqual(detail["seniority_levels"], ["senior", "expert"])
        self.assertEqual(detail["work_mode"], "hybrid")

    def test_entirely_remote(self):
        detail = _detail_from_html(_pracuj_page(["specjalista (Mid / Regular)"], ["praca zdalna"], True))
        self.assertEqual(detail["work_mode"], "remote")
        self.assertEqual(detail["work_mode_raw"], "entirelyRemoteWork; praca zdalna")
        self.assertEqual(detail["seniority_levels"], ["mid"])

    def test_no_offer_data(self):
        detail = _detail_from_html("<html></html>")
        self.assertIsNone(detail["technologies_required"])
        self.assertIsNone(detail["technologies_nice"])


# --------------------------------------------------------------------------- #
# storage
# --------------------------------------------------------------------------- #
OLD_SCHEMA_SQL = f"""
CREATE TABLE {TABLE} (
    surrogate_key VARCHAR PRIMARY KEY, job_id VARCHAR NOT NULL, job_board VARCHAR,
    url VARCHAR, company VARCHAR, title VARCHAR, city VARCHAR, description VARCHAR,
    technologies VARCHAR[], salary_month INTEGER, salary_hour INTEGER, currency VARCHAR,
    salary_type VARCHAR, employment_types VARCHAR[], seniority VARCHAR, work_mode VARCHAR,
    application_url VARCHAR, benefits VARCHAR[], date_posted DATE, scraped_at TIMESTAMP,
    hash_diff VARCHAR, effective_from DATE NOT NULL, effective_to DATE,
    is_current BOOLEAN NOT NULL
);
INSERT INTO {TABLE} VALUES (
    'old-1', 'justjoin:https://justjoin.it/job-offer/a', 'justjoin',
    'https://justjoin.it/job-offer/a', 'Acme', 'Dev', 'Kraków', 'desc', ['Go'],
    NULL, NULL, NULL, NULL, [], NULL, NULL, NULL, [], NULL, '2026-11-15 12:00:00',
    'oldhash', '2026-11-15', NULL, TRUE
);
"""


def _listing(**overrides):
    base = {
        "board": "justjoin",
        "url": "https://justjoin.it/job-offer/a",
        "title": "Dev",
        "company": "Acme",
        "technologies_list": ["Go"],
        "technologies_required": ["Go"],
        "technologies_nice": ["Docker"],
        "seniority": "Senior",
        "work_mode": "hybrid",
        "work_mode_raw": "hybrid",
        "scraped_at": "2026-11-18T10:00:00+00:00",
    }
    base.update(overrides)
    return base


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "t.duckdb")

    def tearDown(self):
        self.tmp.cleanup()

    def _rows(self, sql):
        conn = duckdb.connect(self.db, read_only=True)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def test_new_columns_are_written(self):
        ingest_listings(self.db, [
            _listing(),
            _listing(board="bulldogjobs", url="https://bulldogjob.pl/x",
                     technologies_required=None, technologies_nice=None,
                     technologies_list=["Go"], seniority="medium", work_mode_raw=None),
        ])
        rows = self._rows(
            f"SELECT job_board, technologies, technologies_required, technologies_nice, "
            f"seniority, seniority_levels, work_mode, work_mode_raw FROM {TABLE} ORDER BY 1"
        )
        self.assertEqual(rows[0], ("bulldogjobs", ["Go"], None, None, "medium", ["mid"], "hybrid", None))
        self.assertEqual(
            rows[1],
            ("justjoin", ["Go", "Docker"], ["Go"], ["Docker"], "Senior", ["senior"], "hybrid", "hybrid"),
        )

    def test_typed_fields_are_hashed_but_work_mode_raw_is_not(self):
        row = normalise_row(_listing())
        self.assertNotEqual(hash_diff(row), hash_diff(normalise_row(_listing(technologies_nice=["Git"]))))
        self.assertNotEqual(hash_diff(row), hash_diff(normalise_row(_listing(technologies_nice=None))))
        self.assertNotEqual(hash_diff(row), hash_diff(normalise_row(_listing(seniority_levels=["lead"]))))
        self.assertEqual(hash_diff(row), hash_diff(normalise_row(_listing(work_mode_raw="other text"))))

    def test_rerun_with_new_columns_adds_no_version(self):
        ingest_listings(self.db, [_listing()])
        summary = ingest_listings(self.db, [_listing(work_mode_raw="changed")])
        self.assertEqual(summary["unchanged"], 1)
        self.assertEqual(self._rows(f"SELECT count(*) FROM {TABLE}")[0][0], 1)

    def test_old_database_is_migrated(self):
        conn = duckdb.connect(self.db)
        conn.execute(OLD_SCHEMA_SQL)
        conn.close()

        summary = ingest_listings(self.db, [_listing()])
        self.assertEqual(summary["updated"], 1)
        columns = {r[0] for r in self._rows(f"DESCRIBE {TABLE}")}
        self.assertTrue({"technologies_required", "technologies_nice",
                         "seniority_levels", "work_mode_raw"} <= columns)
        rows = self._rows(
            f"SELECT surrogate_key, is_current, technologies_required, seniority_levels "
            f"FROM {TABLE} ORDER BY is_current"
        )
        self.assertEqual(rows[0], ("old-1", False, None, None))
        self.assertTrue(rows[1][1])
        self.assertEqual(rows[1][2:], (["Go"], ["senior"]))


if __name__ == "__main__":
    unittest.main()
