# Job market data — implementation plan

## Goal

Every scraper returns **flat listing dictionaries**. There is no envelope
(`schema` / `meta`) wrapper and no Python-side schema validator. DuckDB enforces
the final schema and types during Phase 3 ingestion.

## Decisions (confirmed by user)

1. **praca.pl** — leave the stub empty; implement later, not in this round.
2. **OLX** — broaden: stop requiring a salary, walk all available result pages, keep IT-relevant titles.
3. **SCD hash** — exclude `description` from `hash_diff` (less version churn).
4. **Validation** — no Python-side schema validator. The hub only drops
   genuinely broken scrapes (no `url` or no `title`).

## Flat listing contract

One dictionary per offer, with these core keys:

```text
board, url, company, offer_id
title, city, description, technologies, technologies_list
salary-month, salary-hour, currency, salary_type
employment_types, seniority, work_mode, application_url, benefits
date, scraped_at
```

Normalisation rules:

- `technologies` is a comma-joined string; `technologies_list` is the list form.
- Undisclosed salary values are `None`, never `0` or `""`.
- `salary_type` is `brutto` / `netto` (or `None` when ambiguous).
- `work_mode` is `remote` / `hybrid` / `office`.
- `scraped_at` is an ISO-8601 UTC timestamp.
- `board` / `scraped_at` are defaulted by the hub if a scraper omits them.

---

## Phase 1 — flat scraper refactor (DONE)

- **`scrapers/common.py`** — `flat_listing()` plus normalisation helpers
  (`as_list`, `text_list`, `int_or_none`, `str_or_none`,
  `normalize_salary_type`, `normalize_work_mode`, `utc_now_iso`).
- **`scrapers/scraper_hub.py`**
  - Consumes flat dictionaries directly; no envelope unpacking.
  - Sanity guard only: a listing must be a dict with a non-empty `url` and `title`.
  - Defaults `board` and `scraped_at`, returns `{board: [listing, ...]}`.
- **`listing.py`** — deleted.
- **`scrapers/olx/olx.py`**
  - Uses OLX's own IT subcategory `category_id=56` ("Praca > Informatyka").
  - No salary requirement; walks all API pages until the advertised total,
    de-duplicating promoted offers by id.
  - Adds salary type, employment types, work mode, application URL, currency, date.
- **`scrapers/pracuj/pracuj.py`**
  - `build_listings()` pairs raw CSV rows with card records and emits flat dicts.
  - Detail pages add full description, technologies, company, date, currency,
    salary, salary type, employment types, seniority, work mode, application URL
    and benefits.
- **`scrapers/justjoin/justjoin.py`**
  - `raw_to_flat()` converts cached API or HTML raw records to flat listings.
  - API path reads `companyName`, `applyUrl`, `workplaceType`,
    `experienceLevel`, `employmentTypes` (original currency, gross/net) and skills.
  - HTML path reads schema.org `JobPosting` (`hiringOrganization`,
    `employmentType`, `jobLocationType`, `jobBenefits`, `baseSalary`).
- **`scrapers/nofluffjobs/nofluffjobs.py`**
  - Flat API + DOM output with salary fallback through
    `essentials.originalSalary`, company, seniority, employment types,
    work mode, benefits and application URL.
- **`scrapers/bulldogjobs/bulldogjobs.py`**
  - Removed local validator; `parse_salary()` now returns
    `(month, hour, currency, salary_type)`.
  - `detail_handler()` returns a dict with employment type, seniority, work mode,
    benefits, skills and JSON-LD salary.
- **`scrapers/praca/praca.py`** — left as the empty stub (decision 1).

### Phase 1 verification (live / cached)

| Board | Sample | Result |
|---|---|---|
| OLX | page 1 live | 29 flat listings, all core keys present |
| JustJoin | 11,335 cached offers | all converted to flat dicts |
| Pracuj | live detail page | description, 16 techs, company, PLN, remote |
| Nofluff | live API + detail | 3 flat listings, salary, benefits, remote |
| BulldogJob | page 1 live | 50 flat listings, detail fields populated |
| Unit tests | `tests/test_flat_listings.py` | 10/10 passing |

---

## Phase 2 — canonicalisation (REMOVED)

Python no longer canonicalises or validates listings. Scrapers emit the flat
contract above and DuckDB enforces types at ingestion time.

---

## Phase 3 — DuckDB storage (later)

```sql
CREATE TABLE dim_job_listings (
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
```

- Stable `job_id`: `board:canonical_url`, fallback `board:offer_id`; never title-only.
- All flat fields are stored; `salary_month` / `salary_hour` are nullable.
- SCD hash excludes `description` (decision 3).
- Collision-free surrogate key (include `hash_diff` or sequence).
- `technologies`, `employment_types`, `benefits` stored as `VARCHAR[]`.
- Jobby reads only `is_current = TRUE`.

---

## Phase 4 — dropped

No backfill from `job_data_raw.json`. Fresh scrapes only.

---

## Phase 5 — verification (later)

- DuckDB ingestion succeeds with flat dictionaries and nullable salary fields.
- One current version per `job_id`; no duplicate URLs.
- Non-empty percentages for description/url/company/technologies.
- Re-running with unchanged data adds no SCD versions.
- Smoke query: `SELECT url, title, company, description, technologies FROM dim_job_listings WHERE is_current`.
