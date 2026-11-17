# job-market-analysis

Scrapes IT job listings from Polish job boards and stores them in a DuckDB
table with full change history (SCD type 2).

## Boards

| Board | Module | Method |
|---|---|---|
| OLX | `scrapers/olx` | JSON API (IT subcategory) |
| Pracuj.pl | `scrapers/pracuj` | Patchright browser + detail pages |
| JustJoin.it | `scrapers/justjoin` | Candidate API, HTML fallback |
| No Fluff Jobs | `scrapers/nofluffjobs` | API + DOM |
| Bulldogjob | `scrapers/bulldogjobs` | Patchright browser |
| Praca.pl | `scrapers/praca` | not implemented yet |

## How it works

1. `scrapers/scraper_hub.py` runs every scraper concurrently. A failing scraper
   is logged and skipped.
2. Each scraper returns **flat listing dicts**. See `scrapers/common.py`
   (`flat_listing`) and the contract in [`plan.md`](plan.md).
3. `main.py` saves the raw result to `job_data_raw.json`.
4. `storage.py` ingests the listings into `job_listings.duckdb`
   (`dim_job_listings`):
   - `job_id` is stable (`board:canonical_url`).
   - A changed listing gets a new version. An unchanged one is not touched.

## Layout

```text
main.py                 scrape + ingest entry point
storage.py              DuckDB SCD2 storage
scrapers/               one package per board, plus scraper_hub.py and common.py
scripts/verify_phase5.py  integrity checks on the database
tests/                  unit tests
plan.md                 design notes and decisions
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
patchright install chromium
```

## Usage

```bash
python main.py                   # scrape all boards and ingest into DuckDB
python -m scripts.verify_phase5  # check the database
pytest                           # run tests
```

Browser profiles, scraper state, raw scrapes and the database are local runtime
data and are not committed.

## Related

[jobby](https://github.com/NikitaKorDev/jobby) is kept in a separate repository. It reads
`job_listings.duckdb` and uses a local Ollama model to rank listings against
a resume.
