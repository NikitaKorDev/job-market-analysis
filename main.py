import json
import time

import scrapers.scraper_hub as scraper_hub
from storage import DB_FILE, ingest_listings

RAW_FILE = "job_data_raw.json"


def flatten_boards(raw_data: dict) -> list[dict]:
    """Turn ``{board: [listing, ...]}`` into one flat list of listings."""
    if not isinstance(raw_data, dict):
        return []
    listings = []
    for board_listings in raw_data.values():
        listings.extend(board_listings or [])
    return listings


def main() -> None:
    started = time.time()

    print("Fetching listings from scrapers...")
    raw_data = scraper_hub.fetch_all_listings()

    # Save the flat scrape result as a raw backup.
    with open(RAW_FILE, "w", encoding="utf-8") as file:
        json.dump(raw_data, file, indent=2, ensure_ascii=False)
        file.write("\n")

    listings = flatten_boards(raw_data)
    print(f"Raw data backed up. Ingesting {len(listings)} flat listings...")

    summary = ingest_listings(DB_FILE, listings)
    print(
        "DuckDB SCD2 ingestion complete: "
        f"{summary['new']} new, {summary['updated']} updated, "
        f"{summary['unchanged']} unchanged, {summary['skipped']} skipped."
    )
    print(f"Time taken: {(time.time() - started) / 60:.1f} min")


if __name__ == "__main__":
    main()
