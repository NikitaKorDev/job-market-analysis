import argparse
import json
import time

import scrapers.scraper_hub as scraper_hub
from dashboard import Dashboard, fmt_duration
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape all job boards and ingest into DuckDB.")
    parser.add_argument(
        "--stall-after", type=float, default=120.0, metavar="SEC",
        help="flag a scraper as STALLED after this many seconds without output or progress (default: 120)",
    )
    parser.add_argument(
        "--quiet-after", type=float, default=30.0, metavar="SEC",
        help="mark a scraper as idle after this many silent seconds (default: 30)",
    )
    logs = parser.add_mutually_exclusive_group()
    logs.add_argument(
        "--quiet", action="store_const", dest="logs", const="notable",
        help="only show warnings/errors from scrapers above the progress bars",
    )
    logs.add_argument(
        "--no-logs", action="store_const", dest="logs", const="none",
        help="hide all scraper output; show only the progress bars and stall alerts",
    )
    parser.set_defaults(logs="all")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()

    dashboard = Dashboard(
        scraper_hub.scraper_names(),
        quiet_after=args.quiet_after,
        stall_after=args.stall_after,
        show_logs=args.logs,
    )
    console = dashboard.console
    console.rule("[bold]Job market scrape")

    try:
        with dashboard:
            raw_data = scraper_hub.fetch_all_listings()
    finally:
        console.print()
        console.print(dashboard.summary_table())
        console.print()

    # Save the flat scrape result as a raw backup.
    with open(RAW_FILE, "w", encoding="utf-8") as file:
        json.dump(raw_data, file, indent=2, ensure_ascii=False)
        file.write("\n")

    listings = flatten_boards(raw_data)
    console.print(f"Raw data backed up to [bold]{RAW_FILE}[/].")

    with console.status(f"Ingesting {len(listings):,} listings into DuckDB…", spinner="dots"):
        summary = ingest_listings(DB_FILE, listings)
    console.print(
        "[bold green]✔[/] DuckDB SCD2 ingestion complete: "
        f"[green]{summary['new']:,} new[/], [cyan]{summary['updated']:,} updated[/], "
        f"{summary['unchanged']:,} unchanged, [dim]{summary['skipped']:,} skipped[/]."
    )
    console.print(f"Total time: [bold]{fmt_duration(time.time() - started)}[/]")


if __name__ == "__main__":
    main()
