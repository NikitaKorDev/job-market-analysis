import asyncio
import json
import traceback
from pathlib import Path

# Full package paths relative to the project root
from scrapers.olx.olx import fetch_olx_jobs
from scrapers.pracuj.pracuj import fetch_pracuj_listings
from scrapers.justjoin import fetch_justjoin_listings
from scrapers.nofluffjobs import fetch_no_fluff_listings
from scrapers.praca import fetch_praca_listings
from scrapers.bulldogjobs import fetch_bulldog_jobs

JUSTJOIN_RAW_FILE = Path("justjoin_raw.json")


async def _guard(name, awaitable):
    """Run one scraper; a failure is logged and never takes the others down."""
    try:
        return await awaitable
    except Exception:
        print(f"[{name}] failed, proceeding.\n{traceback.format_exc()}")
        return None


async def _fetch_all():
    # Different sites, so running them at the same time doesn't add load to any
    # single site. Blocking (sync) scrapers run in worker threads.
    olx, pracuj, justjoin, nofluff, praca, bulldog = await asyncio.gather(
        _guard("olx", asyncio.to_thread(fetch_olx_jobs)),
        _guard("pracuj", fetch_pracuj_listings(max_pages=2)),
        _guard("justjoin", fetch_justjoin_listings()),
        _guard("nofluff", asyncio.to_thread(fetch_no_fluff_listings)),
        _guard("praca", asyncio.to_thread(fetch_praca_listings)),
        _guard("bulldogjobs", asyncio.to_thread(fetch_bulldog_jobs)),
    )

    # justjoin returns (raw, schema)
    justjoin_raw, justjoin_data = justjoin if justjoin else ([], [])
    if justjoin_raw:
        try:
            # "w", not "a": appending JSON documents produces a file no parser can read
            with open(JUSTJOIN_RAW_FILE, "w", encoding="utf-8") as f:
                json.dump(justjoin_raw, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Failed to write {JUSTJOIN_RAW_FILE} ({e}), proceeding.")

    return {
        "olx": olx or [],
        "pracuj": pracuj or [],
        "justjoin": justjoin_data,
        "nofluff": nofluff or [],
        "praca": praca or [],
        "bulldogjobs": bulldog or [],
    }


def fetch_all_listings():
    results = asyncio.run(_fetch_all())
    for name, data in results.items():
        print(f"{name}: {len(data)} listings")
    return results


if __name__ == "__main__":
    fetch_all_listings()