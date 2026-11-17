import asyncio
import importlib
import inspect
import json
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

# Full package paths relative to the project root
from scrapers.common import utc_now_iso
from scrapers.olx.olx import fetch_olx_jobs
from scrapers.pracuj.pracuj import fetch_pracuj_listings
from scrapers.justjoin import fetch_justjoin_listings
from scrapers.nofluffjobs import fetch_no_fluff_listings
from scrapers.praca import fetch_praca_listings
from scrapers.bulldogjobs import fetch_bulldog_jobs

JUSTJOIN_RAW_FILE = Path("justjoin_raw.json")

# (name, fn, kwargs, how)
# how: "async"  — coroutine on this event loop (async Patchright)
#      "thread" — blocking HTTP in a worker thread
#      "process"— sync Patchright; must not share this process with async drivers
# All of these are started together via asyncio.gather (true overlap across sites).
_SCRAPERS = (
    ("olx", fetch_olx_jobs, {}, "thread"),
    ("pracuj", fetch_pracuj_listings, {"max_pages": 0}, "async"),
    ("justjoin", fetch_justjoin_listings, {}, "async"),
    ("nofluff", fetch_no_fluff_listings, {}, "process"),
    ("praca", fetch_praca_listings, {}, "thread"),
    ("bulldogjobs", fetch_bulldog_jobs, {"headless": True, "max_pages": None}, "async"),
)


def _mp_invoke(module_name: str, func_name: str, kwargs: dict):
    """Child-process entry: import and call a sync scraper (Windows-spawn safe)."""
    fn = getattr(importlib.import_module(module_name), func_name)
    return fn(**kwargs)


async def _call(name, fn, kwargs, how, process_pool):
    """Run one scraper; a failure is logged and never takes the others down."""
    print(f"[{name}] starting")
    try:
        if how == "async":
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{name} marked async but {fn!r} is not a coroutine")
            result = await fn(**kwargs)
        elif how == "process":
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                process_pool, _mp_invoke, fn.__module__, fn.__name__, kwargs
            )
        else:
            result = await asyncio.to_thread(fn, **kwargs)
        print(f"[{name}] finished")
        return result
    except Exception:
        traceback.print_exc()
        print(f"[{name}] failed, proceeding.\n")
        return None


def _as_listings(name: str, result) -> list[dict]:
    """Normalize a scraper result to a list of flat listing dictionaries.

    The only filtering here is a sanity guard for genuinely broken scrapes
    (expired redirects, blocked/empty pages): a listing must be a dict and must
    have both a URL and a title.
    """
    if result is None:
        return []
    if isinstance(result, tuple):  # unexpected shape: flatten one level
        result = [item for part in result for item in (part or [])]

    listings = []
    for listing in result or []:
        if not isinstance(listing, dict) or not listing.get("url") or not listing.get("title"):
            continue
        listing.setdefault("board", name)
        listing.setdefault("scraped_at", utc_now_iso())
        listings.append(listing)
    return listings


async def _fetch_all(process_pool):
    names = [name for name, *_ in _SCRAPERS]
    gathered = await asyncio.gather(
        *(
            _call(name, fn, kwargs, how, process_pool)
            for name, fn, kwargs, how in _SCRAPERS
        )
    )
    by_name = dict(zip(names, gathered))

    return {
        name: _as_listings(name, by_name.get(name))
        for name in names
    }


def fetch_all_listings():
    print("\n\n\n\n\n")
    with ProcessPoolExecutor(max_workers=1) as process_pool:
        results = asyncio.run(_fetch_all(process_pool))
    for name, data in results.items():
        print(f"{name}: {len(data)} listings")
    return results


if __name__ == "__main__":
    full_data = fetch_all_listings()

    with open("job_data_raw.json", "w", encoding="utf-8") as file:
        json.dump(full_data, file)
