import asyncio
import importlib
import inspect
import json
import multiprocessing
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

# Full package paths relative to the project root
from scrapers import progress
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
    #("pracuj", fetch_pracuj_listings, {"max_pages": 0}, "async"),
    ("justjoin", fetch_justjoin_listings, {}, "async"),
    ("nofluff", fetch_no_fluff_listings, {}, "process"),
    #("praca", fetch_praca_listings, {}, "thread"),
    ("bulldogjobs", fetch_bulldog_jobs, {"headless": True, "max_pages": None}, "async"),
)


def _mp_invoke(name: str, module_name: str, func_name: str, kwargs: dict):
    """Child-process entry: import and call a sync scraper (Windows-spawn safe)."""
    progress.bind(name)
    try:
        fn = getattr(importlib.import_module(module_name), func_name)
        return fn(**kwargs)
    finally:
        sys.stdout.flush()
        sys.stderr.flush()


async def _call(name, fn, kwargs, how, process_pool) -> list[dict]:
    """Run one scraper; a failure is logged and never takes the others down."""
    # gather() wraps each coroutine in its own Task with a copied context, so
    # this binding (and everything it tags: output, progress) stays per scraper.
    progress.bind(name)
    progress.set_state("running")
    try:
        if how == "async":
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{name} marked async but {fn!r} is not a coroutine")
            result = await fn(**kwargs)
        elif how == "process":
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                process_pool, _mp_invoke, name, fn.__module__, fn.__name__, kwargs
            )
        else:
            result = await asyncio.to_thread(fn, **kwargs)
        listings = _as_listings(name, result)
        progress.set_state("done", count=len(listings))
        return listings
    except Exception as e:
        traceback.print_exc()
        progress.set_state("failed", error=f"{type(e).__name__}: {e}")
        return []


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
    return dict(zip(names, gathered))


def scraper_names() -> list[str]:
    return [name for name, *_ in _SCRAPERS]


def fetch_all_listings():
    # Child processes report progress/output over this queue when a front end
    # (dashboard) is listening; otherwise they print straight to the terminal.
    mp_context = multiprocessing.get_context()
    queue = mp_context.Queue() if progress.active() else None
    forwarder = progress.start_forwarder(queue) if queue is not None else None
    try:
        with ProcessPoolExecutor(
            max_workers=1,
            mp_context=mp_context,
            initializer=progress.init_child,
            initargs=(queue,),
        ) as process_pool:
            results = asyncio.run(_fetch_all(process_pool))
    finally:
        if forwarder is not None:
            queue.put(None)
            forwarder.join(timeout=5)
    if not progress.active():
        for name, data in results.items():
            print(f"{name}: {len(data)} listings")
    return results


if __name__ == "__main__":
    full_data = fetch_all_listings()

    with open("job_data_raw.json", "w", encoding="utf-8") as file:
        json.dump(full_data, file)
