"""Async patchright scraper for bulldogjob.pl job listings (with pagination)."""

import asyncio
import html
import json
import random
import re
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from patchright.async_api import BrowserContext, Locator
from patchright.async_api import TimeoutError as PWTimeout
from patchright.async_api import async_playwright

BASE_URL = "https://bulldogjob.pl/companies/jobs/s/page,1"
PAGE_URL = "https://bulldogjob.pl/companies/jobs/s/page,{page}"
SITE_ROOT = "https://bulldogjob.pl"

# Everything lives next to this file, regardless of the current working directory
SCRIPT_DIR = Path(__file__).resolve().parent
LINKS_FILE = SCRIPT_DIR / "scraped_links.txt"
PROFILE_DIR = SCRIPT_DIR / ".patchright-profile"

HOURS_PER_MONTH = 168  # 21 working days * 8h, used for month <-> hour conversion

# Resilient selectors (wildcards survive hashed CSS-module class changes)
CARD = 'a[class*="JobListItem_item"]'
SEL_TITLE = '[class*="JobListItem_item__title"] h3'
SEL_CITY = '[class*="JobListItem_item__details"] span.text-xs'
SEL_SALARY = '[class*="JobListItem_item__salary"]'
SEL_TAGS = '[class*="JobListItem_item__tags"] span'


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
def check_listing(listing: dict):
    schema = get_listing_schema()

    def matches(value, template):
        if isinstance(template, dict):
            return (
                isinstance(value, dict)
                and set(value) == set(template)
                and all(matches(value[key], template[key]) for key in template)
            )
        if isinstance(template, list):
            return (
                isinstance(value, list)
                and all(matches(item, template[0]) for item in value)
                if template
                else isinstance(value, list)
            )
        return type(value) is type(template)

    return matches(listing, schema)


def get_listing_schema():
    """Get a listing schema,
    title,
    city,
    description,
    technologies (if possible) - The tech stack needed for this job
    salary-month (if present or if possible to calculate reliably)
    and salary-hour (if present or possible to calculate reliably)"""
    return {
        "title": "",
        "city": "",
        "description": "",
        "technologies": "",
        "salary-month": 0,
        "salary-hour": 0,
        "currency": "",
        "date": "",
    }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
async def _text(card: Locator, selector: str) -> str:
    loc = card.locator(selector)
    if await loc.count() == 0:
        return ""
    return (await loc.first.inner_text()).strip()


def parse_salary(raw: str) -> tuple[int, int, str]:
    """Return (salary_month, salary_hour, currency) from a card salary string.

    Uses the midpoint of a range. Hourly rates ("/h", "godz") are converted to
    monthly with HOURS_PER_MONTH, and monthly to hourly. Returns zeros if the
    string has no numbers.
    """
    if not raw:
        return 0, 0, ""

    text = raw.replace("\u00a0", " ").replace("\u202f", " ")

    cur_m = re.search(r"\b(PLN|EUR|USD|GBP|CHF|CZK)\b|zł", text, re.I)
    currency = ""
    if cur_m:
        currency = "PLN" if cur_m.group(0).lower() == "zł" else cur_m.group(0).upper()

    nums = [int(n.replace(" ", "")) for n in re.findall(r"\d[\d ]*", text) if n.strip()]
    nums = nums[:2]
    if not nums:
        return 0, 0, currency

    value = round(sum(nums) / len(nums))
    is_hourly = bool(re.search(r"/\s*h\b|/\s*godz|per hour|hourly|\bh\b", text, re.I))

    if is_hourly:
        return value * HOURS_PER_MONTH, value, currency
    return value, round(value / HOURS_PER_MONTH), currency


def _strip_html(raw: str) -> str:
    text = re.sub(r"<\s*(br|/p|/li|/h\d)\s*/?>", "\n", raw, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return re.sub(r"\n\s*\n+", "\n\n", text).strip()


def _find_job_posting(node):
    """Recursively find a schema.org JobPosting object in parsed JSON-LD."""
    if isinstance(node, dict):
        node_type = node.get("@type")
        types = node_type if isinstance(node_type, list) else [node_type]
        if "JobPosting" in types:
            return node
        for value in node.values():
            found = _find_job_posting(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_job_posting(item)
            if found:
                return found
    return None


# --------------------------------------------------------------------------- #
# Throttling: bounded tabs + global request rate + adaptive back-off
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Persistent store of already-scraped links
# --------------------------------------------------------------------------- #
def normalize_url(url: str) -> str:
    """Canonical form used for de-duplication: no query string, fragment or trailing slash."""
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}{parts.path}".rstrip("/")


class LinkStore:
    """Append-only text file (one URL per line) of listings that are done.

    Each link is written the moment it is added, so a crash or Ctrl+C never
    loses progress and the file never has to be rewritten.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._links: set[str] = set()
        if self.path.exists():
            lines = self.path.read_text(encoding="utf-8").splitlines()
            self._links = {normalize_url(line.strip()) for line in lines if line.strip()}

    def __contains__(self, url: str) -> bool:
        return url in self._links

    def __len__(self) -> int:
        return len(self._links)

    def add(self, url: str) -> None:
        if url in self._links:
            return
        self._links.add(url)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(url + "\n")


BLOCK_STATUS = {403, 429, 502, 503, 504}


class Blocked(Exception):
    def __init__(self, status: int, retry_after: float | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


class Throttle:
    """Async context manager shared by every request.

    - `concurrency` caps open tabs (protects your RAM/CPU)
    - `rps` caps request starts per second, with jitter (protects you from bans)
    - penalize() pauses ALL workers and permanently slows the rate after a
      block signal; reward() slowly speeds back up on success.
    """

    def __init__(self, concurrency: int, rps: float):
        self._sem = asyncio.Semaphore(concurrency)
        self._base = 1.0 / rps
        self._interval = self._base
        self._lock = asyncio.Lock()
        self._next_slot = 0.0
        self._pause_until = 0.0
        self._strikes = 0.0

    async def __aenter__(self):
        await self._sem.acquire()
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            start = max(now, self._next_slot, self._pause_until)
            self._next_slot = start + self._interval * random.uniform(0.8, 1.4)
        if start > now:
            await asyncio.sleep(start - now)
        # a block may have been signalled while we were waiting for our slot
        while loop.time() < self._pause_until:
            await asyncio.sleep(self._pause_until - loop.time())
        return self

    async def __aexit__(self, *exc):
        self._sem.release()

    def penalize(self, retry_after: float | None = None):
        now = asyncio.get_running_loop().time()
        self._strikes = min(self._strikes + 1, 5)
        wait = retry_after or min(5 * 2**self._strikes, 180)
        self._pause_until = max(self._pause_until, now + wait)
        self._interval = min(self._interval * 1.5, 5.0)
        print(f"  ! rate-limited, pausing {wait:.0f}s and slowing to "
              f"{1 / self._interval:.1f} req/s")

    def reward(self):
        self._strikes = max(0.0, self._strikes - 0.05)
        self._interval = max(self._base, self._interval * 0.97)


async def fetch(context: BrowserContext, url: str, throttle: Throttle, handler,
                retries: int = 4):
    """Open `url` in a fresh tab, run `await handler(page, status)`, retry on
    blocks/timeouts. Returns the handler result, or None if all retries fail."""
    for attempt in range(retries + 1):
        try:
            async with throttle:
                page = await context.new_page()
                try:
                    resp = await page.goto(url, wait_until="domcontentloaded",
                                           timeout=45_000)
                    status = resp.status if resp else 0
                    if status in BLOCK_STATUS:
                        ra = (resp.headers.get("retry-after") or "") if resp else ""
                        raise Blocked(status, float(ra) if ra.isdigit() else None)
                    result = await handler(page, status)
                finally:
                    await page.close()
            throttle.reward()
            return result
        except Blocked as exc:
            throttle.penalize(exc.retry_after)
        except Exception as exc:  # noqa: BLE001 - timeouts, network hiccups
            print(f"  ! {url} attempt {attempt + 1} failed: {exc.__class__.__name__}")
            await asyncio.sleep(min(2**attempt, 15) + random.random())
    print(f"  ! giving up on {url}")
    return None


# --------------------------------------------------------------------------- #
# Page handlers
# --------------------------------------------------------------------------- #
async def listing_handler(page, status: int) -> list[dict]:
    """Return the cards on a listing page; [] means 'no cards' (end of results)."""
    if status == 404:
        return []
    try:
        await page.wait_for_selector(CARD, timeout=15_000)
    except PWTimeout:
        return []
    return await scrape_cards(page)


async def detail_handler(page, status: int) -> tuple[str, str]:
    """Return (description, date) from a job page. Empty strings if missing."""
    blocks = await page.eval_on_selector_all(
        'script[type="application/ld+json"]',
        "els => els.map(e => e.textContent)",
    )
    for block in blocks:
        try:
            posting = _find_job_posting(json.loads(block))
        except json.JSONDecodeError:
            continue
        if posting:
            description = _strip_html(posting.get("description", "") or "")
            date = str(posting.get("datePosted", "") or "")[:10]
            if description:
                return description, date

    meta = await page.evaluate(
        "() => document.querySelector('meta[name=description]')?.content || ''"
    )
    return meta.strip(), ""


async def scrape_cards(page) -> list[dict]:
    """Scrape all job cards on the current listing page."""
    cards = page.locator(CARD)
    out = []
    for i in range(await cards.count()):
        card = cards.nth(i)
        salary_raw = await _text(card, SEL_SALARY)
        month, hour, currency = parse_salary(salary_raw)
        tags = [t.strip() for t in await card.locator(SEL_TAGS).all_inner_texts()]
        href = await card.get_attribute("href") or ""

        out.append(
            {
                "url": normalize_url(urljoin(SITE_ROOT, href)),
                "title": await _text(card, SEL_TITLE),
                "city": await _text(card, SEL_CITY),
                "technologies": ", ".join(t for t in tags if t),
                "salary-month": month,
                "salary-hour": hour,
                "currency": currency,
            }
        )
    return out


async def _block_heavy_assets(route):
    if route.request.resource_type in {"image", "media", "font"}:
        await route.abort()
    else:
        await route.continue_()


# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
async def fetch_bulldog_jobs(
    max_pages: int | None = None,
    with_details: bool = True,
    headless: bool = True,
    concurrency: int = 6,
    rps: float = 4.0,
    links_file: Path = LINKS_FILE,
) -> list[dict]:
    """Scrape BulldogJob concurrently, skipping listings scraped in earlier runs.

    Returns ONLY the listings that are new in this run. A listing's link is
    saved to `links_file` (default: scraped_links.txt next to this script) as
    soon as it has been fully scraped; listings whose detail page failed are
    not saved, so the next run retries them.

    max_pages    -- stop after N listing pages (None = until the last page)
    with_details -- open each job page to fill `description` and `date`
    concurrency  -- max simultaneously open tabs
    rps          -- max request starts per second (auto-lowered on 403/429/5xx)
    links_file   -- where already-scraped links are stored
    """
    store = LinkStore(links_file)
    print(f"{len(store)} listings already scraped ({store.path})")

    throttle = Throttle(concurrency, rps)
    raw_jobs: list[dict] = []
    seen_run: set[str] = set()  # every link met in THIS run (detects wrap-around pages)
    skipped = 0
    failed_pages: list[int] = []

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=headless,
            no_viewport=True,
        )
        try:
            await context.route("**/*", _block_heavy_assets)

            # ---- phase 1: listing pages, in concurrent waves ------------------
            page_no, finished = 1, False
            while not finished and (max_pages is None or page_no <= max_pages):
                last = page_no + concurrency - 1
                if max_pages is not None:
                    last = min(last, max_pages)
                numbers = list(range(page_no, last + 1))
                urls = [BASE_URL if n == 1 else PAGE_URL.format(page=n) for n in numbers]
                print(f"Pages {numbers[0]}-{numbers[-1]}")

                results = await asyncio.gather(
                    *(fetch(context, u, throttle, listing_handler) for u in urls)
                )

                # process in page order; ignore anything after the first empty page
                for n, cards in zip(numbers, results):
                    if cards is None:
                        failed_pages.append(n)
                        continue
                    # "end of results" = no cards, or only cards already seen this run.
                    # Links from previous runs do NOT count: old pages are not the end.
                    fresh = [c for c in cards if c["url"] not in seen_run]
                    if not fresh:
                        print(f"  page {n}: nothing new -> end of results")
                        finished = True
                        break
                    seen_run.update(c["url"] for c in fresh)
                    skipped += sum(c["url"] in store for c in fresh)
                    raw_jobs.extend(fresh)
                print(f"  new so far: {len(raw_jobs)} (already scraped, skipped: {skipped})")
                page_no = last + 1

            if failed_pages:
                print(f"WARNING: listing pages that failed after retries: {failed_pages}")

            # ---- phase 2: detail pages, all queued, throttle keeps it safe ----
            details: list[tuple[str, str] | None]
            if with_details and raw_jobs:
                print(f"Fetching {len(raw_jobs)} detail pages...")

                async def scrape_one(job: dict):
                    result = await fetch(context, job["url"], throttle, detail_handler)
                    if result is not None:
                        store.add(job["url"])  # saved immediately, survives crashes
                    return result

                details = await asyncio.gather(*(scrape_one(j) for j in raw_jobs))
            else:
                for job in raw_jobs:
                    store.add(job["url"])
                details = [("", "")] * len(raw_jobs)
        finally:
            await context.close()

    listings = []
    failed_details = 0
    for job, detail in zip(raw_jobs, details):
        if detail is None:  # not stored -> will be retried next run
            failed_details += 1
            continue
        description, date = detail
        listings.append(
            {
                "title": job["title"],
                "city": job["city"],
                "description": description,
                "technologies": job["technologies"],
                "salary-month": job["salary-month"],
                "salary-hour": job["salary-hour"],
                "currency": job["currency"],
                "date": date,
            }
        )
    if failed_details:
        print(f"{failed_details} listings failed and will be retried next run")
    return listings


if __name__ == "__main__":
    results = asyncio.run(fetch_bulldog_jobs(max_pages=2))
    valid = [r for r in results if check_listing(r)]
    print(f"{len(valid)}/{len(results)} listings match the schema")
    print(json.dumps(results[:3], indent=2, ensure_ascii=False))