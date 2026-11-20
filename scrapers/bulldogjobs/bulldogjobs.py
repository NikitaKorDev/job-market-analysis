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

from scrapers import progress
from scrapers.common import flat_listing, normalize_salary_type, normalize_seniority_levels

BASE_URL = "https://bulldogjob.pl/companies/jobs/s/page,1"
PAGE_URL = "https://bulldogjob.pl/companies/jobs/s/page,{page}"
SITE_ROOT = "https://bulldogjob.pl"

# Everything lives next to this file, regardless of the current working directory
SCRIPT_DIR = Path(__file__).resolve().parent
LINKS_FILE = SCRIPT_DIR / "scraped_links.txt"
PROFILE_DIR = SCRIPT_DIR / ".patchright-profile"

HOURS_PER_MONTH = 168  # 21 working days * 8h, used for month <-> hour conversion

EMPLOYMENT_NAMES = {
    "full_time": "Pełny etat",
    "part_time": "Część etatu",
    "contractor": "Kontrakt",
    "temporary": "Praca tymczasowa",
    "intern": "Staż",
    "volunteer": "Wolontariat",
    "per_diem": "Stawka dzienna",
    "other": "Inna",
}

# Resilient selectors (wildcards survive hashed CSS-module class changes)
CARD = 'a[class*="JobListItem_item"]'
SEL_TITLE = '[class*="JobListItem_item__title"] h3'
SEL_CITY = '[class*="JobListItem_item__details"] span.text-xs'
SEL_SALARY = '[class*="JobListItem_item__salary"]'
SEL_TAGS = '[class*="JobListItem_item__tags"] span'


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
async def _text(card: Locator, selector: str) -> str:
    loc = card.locator(selector)
    if await loc.count() == 0:
        return ""
    return (await loc.first.inner_text()).strip()


def parse_salary(raw: str) -> tuple[int | None, int | None, str | None, str | None]:
    """Return (salary_month, salary_hour, currency, salary_type) from a card salary string.

    Uses the midpoint of a range. Hourly rates ("/h", "godz") are converted to
    monthly with HOURS_PER_MONTH, and monthly to hourly. None means not disclosed.
    """
    if not raw:
        return None, None, None, None

    text = raw.replace("\u00a0", " ").replace("\u202f", " ")
    salary_type = normalize_salary_type(text)

    cur_m = re.search(r"\b(PLN|EUR|USD|GBP|CHF|CZK)\b|zł", text, re.I)
    currency = None
    if cur_m:
        currency = "PLN" if cur_m.group(0).lower() == "zł" else cur_m.group(0).upper()

    nums = [int(n.replace(" ", "")) for n in re.findall(r"\d[\d ]*", text) if n.strip()]
    nums = nums[:2]
    if not nums:
        return None, None, currency, salary_type

    value = round(sum(nums) / len(nums))
    is_hourly = bool(re.search(r"/\s*h\b|/\s*godz|per hour|hourly|\bh\b", text, re.I))

    if is_hourly:
        return value * HOURS_PER_MONTH, value, currency, salary_type
    return value, round(value / HOURS_PER_MONTH), currency, salary_type


def _string_list(value) -> list[str]:
    """Normalize JSON-LD benefits/skills/employmentType to a list of strings."""
    if value is None:
        return []

    def split(text) -> list[str]:
        return [part.strip() for part in str(text).split(",") if part.strip()]

    if isinstance(value, str):
        return split(value)
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, dict):
                text = item.get("name") or item.get("value") or ""
            else:
                text = item
            for part in split(text):
                if part not in out:
                    out.append(part)
        return out
    return split(value)


def _salary_from_jsonld(posting: dict):
    """Fallback salary from a JobPosting baseSalary block."""
    salary = posting.get("baseSalary")
    if not isinstance(salary, dict):
        return None, None, None, None

    currency = str(salary.get("currency") or "").upper() or None
    value = salary.get("value")
    if isinstance(value, dict):
        raw_nums = [value.get("minValue"), value.get("maxValue"), value.get("value")]
        unit = value.get("unitText") or salary.get("unitText") or ""
    else:
        raw_nums = [value]
        unit = salary.get("unitText") or ""

    nums = [float(n) for n in raw_nums if isinstance(n, (int, float)) and n > 0]
    if not nums:
        return None, None, currency, None

    midpoint = sum(nums) / len(nums)
    unit = str(unit).lower()
    if "hour" in unit:
        month, hour = midpoint * HOURS_PER_MONTH, midpoint
    elif "year" in unit:
        month, hour = midpoint / 12, midpoint / 12 / HOURS_PER_MONTH
    else:
        month, hour = midpoint, midpoint / HOURS_PER_MONTH
    return int(round(month)), int(round(hour)), currency, None


def _seniority(months) -> str | None:
    if not isinstance(months, (int, float)) or months <= 0:
        return None
    if months >= 60:
        return "senior"
    if months >= 24:
        return "mid"
    return "junior"


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

    def __init__(self, path: Path, load_existing: bool = True, persist: bool = True):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.persist = persist
        self._links: set[str] = set()
        if load_existing and self.path.exists():
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
        if self.persist:
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


_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I
)
_NEXT_RE = re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.S | re.I)
_META_DESC_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']', re.I
)

# job.experienceLevel (junior / medium / senior / lead ...) is kept as the board
# text in `seniority`; seniority_levels maps medium -> mid, the rest by name
# (see normalize_seniority_levels).


def _next_job(page_html: str) -> dict:
    """props.pageProps.data.job from the page's __NEXT_DATA__ ({} if absent)."""
    match = _NEXT_RE.search(page_html or "")
    if not match:
        return {}
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    job = (((data.get("props") or {}).get("pageProps") or {}).get("data") or {}).get("job")
    return job if isinstance(job, dict) else {}


def work_mode_from_job(job: dict, posting: dict | None = None) -> tuple[str | None, str | None]:
    """(work_mode, work_mode_raw) for a Bulldogjob offer.

    job.workModes lists the modes a candidate may choose ("on-site", "hybrid",
    "full-remote"):
      only full-remote -> remote; any hybrid, or full-remote together with on-site
      -> hybrid; only on-site -> office.
    Without workModes: job.remote True -> remote; else environment.remotePossible (share
    of remote work, %) > 0 -> hybrid (even 100: with remote False those offers read
    "a mix of remote, client site and office"), 0 -> office; else JSON-LD TELECOMMUTE ->
    remote; otherwise unknown.
    """
    modes = [str(m).strip().lower() for m in job.get("workModes") or [] if str(m).strip()]
    environment = job.get("environment") if isinstance(job.get("environment"), dict) else {}
    possible = environment.get("remotePossible")
    if isinstance(possible, bool) or not isinstance(possible, (int, float)):
        possible = None
    location_type = str((posting or {}).get("jobLocationType") or "").strip()

    raw_parts = []
    if modes:
        raw_parts.append("workModes: " + ", ".join(modes))
    if "remote" in job:
        raw_parts.append(f"remote: {str(bool(job.get('remote'))).lower()}")
    if possible is not None:
        raw_parts.append(f"remotePossible: {possible:g}%")
    if location_type:
        raw_parts.append(f"jobLocationType: {location_type}")
    raw = "; ".join(raw_parts) or None

    remote = any("remote" in m for m in modes)
    hybrid = any("hybrid" in m for m in modes)
    office = any(m in ("on-site", "onsite", "office") for m in modes)
    if modes:
        if hybrid or (remote and office):
            return "hybrid", raw
        if remote:
            return "remote", raw
        if office:
            return "office", raw
    if job.get("remote") is True:
        return "remote", raw
    if possible is not None:
        # job.remote is not True, so this is not a fully remote job: remote work is
        # only possible part of the time (e.g. "mix of remote, client site and office").
        return ("hybrid" if possible > 0 else "office"), raw
    if location_type.upper() == "TELECOMMUTE":
        return "remote", raw
    return None, raw


def parse_detail_html(page_html: str) -> dict:
    """Detail fields from a job page's HTML: schema.org JobPosting + __NEXT_DATA__ job."""
    posting = None
    for block in _LD_RE.findall(page_html or ""):
        try:
            posting = _find_job_posting(json.loads(block))
        except json.JSONDecodeError:
            continue
        if posting:
            break
    posting = posting or {}
    job = _next_job(page_html)

    description = _strip_html(posting.get("description", "") or "")
    if not description:
        match = _META_DESC_RE.search(page_html or "")
        description = html.unescape(match.group(1)).strip() if match else ""
    organization = posting.get("hiringOrganization") or {}
    company = str(organization.get("name") or "").strip() if isinstance(organization, dict) else ""
    if not company and isinstance(job.get("company"), dict):
        company = str(job["company"].get("name") or "").strip()
    month, hour, currency, salary_type = _salary_from_jsonld(posting)
    experience = posting.get("experienceRequirements") or {}
    months = experience.get("monthsOfExperience") if isinstance(experience, dict) else None

    level = str(job.get("experienceLevel") or "").strip()
    # board text if present; otherwise the old months-of-experience estimate
    seniority = level or _seniority(months)
    work_mode, work_mode_raw = work_mode_from_job(job, posting)

    skills = _string_list(posting.get("skills"))
    for tag in job.get("technologyTags") or []:
        if str(tag).strip() and str(tag).strip() not in skills:
            skills.append(str(tag).strip())

    return {
        "description": description,
        "date": str(posting.get("datePosted", "") or "")[:10],
        "company": company,
        "employment_types": [
            EMPLOYMENT_NAMES.get(item.lower(), item)
            for item in _string_list(posting.get("employmentType"))
        ],
        "seniority": seniority,
        "seniority_levels": normalize_seniority_levels(seniority),
        "work_mode": work_mode,
        "work_mode_raw": work_mode_raw,
        "benefits": _string_list(posting.get("benefits") or posting.get("jobBenefits")),
        "skills": skills,
        "salary_month": month,
        "salary_hour": hour,
        "currency": currency,
        "salary_type": salary_type,
    }


async def detail_handler(page, status: int) -> dict:
    """Return detail fields from a job page (empty values if missing)."""
    return parse_detail_html(await page.content())


async def scrape_cards(page) -> list[dict]:
    """Scrape all job cards on the current listing page."""
    cards = page.locator(CARD)
    out = []
    for i in range(await cards.count()):
        card = cards.nth(i)
        salary_raw = await _text(card, SEL_SALARY)
        month, hour, currency, salary_type = parse_salary(salary_raw)
        tags = [t.strip() for t in await card.locator(SEL_TAGS).all_inner_texts()]
        href = await card.get_attribute("href") or ""

        out.append(
            {
                "url": normalize_url(urljoin(SITE_ROOT, href)),
                "title": await _text(card, SEL_TITLE),
                "city": await _text(card, SEL_CITY),
                "technologies": [t for t in tags if t],
                "salary-month": month,
                "salary-hour": hour,
                "currency": currency,
                "salary_type": salary_type,
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
    rebuild: bool = False,
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
    rebuild      -- ignore `links_file` entirely (re-scrape and do not write it)
    """
    store = LinkStore(links_file, load_existing=not rebuild, persist=not rebuild)
    if rebuild:
        print("Rebuild mode: ignoring previously scraped links")
    else:
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
            progress.stage("listing pages", total=max_pages)
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
                progress.advance(len(urls))

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
            details: list[dict | None]
            if with_details and raw_jobs:
                print(f"Fetching {len(raw_jobs)} detail pages...")
                progress.stage(f"details ({skipped:,} known)", total=len(raw_jobs))

                async def scrape_one(job: dict):
                    result = await fetch(context, job["url"], throttle, detail_handler)
                    progress.advance()
                    if result is not None:
                        store.add(job["url"])  # saved immediately, survives crashes
                    return result

                details = await asyncio.gather(*(scrape_one(j) for j in raw_jobs))
            else:
                for job in raw_jobs:
                    store.add(job["url"])
                details = [{} for _ in raw_jobs]
        finally:
            await context.close()

    listings = []
    failed_details = 0
    for job, detail in zip(raw_jobs, details):
        if detail is None:  # not stored -> will be retried next run
            failed_details += 1
            continue
        detail = detail or {}

        salary_month = job["salary-month"] or detail.get("salary_month")
        salary_hour = job["salary-hour"] or detail.get("salary_hour")
        currency = job["currency"] or detail.get("currency")
        salary_type = job["salary_type"] or detail.get("salary_type")

        technologies = list(dict.fromkeys(
            list(job.get("technologies") or []) + list(detail.get("skills") or [])
        ))

        work_mode = detail.get("work_mode")
        work_mode_raw = detail.get("work_mode_raw")
        if not work_mode and "remote" in str(job.get("city") or "").lower():
            work_mode = "remote"  # card location "Remote" (no detail data)
            work_mode_raw = work_mode_raw or job.get("city")

        listings.append(
            flat_listing(
                board="bulldogjobs",
                url=job["url"],
                company=detail.get("company") or "",
                offer_id=job["url"].rstrip("/").rsplit("/", 1)[-1],
                title=job["title"],
                city=job["city"],
                description=detail.get("description") or "",
                technologies=technologies,
                salary_month=salary_month,
                salary_hour=salary_hour,
                currency=currency,
                salary_type=salary_type,
                employment_types=detail.get("employment_types") or [],
                seniority=detail.get("seniority"),
                seniority_levels=detail.get("seniority_levels"),
                work_mode=work_mode,
                work_mode_raw=work_mode_raw,
                application_url=job["url"],
                benefits=detail.get("benefits") or [],
                date=detail.get("date"),
            )
        )
    if failed_details:
        print(f"{failed_details} listings failed and will be retried next run")
    return listings


if __name__ == "__main__":
    results = asyncio.run(fetch_bulldog_jobs(max_pages=2, rebuild=True))
    print(f"{len(results)} listings scraped")
    print(json.dumps(results[:3], indent=2, ensure_ascii=False))