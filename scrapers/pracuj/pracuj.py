import asyncio
import csv
import json
import os
import random
import re
import time
import unicodedata
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from patchright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from scrapers.common import flat_listing, normalize_salary_type, normalize_work_mode

CSV_FILENAME = "pracuj_it_jobs.csv"
PROFILE_DIR = "./chrome_profile"   # persistent Chrome profile (keeps cookies/consent)
DEBUG_DIR = "./debug"              # screenshots of pages that failed to load

# Patchright works best with real Chrome, headful (or new-headless). Start with False.
HEADLESS = False
CONCURRENCY_LIMIT = 2              # keep low: many parallel tabs look like a bot
MAX_RETRIES = 3                    # attempts per page before giving up on it
HARD_PAGE_CAP = 500                # safety cap for sequential "until empty" mode
MAX_EMPTY_IN_A_ROW = 2             # sequential mode: stop after this many empty pages
HOURS_PER_MONTH = 168

# Detail pages are plain HTTP (curl_cffi), not browser tabs.
DETAIL_CONCURRENCY = 12            # was 4. Lower it if you start seeing 403/429.
DETAIL_RETRIES = 2
DETAIL_TIMEOUT = 15                # was 20; a hung request no longer blocks a slot for long
DETAIL_RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
DETAIL_CACHE = "./pracuj_detail_cache.jsonl"
DETAIL_CACHE_TTL_DAYS = 7          # cached details older than this are re-fetched
DETAIL_SECTIONS = {
    "about-project",
    "responsibilities",
    "requirements-expected",
    "requirements-optional",
    "offered",
    "training-space",
    "work-organization-work-style",
    "work-organization-work-time",
    "work-organization-work-mode",
    "technologies-expected",
    "technologies-optional",
}
CURRENCY_RE = re.compile(r"\b(PLN|EUR|USD|GBP|CHF|CZK)\b|zł", re.I)

BASE_URL = (
    "https://it.pracuj.pl/praca?its=backend%2Cfrontend%2Cfullstack%2Cmobile%2Carchitecture%2Cdevops%2Cgamedev%2Cdata-analytics-and-bi%2Cbig-data-science%2Cux-ui%2Cbusiness-analytics%2Cagile%2Cproject-management%2Cproduct-management%2Cai-ml%2Cit-admin%2Csecurity%2Chelpdesk%2Ctesting%2Cembedded%2Csystem-analytics%2Csap-erp"
)

# Raw Data Lake CSV Schema
FIELDNAMES = [
    "offer_id",
    "title",
    "city",
    "description",
    "salary-month",
    "salary-hour",
    "company",
    "url",
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def build_page_url(base_url: str, page_number: int) -> str:
    """Appends or updates the 'pn' (page number) query parameter in the target URL."""
    url_parts = list(urlparse(base_url))
    query = parse_qs(url_parts[4])
    query["pn"] = [str(page_number)]
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


def clean_text(text: str) -> str:
    """Normalizes non-breaking spaces and collapses internal whitespace runs."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(normalized.split())


def parse_salary_number(text: str) -> int:
    """Extracts numeric salary values from text (returns average if a range)."""
    cleaned = re.sub(r"\s+", "", text)
    nums = [int(n) for n in re.findall(r"\d+", cleaned)]
    if not nums:
        return 0
    if len(nums) == 1:
        return nums[0]
    return int(sum(nums) / len(nums))


def parse_currency(text: str) -> str:
    if not text:
        return ""
    match = CURRENCY_RE.search(text)
    if not match:
        return ""
    token = match.group(0)
    return "PLN" if token.lower() == "zł" else token.upper()


def _next_data(html: str) -> dict:
    match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S | re.I
    )
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def _offer_data(html: str) -> dict:
    payload = _next_data(html)
    try:
        queries = payload["props"]["pageProps"]["dehydratedState"]["queries"]
        return queries[0]["state"]["data"] or {}
    except (KeyError, IndexError, TypeError):
        return {}


def _salary_from_employment(employment: dict) -> tuple[int | None, int | None, str | None, str | None]:
    """Pick the first contract that discloses a salary and normalize it."""
    for contract in employment.get("typesOfContracts") or []:
        if not isinstance(contract, dict):
            continue
        salary = contract.get("salary")
        if not isinstance(salary, dict):
            continue
        values = [
            value
            for value in (salary.get("from"), salary.get("to"))
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0
        ]
        if not values:
            continue
        midpoint = sum(values) / len(values)
        unit = str((((salary.get("timeUnit") or {}).get("shortForm") or {}).get("name") or "")).lower()
        if "godz" in unit or "/h" in unit:
            month, hour = midpoint * HOURS_PER_MONTH, midpoint
        else:
            month, hour = midpoint, midpoint / HOURS_PER_MONTH
        kind = (salary.get("salaryKind") or {}).get("name")
        currency = (salary.get("currency") or {}).get("code")
        return int(round(month)), int(round(hour)), currency, normalize_salary_type(kind)
    return None, None, None, None


def _benefits_from_sections(sections: list) -> list[str]:
    for section in sections:
        if not isinstance(section, dict) or section.get("sectionType") != "benefits":
            continue
        elements = section.get("textElements")
        if isinstance(elements, list):
            values = [clean_text(str(item)) for item in elements]
            values = [value for value in values if value]
            if values:
                return list(dict.fromkeys(values))
        text = clean_text(section.get("plainText") or "")
        if text:
            return [part.strip() for part in re.split(r"[,;\n]", text) if part.strip()]
    return []


def _detail_from_html(html: str) -> dict:
    """Extract full description, technologies, employer, salary and metadata."""
    data = _offer_data(html)
    attributes = data.get("attributes") or {}
    sections = data.get("textSections") or []
    posting = _jobposting_jsonld(html) or {}

    chunks = []
    if attributes.get("description"):
        chunks.append(clean_text(attributes["description"]))

    technologies = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_type = str(section.get("sectionType") or "")
        text = clean_text(section.get("plainText") or "")
        if section_type in DETAIL_SECTIONS and text:
            chunks.append(text)
        if section_type in ("technologies-expected", "technologies-optional"):
            for item in section.get("textElements") or []:
                item = clean_text(str(item))
                if item and item not in technologies:
                    technologies.append(item)

    description = "\n\n".join(dict.fromkeys(chunk for chunk in chunks if chunk))

    date_posted = str(posting.get("datePosted") or attributes.get("datePosted") or "")[:10]

    organization = posting.get("hiringOrganization") or {}
    company = clean_text(str(attributes.get("displayEmployerName") or ""))
    if not company and isinstance(organization, dict):
        company = clean_text(str(organization.get("name") or ""))

    employment = attributes.get("employment") or {}
    salary_month, salary_hour, salary_currency, salary_type = _salary_from_employment(employment)

    salary = posting.get("baseSalary") or {}
    currency = salary_currency
    if not currency and isinstance(salary, dict):
        currency = str(salary.get("currency") or "").upper() or None

    contract_names = [
        contract.get("name")
        for contract in employment.get("typesOfContracts") or []
        if isinstance(contract, dict)
    ]
    schedule_names = [
        schedule.get("name")
        for schedule in employment.get("workSchedules") or []
        if isinstance(schedule, dict)
    ]
    employment_types = [
        name for name in dict.fromkeys(contract_names + schedule_names) if name
    ]

    levels = employment.get("positionLevels") or []
    seniority = levels[0].get("name") if levels and isinstance(levels[0], dict) else None

    if employment.get("entirelyRemoteWork"):
        work_mode = "remote"
    else:
        mode_names = [
            mode.get("name")
            for mode in employment.get("workModes") or []
            if isinstance(mode, dict)
        ]
        work_mode = normalize_work_mode(", ".join(name for name in mode_names if name))

    applying = attributes.get("applying") or {}

    return {
        "description": description,
        "technologies": technologies,
        "company": company,
        "url": str(attributes.get("offerAbsoluteUrl") or ""),
        "date": date_posted,
        "currency": currency,
        "salary_month": salary_month,
        "salary_hour": salary_hour,
        "salary_type": salary_type,
        "employment_types": employment_types,
        "seniority": seniority,
        "work_mode": work_mode,
        "application_url": applying.get("applyUrl") or "",
        "benefits": _benefits_from_sections(sections),
    }


def _jobposting_jsonld(html: str) -> dict | None:
    """Find the schema.org JobPosting block on a pracuj offer page."""
    for block in re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S | re.I
    ):
        try:
            payload = json.loads(block)
        except json.JSONDecodeError:
            continue
        stack = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if item.get("@type") == "JobPosting":
                    return item
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
    return None


# --------------------------------------------------------------------------- #
# Detail enrichment (shared async session, dedupe, disk cache)
# --------------------------------------------------------------------------- #
def _load_detail_cache() -> dict[str, dict]:
    """url -> parsed detail dict, for entries newer than the TTL."""
    cache: dict[str, dict] = {}
    if not os.path.exists(DETAIL_CACHE):
        return cache
    cutoff = time.time() - DETAIL_CACHE_TTL_DAYS * 86400
    with open(DETAIL_CACHE, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # half-written last line after a crash
            if rec.get("url") and rec.get("detail") and rec.get("ts", 0) >= cutoff:
                cache[rec["url"]] = rec["detail"]
    return cache


def _apply_detail(listing: dict, detail: dict) -> None:
    """Merge a parsed detail-page record into a listing, in place."""
    if detail["description"]:
        listing["description"] = detail["description"]
    if detail["technologies"]:
        listing["technologies_list"] = detail["technologies"]
        listing["technologies"] = ", ".join(detail["technologies"])
    if detail["company"]:
        listing["company"] = detail["company"]
    if detail["url"]:
        listing["url"] = detail["url"]
    if detail["date"]:
        listing["date"] = detail["date"]
    if detail["currency"]:
        listing["currency"] = detail["currency"]
    if detail["salary_month"] is not None:
        listing["salary-month"] = detail["salary_month"]
    if detail["salary_hour"] is not None:
        listing["salary-hour"] = detail["salary_hour"]
    if detail["salary_type"]:
        listing["salary_type"] = detail["salary_type"]
    if detail["employment_types"]:
        listing["employment_types"] = detail["employment_types"]
    if detail["seniority"]:
        listing["seniority"] = detail["seniority"]
    if detail["work_mode"]:
        listing["work_mode"] = detail["work_mode"]
    if detail["application_url"]:
        listing["application_url"] = detail["application_url"]
    if detail["benefits"]:
        listing["benefits"] = detail["benefits"]


async def _fetch_detail(session: AsyncSession, url: str) -> dict | None:
    """Fetch + parse one detail page over a shared, connection-reusing session."""
    for attempt in range(DETAIL_RETRIES + 1):
        retry_after = 0.0
        try:
            response = await session.get(url, timeout=DETAIL_TIMEOUT)
            if response.status_code == 200:
                # Parsing a large page is CPU work: keep it off the event loop.
                return await asyncio.to_thread(_detail_from_html, response.text)
            if response.status_code not in DETAIL_RETRY_STATUSES:
                return None  # 404/410 etc: the offer is gone, retrying is pointless
            header = response.headers.get("Retry-After", "")
            retry_after = float(header) if header.isdigit() else 0.0
        except Exception:
            pass

        if attempt < DETAIL_RETRIES:
            await asyncio.sleep(max(retry_after, 1.5 * (attempt + 1)) + random.random())
    return None


async def enrich_details(listings: list[dict]) -> None:
    """Fill detail-page fields in place. Dedupes URLs, uses a disk cache, one shared session."""
    by_url: dict[str, list[dict]] = {}
    for listing in listings:
        url = listing.get("url") or ""
        if url:
            by_url.setdefault(url, []).append(listing)

    cache = _load_detail_cache()
    pending: list[str] = []
    for url, group in by_url.items():
        detail = cache.get(url)
        if detail:
            for listing in group:
                _apply_detail(listing, detail)
        else:
            pending.append(url)

    print(
        f"[pracuj] details: {len(by_url)} unique urls "
        f"({len(listings) - len(by_url)} duplicates), "
        f"{len(by_url) - len(pending)} from cache, {len(pending)} to fetch"
    )
    if not pending:
        return

    semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
    total = len(pending)
    done = failed = 0
    started = time.monotonic()

    async with AsyncSession(
        impersonate="chrome124",
        headers={"Accept-Language": "pl-PL,pl;q=0.9"},
        max_clients=DETAIL_CONCURRENCY,
    ) as session:
        with open(DETAIL_CACHE, "a", encoding="utf-8") as cache_fh:

            async def one(url: str) -> None:
                nonlocal done, failed
                async with semaphore:
                    detail = await _fetch_detail(session, url)

                done += 1
                if detail:
                    # The cache is keyed by the ORIGINAL url (before _apply_detail may rewrite it).
                    cache_fh.write(
                        json.dumps({"url": url, "ts": time.time(), "detail": detail}, ensure_ascii=False) + "\n"
                    )
                    cache_fh.flush()  # a crash at 90% keeps the 90%
                    for listing in by_url[url]:
                        _apply_detail(listing, detail)
                else:
                    failed += 1

                if done % 100 == 0 or done == total:
                    rate = done / max(time.monotonic() - started, 1e-6)
                    eta = (total - done) / rate if rate else 0
                    print(
                        f"[pracuj] details {done}/{total} "
                        f"({rate:.1f}/s, ~{eta / 60:.1f} min left, {failed} failed)"
                    )

            await asyncio.gather(*(one(url) for url in pending))


# --------------------------------------------------------------------------- #
# Results-page helpers
# --------------------------------------------------------------------------- #
def extract_max_pages(soup: BeautifulSoup) -> int | None:
    """
    Returns the total page count, or None if it can't be determined reliably.

    Only the dedicated "max page" element is trusted. The bottom pagination
    buttons usually show just a window of pages (e.g. 1..5), so using them
    would silently under-report the total.
    """
    max_elem = soup.select_one('[data-test="top-pagination-max-page-number"]')
    if max_elem and max_elem.get_text(strip=True).isdigit():
        return int(max_elem.get_text(strip=True))
    return None


async def auto_scroll(page, max_scrolls: int = 12, step: int = 1200):
    """Fast incremental scrolling to trigger lazy hydration."""
    current_scroll = 0
    for _ in range(max_scrolls):
        current_scroll += step
        await page.evaluate(f"window.scrollTo(0, {current_scroll})")
        await page.wait_for_timeout(80)

        total_height = await page.evaluate("document.body.scrollHeight")
        if current_scroll >= total_height:
            break

    await page.wait_for_timeout(200)


async def dismiss_overlays(page):
    """Dismisses popups and cookie consent banners."""
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass

    overlay_selector = (
        '[data-test="button-submitCookie"], '
        'button[aria-label="Zamknij"], '
        'button[data-test="button-close"], '
        'button[data-test="button-dialog-close"]'
    )

    try:
        btn = page.locator(overlay_selector).first
        if await btn.is_visible(timeout=200):
            await btn.click(timeout=400)
    except Exception:
        pass

    try:
        await page.evaluate("""() => {
            document.querySelectorAll('dialog[open]').forEach(dialog => dialog.remove());
        }""")
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Card parsing (each card is parsed ONCE and yields both representations)
# --------------------------------------------------------------------------- #
def _short_description(card: BeautifulSoup) -> str:
    desc_elem = (
        card.select_one('[data-test="section-short-description-projectDescriptionAccordion"] p')
        or card.select_one('[data-test="description-content-container"] p')
        or card.select_one('[data-test="seo-content-wrapper"] p')
    )
    return clean_text(desc_elem.get_text(" ", strip=True)) if desc_elem else ""


def parse_card(card: BeautifulSoup) -> tuple[dict, dict] | None:
    """Parses one job card into (raw_row_for_csv, formatted_dict). None if no title."""
    title_elem = card.select_one('[data-test="offer-title"]')
    title = clean_text(title_elem.get_text(" ", strip=True)) if title_elem else ""
    if not title:
        return None

    city_elem = card.select_one('[data-test="text-region"]')
    city = clean_text(city_elem.get_text(" ", strip=True)) if city_elem else ""

    description = _short_description(card)

    # URL / offer id
    link_elem = card.select_one('a[data-test="link-offer"]')
    raw_href = link_elem["href"] if link_elem and link_elem.has_attr("href") else ""
    clean_url = raw_href.split("?")[0] if raw_href else ""
    match = re.search(r",oferta,(\d+)", raw_href)
    offer_id = match.group(1) if match else (clean_url.rstrip("/").split("/")[-1] if clean_url else "")

    # Company
    company_elem = (
        card.select_one('[data-test="link-company-profile"]')
        or card.select_one('[data-test="text-company-name"]')
    )
    company = clean_text(company_elem.get_text(" ", strip=True)) if company_elem else ""

    # Salary
    salary_text = ""
    salary_elem = card.select_one('[data-test="offer-salary"]')
    if salary_elem:
        salary_text = clean_text(salary_elem.get_text(" ", strip=True))

    raw_month = salary_text if "mies." in salary_text else ""
    raw_hour = salary_text if "godz." in salary_text else ""

    parsed_val = parse_salary_number(salary_text) if salary_text else 0
    fmt_month = parsed_val if "mies." in salary_text else 0
    fmt_hour = parsed_val if "godz." in salary_text else 0

    # Technologies
    tech_elems = card.select(
        '[data-test="item-technologies"], '
        '[data-test="chip-technology"], '
        '[data-test="item-technology"], '
        '[data-test="technologies-list"] item, '
        '[data-test="technologies-list"] span'
    )
    tech_list = []
    for elem in tech_elems:
        t = clean_text(elem.get_text(" ", strip=True))
        if t and t not in tech_list:
            tech_list.append(t)

    # Date
    date_elem = (
        card.select_one('[data-test="text-added"]')
        or card.select_one('[data-test="text-added-date"]')
        or card.select_one('[data-test="text-date"]')
        or card.select_one('[data-test="offer-published-date"]')
    )
    date_str = clean_text(date_elem.get_text(" ", strip=True)) if date_elem else ""

    raw = {
        "offer_id": offer_id,
        "title": title,
        "city": city,
        "description": description,
        "salary-month": raw_month,
        "salary-hour": raw_hour,
        "company": company,
        "url": clean_url,
    }
    # Intermediate record; build_listings() turns it into a flat dictionary.
    formatted = {
        "title": title,
        "city": city,
        "description": description,
        "technologies": ", ".join(tech_list),
        "salary-month": fmt_month,
        "salary-hour": fmt_hour,
        "currency": parse_currency(salary_text),
        "salary_type": normalize_salary_type(salary_text),
        "date": date_str,
    }
    return raw, formatted


def build_listings(parsed: list[tuple[dict, dict]]) -> list[dict]:
    """Pair each raw CSV row with its card record and emit flat listings."""
    listings = []
    for raw, formatted in parsed:
        listings.append(
            flat_listing(
                board="pracuj",
                url=str(raw.get("url") or ""),
                company=str(raw.get("company") or ""),
                offer_id=raw.get("offer_id") or "",
                title=formatted.get("title") or "",
                city=formatted.get("city") or "",
                description=formatted.get("description") or "",
                technologies=formatted.get("technologies") or [],
                salary_month=formatted.get("salary-month"),
                salary_hour=formatted.get("salary-hour"),
                currency=formatted.get("currency"),
                salary_type=formatted.get("salary_type"),
                date=formatted.get("date"),
                application_url=raw.get("url"),
            )
        )
    return listings


# --------------------------------------------------------------------------- #
# CSV persistence
# --------------------------------------------------------------------------- #
class CsvSink:
    """Append-only CSV writer with in-memory dedup. Validates the header up front."""

    def __init__(self, filename: str):
        self.filename = filename
        self.lock = asyncio.Lock()
        self.seen: set[str] = set()
        self.has_content = os.path.exists(filename) and os.path.getsize(filename) > 0 # If file exists in path and its size is 0 > === its not empty

        if self.has_content:
            with open(filename, mode="r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames != FIELDNAMES:
                    # Raised before any browser work starts, so it can't kill a gather() midway.
                    raise SystemExit(
                        f"Header mismatch in '{filename}'. Expected: {FIELDNAMES}, got: {reader.fieldnames}"
                    )
                for row in reader:
                    ident = row.get("offer_id") or row.get("url")
                    if ident:
                        self.seen.add(ident)

    async def save(self, jobs: list[dict]) -> int:
        async with self.lock:
            new_jobs = []
            for j in jobs:
                ident = j.get("offer_id") or j.get("url")
                if not ident or ident in self.seen:
                    continue
                self.seen.add(ident)
                new_jobs.append(j)

            if not new_jobs:
                return 0

            mode = "a" if self.has_content else "w"
            with open(self.filename, mode=mode, encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                if not self.has_content:
                    writer.writeheader()
                writer.writerows(new_jobs)
            self.has_content = True
            return len(new_jobs)


# --------------------------------------------------------------------------- #
# Scraping
# --------------------------------------------------------------------------- #
async def load_page_cards(context, target_url: str, page_num: int):
    """
    Loads one results page. Returns (soup, cards) on success, or None if no cards
    were found (timeout / block / end of results). Logs enough to tell which.
    """
    page = await context.new_page()
    try:
        page_url = build_page_url(target_url, page_num)
        print(f"[Page {page_num}] Navigating -> {page_url}")

        await page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        await dismiss_overlays(page)

        try:
            await page.wait_for_selector('[data-test="default-offer"]', timeout=15000)
        except PlaywrightTimeoutError:
            title = await page.title()
            print(f"[Page {page_num}] No cards. title={title!r} final_url={page.url}")
            os.makedirs(DEBUG_DIR, exist_ok=True)
            try:
                await page.screenshot(path=os.path.join(DEBUG_DIR, f"page_{page_num}.png"))
            except Exception:
                pass
            return None

        await auto_scroll(page)
        soup = BeautifulSoup(await page.content(), "html.parser")
        cards = soup.select('[data-test="default-offer"]')
        return soup, cards

    except Exception as e:
        print(f"[Page {page_num}] Error while loading: {type(e).__name__}: {e}")
        return None
    finally:
        await page.close()


async def scrape_page(context, target_url: str, page_num: int, sink: CsvSink):
    """Scrapes one page with retries. Returns (flat listings, soup) or ([], None) on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        # Small random delay so requests don't arrive in a perfectly regular pattern
        await asyncio.sleep(random.uniform(0.5, 1.5))

        result = await load_page_cards(context, target_url, page_num)
        if result is not None:
            soup, cards = result
            parsed = [p for p in (parse_card(c) for c in cards) if p]
            if parsed:
                raw_jobs = [r for r, _ in parsed]
                added = await sink.save(raw_jobs)
                print(f"[Page {page_num}] Parsed {len(parsed)} cards, {added} new rows saved.")
                return build_listings(parsed), soup

        if attempt < MAX_RETRIES:
            backoff = 2 * attempt + random.random()
            print(f"[Page {page_num}] Attempt {attempt}/{MAX_RETRIES} failed, retrying in {backoff:.1f}s")
            await asyncio.sleep(backoff)

    print(f"[Page {page_num}] FAILED after {MAX_RETRIES} attempts")
    return [], None


async def fetch_pracuj_listings(target_url: str = BASE_URL, max_pages: int = 0) -> list[dict]:
    """Scrapes all result pages. max_pages=0 means 'everything'."""
    sink = CsvSink(CSV_FILENAME)  # validates CSV header before any browser work
    all_listings: list[dict] = []
    failed_pages: list[int] = []

    async with async_playwright() as p:
        # Patchright-recommended setup: real Chrome, persistent profile, no custom UA,
        # no extra evasion args, no navigator.webdriver patching.
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            channel="chrome",
            headless=HEADLESS,
            no_viewport=True,
            locale="pl-PL",
            timezone_id="Europe/Warsaw",
        )

        try:
            print("--- Processing Page 1 & discovering total pages ---")
            page1_jobs, soup1 = await scrape_page(context, target_url, 1, sink)
            if not page1_jobs:
                print("Failed to load page 1. Check ./debug/page_1.png and the log above.")
                return []
            all_listings.extend(page1_jobs)

            detected_total = extract_max_pages(soup1)
            limit = max_pages if max_pages and max_pages > 0 else None

            if detected_total is not None:
                total_to_fetch = min(limit, detected_total) if limit else detected_total
                print(f"Detected {detected_total} total pages; fetching {total_to_fetch}.")

                if total_to_fetch > 1:
                    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

                    async def worker(n: int):
                        async with semaphore:
                            jobs, _ = await scrape_page(context, target_url, n, sink)
                            return n, jobs

                    results = await asyncio.gather(*(worker(n) for n in range(2, total_to_fetch + 1)))
                    for n, jobs in sorted(results):
                        if jobs:
                            all_listings.extend(jobs)
                        else:
                            failed_pages.append(n)
            else:
                # Page count unknown: walk pages sequentially until results run out.
                print("Could not detect total pages; paging sequentially until empty.")
                cap = min(limit, HARD_PAGE_CAP) if limit else HARD_PAGE_CAP
                empty_in_a_row = 0
                for n in range(2, cap + 1):
                    jobs, _ = await scrape_page(context, target_url, n, sink)
                    if jobs:
                        all_listings.extend(jobs)
                        empty_in_a_row = 0
                    else:
                        empty_in_a_row += 1
                        failed_pages.append(n)
                        if empty_in_a_row >= MAX_EMPTY_IN_A_ROW:
                            print(f"{MAX_EMPTY_IN_A_ROW} empty pages in a row; assuming end of results.")
                            break

            if failed_pages:
                print(f"\nPages with no results after retries: {failed_pages}")
            print(f"\nExtracted {len(all_listings)} listings total.")

        finally:
            await context.close()

    print(f"[pracuj] Fetching detail pages for {len(all_listings)} listings...")
    await enrich_details(all_listings)

    return all_listings


if __name__ == "__main__":
    listings = asyncio.run(fetch_pracuj_listings(max_pages=0))
    print(f"Retrieved {len(listings)} listings")
    print(f"Sample: {listings[0] if listings else 'No listings'}")