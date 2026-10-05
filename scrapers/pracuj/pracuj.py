import asyncio
import csv
import os
import random
import re
import unicodedata
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

CSV_FILENAME = "pracuj_it_jobs.csv"
PROFILE_DIR = "./chrome_profile"   # persistent Chrome profile (keeps cookies/consent)
DEBUG_DIR = "./debug"              # screenshots of pages that failed to load

# Patchright works best with real Chrome, headful (or new-headless). Start with False.
HEADLESS = False
CONCURRENCY_LIMIT = 2              # keep low: many parallel tabs look like a bot
MAX_RETRIES = 3                    # attempts per page before giving up on it
HARD_PAGE_CAP = 500                # safety cap for sequential "until empty" mode
MAX_EMPTY_IN_A_ROW = 2             # sequential mode: stop after this many empty pages

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
    formatted = {
        "title": title,
        "city": city,
        "description": description,
        "technologies": ", ".join(tech_list),
        "salary-month": fmt_month,
        "salary-hour": fmt_hour,
        "date": date_str,
    }
    return raw, formatted


# --------------------------------------------------------------------------- #
# CSV persistence
# --------------------------------------------------------------------------- #
class CsvSink:
    """Append-only CSV writer with in-memory dedup. Validates the header up front."""

    def __init__(self, filename: str):
        self.filename = filename
        self.lock = asyncio.Lock()
        self.seen: set[str] = set()
        self.has_content = os.path.exists(filename) and os.path.getsize(filename) > 0

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
    """Scrapes one page with retries. Returns (formatted_jobs, soup) or ([], None) on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        # Small random delay so requests don't arrive in a perfectly regular pattern
        await asyncio.sleep(random.uniform(0.5, 1.5))

        result = await load_page_cards(context, target_url, page_num)
        if result is not None:
            soup, cards = result
            parsed = [p for p in (parse_card(c) for c in cards) if p]
            if parsed:
                raw_jobs = [r for r, _ in parsed]
                formatted_jobs = [f for _, f in parsed]
                added = await sink.save(raw_jobs)
                print(f"[Page {page_num}] Parsed {len(parsed)} cards, {added} new rows saved.")
                return formatted_jobs, soup

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
            print(f"\nExtracted {len(all_listings)} formatted listings total.")

        finally:
            await context.close()

    return all_listings


if __name__ == "__main__":
    listings = asyncio.run(fetch_pracuj_listings(max_pages=0))
    print(f"Sample formatted listing: {listings[0] if listings else 'No listings'}")