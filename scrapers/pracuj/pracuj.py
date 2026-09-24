import asyncio
import csv
import os
import re
import unicodedata
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup
from patchright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

CSV_FILENAME = "pracuj_it_jobs.csv"
CONCURRENCY_LIMIT = 5  # Number of parallel browser tabs

BASE_URL = (
    "https://it.pracuj.pl/praca?its=backend%2Cfrontend%2Cfullstack%2Cmobile%2Carchitecture"
    "%2Cdevops%2Cgamedev%2Cdata-analytics-and-bi%2Cbig-data-science%2Cembedded%2Ctesting"
    "%2Csecurity%2Chelpdesk%2Cproduct-management%2Cproject-management%2Cagile%2Cux-ui"
    "%2Cbusiness-analytics%2Csystem-analytics%2Csap-erp%2Cit-admin%2Cai-ml"
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

# Realistic browser settings for stealth headless mode
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)


def build_page_url(base_url: str, page_number: int) -> str:
    """Appends or updates the 'pn' (page number) query parameter in the target URL."""
    url_parts = list(urlparse(base_url))
    query = parse_qs(url_parts[4])
    query["pn"] = [str(page_number)]
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


def clean_text(text: str) -> str:
    """Normalizes non-breaking spaces (\xa0) and collapses internal whitespace runs."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(normalized.split())


def parse_salary_number(text: str) -> int:
    """Extracts numeric salary values from text string (returns average if range)."""
    cleaned = re.sub(r"\s+", "", text)
    nums = [int(n) for n in re.findall(r"\d+", cleaned)]
    if not nums:
        return 0
    if len(nums) == 1:
        return nums[0]
    return int(sum(nums) / len(nums))


def extract_max_pages(soup: BeautifulSoup) -> int:
    """Extracts total pages available from pagination DOM elements."""
    max_elem = soup.select_one('[data-test="top-pagination-max-page-number"]')
    if max_elem and max_elem.get_text(strip=True).isdigit():
        return int(max_elem.get_text(strip=True))

    page_btns = soup.select('[data-test^="bottom-pagination-button-page-"]')
    pages = [int(btn.get_text(strip=True)) for btn in page_btns if btn.get_text(strip=True).isdigit()]
    return max(pages) if pages else 1


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


def extract_raw_job_card(card: BeautifulSoup) -> dict:
    """Parses raw job card for CSV Data Lake persistence."""
    link_elem = card.select_one('a[data-test="link-offer"]')
    raw_href = link_elem["href"] if link_elem and link_elem.has_attr("href") else ""

    clean_url = raw_href.split("?")[0] if raw_href else ""
    match = re.search(r",oferta,(\d+)", raw_href)
    offer_id = match.group(1) if match else (clean_url.rstrip("/").split("/")[-1] if clean_url else "")

    title_elem = card.select_one('[data-test="offer-title"]')
    title = clean_text(title_elem.get_text(" ", strip=True)) if title_elem else ""

    city_elem = card.select_one('[data-test="text-region"]')
    city = clean_text(city_elem.get_text(" ", strip=True)) if city_elem else ""

    company_elem = card.select_one('[data-test="link-company-profile"]') or card.select_one('[data-test="text-company-name"]')
    company = clean_text(company_elem.get_text(" ", strip=True)) if company_elem else ""

    salary_month, salary_hour = "", ""
    salary_elem = card.select_one('[data-test="offer-salary"]')
    if salary_elem:
        salary_text = clean_text(salary_elem.get_text(" ", strip=True))
        if "mies." in salary_text:
            salary_month = salary_text
        elif "godz." in salary_text:
            salary_hour = salary_text

    desc_elem = (
        card.select_one('[data-test="section-short-description-projectDescriptionAccordion"] p')
        or card.select_one('[data-test="description-content-container"] p')
        or card.select_one('[data-test="seo-content-wrapper"] p')
    )
    description = clean_text(desc_elem.get_text(" ", strip=True)) if desc_elem else ""

    return {
        "offer_id": offer_id,
        "title": title,
        "city": city,
        "description": description,
        "salary-month": salary_month,
        "salary-hour": salary_hour,
        "company": company,
        "url": clean_url,
    }


def extract_formatted_job_card(card: BeautifulSoup) -> dict:
    """Parses job card to match requested target dictionary schema."""
    title_elem = card.select_one('[data-test="offer-title"]')
    title = clean_text(title_elem.get_text(" ", strip=True)) if title_elem else ""

    city_elem = card.select_one('[data-test="text-region"]')
    city = clean_text(city_elem.get_text(" ", strip=True)) if city_elem else ""

    desc_elem = (
        card.select_one('[data-test="section-short-description-projectDescriptionAccordion"] p')
        or card.select_one('[data-test="description-content-container"] p')
        or card.select_one('[data-test="seo-content-wrapper"] p')
    )
    description = clean_text(desc_elem.get_text(" ", strip=True)) if desc_elem else ""

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
    technologies = ", ".join(tech_list)

    salary_month, salary_hour = 0, 0
    salary_elem = card.select_one('[data-test="offer-salary"]')
    if salary_elem:
        salary_text = clean_text(salary_elem.get_text(" ", strip=True))
        parsed_val = parse_salary_number(salary_text)
        if "mies." in salary_text:
            salary_month = parsed_val
        elif "godz." in salary_text:
            salary_hour = parsed_val

    date_elem = (
        card.select_one('[data-test="text-added"]')
        or card.select_one('[data-test="text-added-date"]')
        or card.select_one('[data-test="text-date"]')
        or card.select_one('[data-test="offer-published-date"]')
    )
    date_str = clean_text(date_elem.get_text(" ", strip=True)) if date_elem else ""

    return {
        "title": title,
        "city": city,
        "description": description,
        "technologies": technologies,
        "salary-month": salary_month,
        "salary-hour": salary_hour,
        "date": date_str,
    }


def save_jobs_to_csv(filename: str, jobs: list[dict]):
    """Handles thread/async-safe Raw CSV persistence and deduplication."""
    file_has_content = os.path.exists(filename) and os.path.getsize(filename) > 0
    existing_ids = set()

    if file_has_content:
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDNAMES:
                raise SystemExit(f"Header Mismatch Error in '{filename}'. Expected: {FIELDNAMES}")
            for row in reader:
                identifier = row.get("offer_id") or row.get("url")
                if identifier:
                    existing_ids.add(identifier)

    new_jobs = []
    seen = set(existing_ids)
    for j in jobs:
        item_id = j.get("offer_id") or j.get("url")
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        new_jobs.append(j)

    if not new_jobs:
        return

    mode = "a" if file_has_content else "w"
    with open(filename, mode=mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_has_content:
            writer.writeheader()
        writer.writerows(new_jobs)

    print(f"Appended {len(new_jobs)} unique listings to '{filename}'.")


async def scrape_page(context, target_url: str, page_num: int, semaphore: asyncio.Semaphore, csv_lock: asyncio.Lock) -> list[dict]:
    """Processes a single page tab concurrently in headless mode."""
    async with semaphore:
        page = await context.new_page()
        formatted_jobs = []
        try:
            page_url = build_page_url(target_url, page_num)
            print(f"[Page {page_num}] Navigating -> {page_url}")

            await page.goto(page_url, wait_until="domcontentloaded", timeout=15000)
            await dismiss_overlays(page)

            try:
                await page.wait_for_selector('[data-test="default-offer"]', timeout=12000)
            except PlaywrightTimeoutError:
                print(f"[Page {page_num}] No cards found or timeout reached.")
                return []

            await auto_scroll(page)
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            cards = soup.select('[data-test="default-offer"]')

            raw_jobs = [extract_raw_job_card(c) for c in cards if extract_raw_job_card(c).get("title")]
            formatted_jobs = [extract_formatted_job_card(c) for c in cards if extract_formatted_job_card(c).get("title")]

            print(f"[Page {page_num}] Scraped {len(raw_jobs)} cards.")

            async with csv_lock:
                save_jobs_to_csv(CSV_FILENAME, raw_jobs)

        except Exception as e:
            print(f"[Page {page_num}] Unexpected error: {e}")
        finally:
            await page.close()

        return formatted_jobs


async def fetch_pracuj_listings(target_url: str = BASE_URL, max_pages: int = 0) -> list[dict]:
    """Orchestrates concurrent headless scraping with anti-detection evasions."""
    all_formatted_listings = []

    async with async_playwright() as p:
        # Launch Chromium with anti-bot evasion arguments for headless execution
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--headless=new",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-size=1920,1080",
            ],
        )

        try:
            # Create browser context with realistic viewport, locale, and user agent
            context = await browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="pl-PL",
                timezone_id="Europe/Warsaw",
            )

            # Mask navigator.webdriver flag in all new tabs
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            print("--- Processing Page 1 & Discovering Total Pages (Headless) ---")
            init_page = await context.new_page()
            page_1_url = build_page_url(target_url, 1)
            await init_page.goto(page_1_url, wait_until="domcontentloaded", timeout=15000)
            await dismiss_overlays(init_page)

            try:
                await init_page.wait_for_selector('[data-test="default-offer"]', timeout=12000)
            except PlaywrightTimeoutError:
                print("Failed to load initial page listings in headless mode.")
                return []

            await auto_scroll(init_page)
            content = await init_page.content()
            soup = BeautifulSoup(content, "html.parser")

            detected_total_pages = extract_max_pages(soup)
            print(f"Detected {detected_total_pages} total available pages.")

            cards = soup.select('[data-test="default-offer"]')
            p1_raw_jobs = [extract_raw_job_card(c) for c in cards if extract_raw_job_card(c).get("title")]
            p1_formatted_jobs = [extract_formatted_job_card(c) for c in cards if extract_formatted_job_card(c).get("title")]

            save_jobs_to_csv(CSV_FILENAME, p1_raw_jobs)
            all_formatted_listings.extend(p1_formatted_jobs)
            await init_page.close()

            if max_pages > 0:
                total_to_fetch = min(max_pages, detected_total_pages)
            else:
                total_to_fetch = detected_total_pages

            if total_to_fetch > 1:
                print(f"\n--- Spawning Concurrent Headless Tasks for Pages 2 to {total_to_fetch} ---")
                semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
                csv_lock = asyncio.Lock()

                tasks = [
                    scrape_page(context, target_url, page_num, semaphore, csv_lock)
                    for page_num in range(2, total_to_fetch + 1)
                ]

                results = await asyncio.gather(*tasks)
                for res in results:
                    all_formatted_listings.extend(res)

            print(f"\nSuccessfully extracted {len(all_formatted_listings)} formatted listings total!")

        finally:
            await browser.close()

    return all_formatted_listings


if __name__ == "__main__":
    listings = asyncio.run(fetch_pracuj_listings(max_pages=0))
    print(f"Sample formatted listing: {listings[0] if listings else 'No listings'}")