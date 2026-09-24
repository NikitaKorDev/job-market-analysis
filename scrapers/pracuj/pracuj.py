import csv
import os
import re
import unicodedata
from bs4 import BeautifulSoup
from patchright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CSV_FILENAME = "pracuj_it_jobs.csv"

# Comprehensive IT categories base URL
BASE_URL = (
    "https://it.pracuj.pl/praca?its=backend%2Cfrontend%2Cfullstack%2Cmobile%2Carchitecture"
    "%2Cdevops%2Cgamedev%2Cdata-analytics-and-bi%2Cbig-data-science%2Cembedded%2Ctesting"
    "%2Csecurity%2Chelpdesk%2Cproduct-management%2Cproject-management%2Cagile%2Cux-ui"
    "%2Cbusiness-analytics%2Csystem-analytics%2Csap-erp%2Cit-admin%2Cai-ml"
)

# Unified schema retaining core listing fields alongside deduplication trackers
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


def clean_text(text: str) -> str:
    """Normalizes non-breaking spaces (\xa0) and collapses internal whitespace runs."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(normalized.split())


def auto_scroll(page, max_scrolls: int = 40, step: int = 600):
    """Scrolls incrementally to trigger lazy hydration with a hard cap on iterations."""
    current_scroll = 0
    scroll_count = 0

    while scroll_count < max_scrolls:
        current_scroll += step
        page.evaluate(f"window.scrollTo(0, {current_scroll})")
        page.wait_for_timeout(250)
        scroll_count += 1

        total_height = page.evaluate("document.body.scrollHeight")
        if current_scroll >= total_height:
            break

    page.wait_for_timeout(800)


def dismiss_overlays(page):
    """Dismisses popups, cookie consent banners, and modal dialogs that intercept clicks."""
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    # Common cookie / popup / modal buttons on Pracuj.pl
    overlay_selectors = [
        '[data-test="button-submitCookie"]',
        'button[aria-label="Zamknij"]',
        'button[data-test="button-close"]',
        'button[data-test="button-dialog-close"]',
    ]

    for selector in overlay_selectors:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=300):
                btn.click(timeout=1000)
        except Exception:
            pass

    # Remove open <dialog> elements directly from DOM if they persist and intercept clicks
    try:
        page.evaluate("""() => {
            const openDialogs = document.querySelectorAll('dialog[open]');
            openDialogs.forEach(dialog => dialog.remove());
        }""")
    except Exception:
        pass


def extract_job_card(card: BeautifulSoup) -> dict:
    """Parses an individual job card with resilient selector fallbacks and robust offer_id extraction."""
    link_elem = card.select_one('a[data-test="link-offer"]')
    raw_href = link_elem["href"] if link_elem and link_elem.has_attr("href") else ""

    # Strip query parameters (e.g. ?s=...&searchId=...) and extract pure offer ID via regex
    clean_url = raw_href.split("?")[0] if raw_href else ""
    match = re.search(r",oferta,(\d+)", raw_href)
    if match:
        offer_id = match.group(1)
    else:
        offer_id = clean_url.rstrip("/").split("/")[-1] if clean_url else ""

    title_elem = card.select_one('[data-test="offer-title"]')
    title = clean_text(title_elem.get_text(" ", strip=True)) if title_elem else ""

    city_elem = card.select_one('[data-test="text-region"]')
    city = clean_text(city_elem.get_text(" ", strip=True)) if city_elem else ""

    company_elem = card.select_one('[data-test="link-company-profile"]') or card.select_one('[data-test="text-company-name"]')
    company = clean_text(company_elem.get_text(" ", strip=True)) if company_elem else ""

    # Monthly vs Hourly salary split
    salary_month, salary_hour = "", ""
    salary_elem = card.select_one('[data-test="offer-salary"]')
    if salary_elem:
        salary_text = clean_text(salary_elem.get_text(" ", strip=True))
        if "mies." in salary_text:
            salary_month = salary_text
        elif "godz." in salary_text:
            salary_hour = salary_text

    # Description selector fallback chain
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


def save_jobs_to_csv(filename: str, jobs: list[dict]):
    """Handles CSV persistence, schema header verification, and in-batch deduplication."""
    file_has_content = os.path.exists(filename) and os.path.getsize(filename) > 0
    existing_ids = set()

    # Verify header compatibility if CSV file exists and has content
    if file_has_content:
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDNAMES:
                raise SystemExit(
                    f"Header Mismatch Error: '{filename}' has field headers {reader.fieldnames}.\n"
                    f"Expected headers: {FIELDNAMES}.\n"
                    f"Please delete or rename the old CSV file before continuing."
                )
            for row in reader:
                identifier = row.get("offer_id") or row.get("url")
                if identifier:
                    existing_ids.add(identifier)

    # In-batch deduplication tracking seen IDs during parsing
    new_jobs = []
    seen = set(existing_ids)
    for j in jobs:
        item_id = j.get("offer_id") or j.get("url")
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        new_jobs.append(j)

    if not new_jobs and file_has_content:
        print("No new unique jobs found to append.")
        return

    mode = "a" if file_has_content else "w"
    with open(filename, mode=mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_has_content:
            writer.writeheader()
        writer.writerows(new_jobs)

    print(f"Successfully appended {len(new_jobs)} new listings to '{filename}'.")


def run_scraper(target_url: str = BASE_URL, max_pages: int = 3):
    """Runs job scraper across multiple pages with overlay dismissal and resilient click handling."""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--start-maximized"]
        )

        try:
            context = browser.new_context(no_viewport=True)
            page = context.new_page()

            print(f"Navigating to {target_url}...")
            page.goto(target_url, wait_until="domcontentloaded")

            current_page = 1
            while True:
                pages_label = f"of {max_pages}" if max_pages > 0 else "(Unlimited mode)"
                print(f"\n--- Processing Page {current_page} {pages_label} ---")

                # Clear modal popups or cookie overlays
                dismiss_overlays(page)

                # Explicitly wait for initial card hydration before scrolling
                print("Waiting for job cards to hydrate in DOM...")
                try:
                    page.wait_for_selector('[data-test="default-offer"]', timeout=15000)
                except PlaywrightTimeoutError:
                    print(f"No job cards found on page {current_page}. Reached end of results?")
                    break

                print("Scrolling page to trigger lazy loading...")
                auto_scroll(page)

                # Hand off hydrated DOM to BeautifulSoup
                soup = BeautifulSoup(page.content(), "html.parser")
                cards = soup.select('[data-test="default-offer"]')

                print(f"Extracted {len(cards)} raw job cards from page {current_page}.")

                jobs = []
                for card in cards:
                    job_data = extract_job_card(card)
                    if job_data["title"]:
                        jobs.append(job_data)

                save_jobs_to_csv(CSV_FILENAME, jobs)

                # Stop if max_pages limit is reached (ignored if max_pages == 0)
                if max_pages > 0 and current_page >= max_pages:
                    print(f"Reached requested limit of {max_pages} pages.")
                    break

                # Pagination: locate next page button
                next_btn = page.locator('[data-test="bottom-pagination-button-next"]')

                if next_btn.count() == 0 or not next_btn.is_visible():
                    print("Next page button not found. Reached last page.")
                    break

                # Check if next button is disabled
                is_disabled = page.evaluate(
                    """el => el.hasAttribute('disabled') || el.classList.contains('disabled')""",
                    next_btn.element_handle()
                )
                if is_disabled:
                    print("Next page button is disabled. Reached end of pagination.")
                    break

                # Attempt clean navigation click
                print("Navigating to next page...")
                dismiss_overlays(page)
                next_btn.scroll_into_view_if_needed()
                page.wait_for_timeout(500)

                try:
                    next_btn.click(timeout=5000)
                except PlaywrightTimeoutError:
                    print("Standard click blocked by overlay dialog. Applying force/JS click fallback...")
                    dismiss_overlays(page)
                    try:
                        next_btn.click(force=True, timeout=5000)
                    except Exception:
                        next_btn.evaluate("el => el.click()")

                # Allow domcontentloaded event after pagination click
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                page.wait_for_timeout(1500)

                current_page += 1

        finally:
            browser.close()

if __name__ == "__main__":
    run_scraper(max_pages=0)