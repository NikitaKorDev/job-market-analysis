"""NoFluffJobs scraper (sync Patchright).

Instead of guessing the search API's request format, we let the real page make
ONE "load more" request, capture its exact URL/headers/body, and replay it for
every other page in parallel (in-page fetch, so Cloudflare cookies just work).
Offer details come from /api/posting/<slug>, also in parallel.
If anything fails, we fall back to DOM parsing (click "load more" until the
list is exhausted, then read the cards; descriptions come from each offer's
JSON-LD when available).

    pip install patchright
    patchright install chrome
"""
import html
import json
import math
import re
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from scrapers.common import (
    flat_listing,
    normalize_salary_type,
    normalize_work_mode,
)

BASE_URL = "https://nofluffjobs.com/pl/artificial-intelligence?criteria=category%3Dbackend,data,devops,fullstack"

SITE_ROOT = "https://nofluffjobs.com"
API_POSTING = f"{SITE_ROOT}/api/posting/"
CARD = "a.posting-list-item"
LOAD_MORE = "button[nfjloadmore]"
BATCH = 15                # parallel in-page requests per round
HOURS_PER_MONTH = 160     # used to derive salary-hour from salary-month
USER_DATA_DIR = ".patchright_profile"


class _ApiError(Exception):
    pass


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _clean(text) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _html_to_text(value) -> str:
    if not value or not isinstance(value, str):
        return ""
    s = re.sub(r"<\s*(?:br|/p|/li|/ul|/ol|/h\d|/div)\s*/?>", "\n", value, flags=re.I)
    s = html.unescape(re.sub(r"<[^>]+>", " ", s))
    return "\n".join(_clean(line) for line in s.splitlines() if line.strip())


def _date(ms) -> str:
    if not isinstance(ms, (int, float)) or ms <= 0:
        return ""
    if ms > 1e11:  # milliseconds
        ms /= 1000
    return datetime.fromtimestamp(ms, tz=timezone.utc).date().isoformat()


def _city(posting: dict) -> str:
    loc = posting.get("location") or {}
    cities = [_clean(p.get("city")) for p in loc.get("places") or [] if isinstance(p, dict)]
    cities = [c for c in cities if c]
    if loc.get("fullyRemote") and not any(c.lower() == "remote" for c in cities):
        cities.append("Remote")
    return ", ".join(dict.fromkeys(cities))


CONTRACT_NAMES = {
    "b2b": "B2B",
    "permanent": "Umowa o pracę",
    "mandate": "Umowa zlecenie",
    "internship": "Staż",
    "contract": "Kontrakt",
}


def _salary(posting: dict, period: str = "month", detail: dict | None = None):
    """(salary_month, salary_hour, currency, salary_type).

    Prefers the search posting's salary, then the detail API's originalSalary.
    """
    s = posting.get("salary") or {}
    currency = s.get("currency") or None
    values = [v for v in (s.get("from"), s.get("to")) if isinstance(v, (int, float)) and v > 0]
    contract_type = None

    if not values:
        original = ((detail or {}).get("essentials") or {}).get("originalSalary") or {}
        currency = original.get("currency") or currency
        for key, spec in (original.get("types") or {}).items():
            if not isinstance(spec, dict):
                continue
            values = [v for v in (spec.get("range") or []) if isinstance(v, (int, float)) and v > 0]
            if values:
                contract_type = str(key).lower()
                period = str(spec.get("period") or period).lower()
                break

    if not values:
        return None, None, currency, None

    midpoint = sum(values) / len(values)
    period = period.lower()
    if period.startswith("hour"):
        month, hour = midpoint * HOURS_PER_MONTH, midpoint
    elif period.startswith("year"):
        month = midpoint / 12
        hour = month / HOURS_PER_MONTH
    else:
        month, hour = midpoint, midpoint / HOURS_PER_MONTH

    if contract_type == "b2b":
        salary_type = "netto"  # Polish B2B rates are net + VAT
    elif contract_type in ("permanent", "mandate", "internship"):
        salary_type = "brutto"
    else:
        salary_type = None
    return int(round(month)), int(round(hour)), currency, salary_type


def _technologies(posting: dict, detail: dict | None) -> str:
    names = [posting.get("technology") or ""]
    musts = ((detail or {}).get("requirements") or {}).get("musts") or []
    names += [m.get("value", "") for m in musts if isinstance(m, dict)]
    return ", ".join(dict.fromkeys(_clean(n) for n in names if _clean(n)))


def _description(detail: dict | None, posting_jsonld: dict | None = None) -> str:
    chunks = []
    if detail:
        chunks = [
            _html_to_text((detail.get("details") or {}).get("description")),
            _html_to_text((detail.get("requirements") or {}).get("description")),
        ]
        for task in (detail.get("specs") or {}).get("dailyTasks") or []:
            chunks.append(_html_to_text(task if isinstance(task, str) else (task or {}).get("value")))
    description = "\n".join(c for c in chunks if c)
    if not description and isinstance(posting_jsonld, dict):
        description = _html_to_text(posting_jsonld.get("description"))
    return description


def _posting_url(posting: dict) -> str:
    """Canonical offer URL from a search posting or API detail object."""
    slug = str(posting.get("url") or posting.get("id") or "").strip()
    for key in ("postingUrl", "defaultUrl"):
        value = str(posting.get(key) or "").strip()
        if value:
            return urljoin(SITE_ROOT, value)
    return f"{SITE_ROOT}/pl/job/{slug}" if slug else ""


def _company(posting: dict, detail: dict | None, posting_jsonld: dict | None) -> str:
    for source in (detail, posting):
        company = (source or {}).get("company")
        if isinstance(company, dict):
            name = _clean(company.get("name"))
            if name:
                return name
        elif isinstance(company, str) and company.strip():
            return _clean(company)
    if isinstance(posting_jsonld, dict):
        organization = posting_jsonld.get("hiringOrganization") or {}
        if isinstance(organization, dict):
            return _clean(organization.get("name"))
    return ""


def _technologies_list(posting: dict, detail: dict | None) -> list[str]:
    raw = _technologies(posting, detail)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _date_from_jsonld(posting_jsonld: dict | None) -> str:
    if isinstance(posting_jsonld, dict) and posting_jsonld.get("datePosted"):
        return str(posting_jsonld["datePosted"])[:10]
    return ""


def _employment_types(detail: dict | None) -> list[str]:
    original = ((detail or {}).get("essentials") or {}).get("originalSalary") or {}
    keys = [str(key) for key in (original.get("types") or {})]
    return [CONTRACT_NAMES.get(key.lower(), key) for key in keys]


def _seniority(detail: dict | None) -> str | None:
    levels = ((detail or {}).get("basics") or {}).get("seniority") or []
    values = [_clean(level) for level in levels if _clean(level)]
    return ", ".join(dict.fromkeys(values)) or None


def _work_mode(posting: dict, detail: dict | None, posting_jsonld: dict | None) -> str | None:
    location = (detail or {}).get("location") or posting.get("location") or {}
    if location.get("fullyRemote"):
        return "remote"
    cities = [
        _clean(place.get("city"))
        for place in location.get("places") or []
        if isinstance(place, dict)
    ]
    if any(city.lower() == "remote" for city in cities):
        return "remote"
    if isinstance(posting_jsonld, dict):
        if str(posting_jsonld.get("jobLocationType") or "").upper() == "TELECOMMUTE":
            return "remote"
    return normalize_work_mode(", ".join(city for city in cities if city))


def _benefits(detail: dict | None, posting_jsonld: dict | None = None) -> list[str]:
    values = ((detail or {}).get("benefits") or {}).get("benefits") or []
    benefits = [_clean(value) for value in values if _clean(value)]
    if not benefits and isinstance(posting_jsonld, dict):
        raw = posting_jsonld.get("jobBenefits")
        if isinstance(raw, list):
            benefits = [_clean(value) for value in raw if _clean(value)]
    return benefits


def _to_listing(
    posting: dict,
    detail: dict | None,
    posting_jsonld: dict | None = None,
    period: str = "month",
) -> dict:
    month, hour, currency, salary_type = _salary(posting, period, detail)
    employment_types = _employment_types(detail)
    if salary_type is None and employment_types:
        lowered = [name.lower() for name in employment_types]
        if all("b2b" in name for name in lowered):
            salary_type = "netto"
        elif all(
            any(key in name for key in ("umowa o pracę", "umowa zlecenie", "staż"))
            for name in lowered
        ):
            salary_type = "brutto"
    date = _date(posting.get("posted")) or _date_from_jsonld(posting_jsonld)
    url = _posting_url(posting)
    return flat_listing(
        board="nofluff",
        url=url,
        company=_company(posting, detail, posting_jsonld),
        offer_id=posting.get("id") or posting.get("url") or "",
        title=_clean(posting.get("title")),
        city=_city(posting),
        description=_description(detail, posting_jsonld),
        technologies=_technologies_list(posting, detail),
        salary_month=month,
        salary_hour=hour,
        currency=currency,
        salary_type=salary_type,
        employment_types=employment_types,
        seniority=_seniority(detail),
        work_mode=_work_mode(posting, detail, posting_jsonld),
        application_url=url,
        benefits=_benefits(detail, posting_jsonld),
        date=date,
    )


def _with_page(url: str, body, page: int):
    """Return (url, body) with the pagination fields of a captured request set to `page`."""
    parts = urlparse(url)
    qs = parse_qs(parts.query, keep_blank_values=True)
    body = dict(body) if isinstance(body, dict) else {}
    changed = False
    if "page" in body:
        body["page"] = page
        changed = True
    for key in ("page", "pageFrom", "pageTo"):
        if key in qs:
            qs[key] = [str(page)]
            changed = True
    if "offset" in qs and "limit" in qs:
        qs["offset"] = [str((page - 1) * int(qs["limit"][0]))]
        changed = True
    if not changed:
        raise _ApiError(f"can't find a page field in captured request: {url} {body}")
    return parts._replace(query=urlencode(qs, doseq=True)).geturl(), body


# --------------------------------------------------------------------------
# Browser: capture the site's own request, replay it in parallel
# --------------------------------------------------------------------------
_FETCH_JS = """
async (jobs) => Promise.all(jobs.map(async j => {
  if (!j.url) return {__error: "no url"};
  try {
    const h = Object.assign({"Accept": "application/json"}, j.headers || {});
    if (j.body && !Object.keys(h).some(k => k.toLowerCase() === "content-type")) h["Content-Type"] = "application/json";
    const r = await fetch(j.url, {method: j.method, headers: h, body: j.body ? JSON.stringify(j.body) : undefined});
    if (!r.ok) return {__error: "HTTP " + r.status};
    return await r.json();
  } catch (e) { return {__error: String(e)}; }
}))
"""


class _Browser:
    def __init__(self, headless=False):
        from patchright.sync_api import sync_playwright

        self.pw = sync_playwright().start()
        self.ctx = self.pw.chromium.launch_persistent_context(
            USER_DATA_DIR,
            channel="chrome",
            headless=headless,
            viewport={"width": 1920, "height": 1080},
            args=["--disable-blink-features=AutomationControlled"],
        )
        self.page = self.ctx.pages[0] if self.ctx.pages else self.ctx.new_page()
        self.last_error = None

    def discover_search_request(self):
        """Load the page, click 'load more' once, return that request's (url, body, headers)."""
        pg = self.page
        pg.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_selector(CARD, timeout=30000)
        pg.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        btn = pg.locator(LOAD_MORE)
        try:
            btn.wait_for(state="visible", timeout=15000)
        except Exception:
            raise _ApiError("no 'load more' button - fewer results than one page?")
        with pg.expect_request(
            lambda r: "/api/search/posting" in r.url and r.method == "POST", timeout=15000
        ) as info:
            btn.evaluate("e => e.click()")  # JS click: works even under a cookie banner
        req = info.value
        headers = {
            k: v for k, v in req.headers.items()
            if k.lower() in ("content-type", "accept") or k.lower().startswith("x-")
        }
        return req.url, req.post_data_json, headers

    def run(self, jobs):
        """Fire requests in parallel batches from inside the page; None for failures."""
        out = []
        for i in range(0, len(jobs), BATCH):
            for res in self.page.evaluate(_FETCH_JS, jobs[i:i + BATCH]):
                if isinstance(res, dict) and "__error" in res:
                    self.last_error = self.last_error or res["__error"]
                    res = None
                out.append(res)
        return out

    def close(self):
        try:
            self.ctx.close()
        finally:
            self.pw.stop()


def _fetch_via_api(fetch_details, max_listings, headless):
    br = _Browser(headless)
    try:
        url, body, headers = br.discover_search_request()
        print(f"[nofluff] captured search request: {url}\n[nofluff] body: {json.dumps(body)[:300]}")
        period = parse_qs(urlparse(url).query).get("salaryPeriod", ["month"])[0]

        def search_job(p):
            u, b = _with_page(url, body, p)
            return {"method": "POST", "url": u, "headers": headers, "body": b}

        first = br.run([search_job(1)])[0]
        if not first or not first.get("postings"):
            raise _ApiError(f"page 1 replay failed ({br.last_error})")
        postings = list(first["postings"])
        per_page = len(postings)
        total = first.get("totalCount") or per_page
        pages = math.ceil(total / per_page)
        if max_listings:
            pages = min(pages, math.ceil(max_listings / per_page))
        print(f"[nofluff] totalCount={total}, {per_page}/page, fetching {pages} page(s)")

        if pages > 1:
            rest = br.run([search_job(p) for p in range(2, pages + 1)])
            if any(r is None for r in rest):
                raise _ApiError(f"a search page failed ({br.last_error})")
            for r in rest:
                postings.extend(r.get("postings") or [])

        unique = {}
        for p in postings:
            unique.setdefault(p.get("id") or p.get("url"), p)
        postings = list(unique.values())
        if pages > 1 and len(postings) <= per_page:
            raise _ApiError("server returned the same page every time")
        if max_listings:
            postings = postings[:max_listings]

        details = [None] * len(postings)
        if fetch_details:
            br.last_error = None
            jobs = [
                {"method": "GET", "url": API_POSTING + p["url"] if p.get("url") else "", "headers": {}, "body": None}
                for p in postings
            ]
            details = br.run(jobs)
            missing = sum(d is None for d in details)
            if missing:
                print(f"[nofluff] {missing}/{len(details)} detail requests failed (first error: {br.last_error})")

        # The API detail is sometimes missing/empty. Read the offer page's
        # schema.org JobPosting block for those listings so descriptions,
        # company and date are still filled in.
        jsonld_by_index: dict[int, dict] = {}
        needs_html = [
            (i, _posting_url(p))
            for i, (p, d) in enumerate(zip(postings, details))
            if not _description(d) and _posting_url(p)
        ]
        if needs_html:
            print(f"[nofluff] {len(needs_html)}/{len(postings)} missing descriptions; reading offer HTML")
            for start in range(0, len(needs_html), 10):
                chunk = needs_html[start:start + 10]
                try:
                    pages_html = br.page.evaluate(FETCH_HTML_JS, [u for _, u in chunk])
                except Exception as e:
                    print(f"[nofluff] HTML detail batch failed: {e!r}")
                    continue
                for (index, _), page_html in zip(chunk, pages_html):
                    posting_jsonld = _jsonld_jobposting(page_html)
                    if posting_jsonld:
                        jsonld_by_index[index] = posting_jsonld

        return [
            _to_listing(p, d, jsonld_by_index.get(i), period)
            for i, (p, d) in enumerate(zip(postings, details))
        ]
    finally:
        br.close()


# --------------------------------------------------------------------------
# DOM fallback (only if the API path fails)
# --------------------------------------------------------------------------
CURRENCIES = {"PLN": "PLN", "ZŁ": "PLN", "EUR": "EUR", "€": "EUR", "USD": "USD", "$": "USD", "GBP": "GBP", "£": "GBP", "CHF": "CHF"}

# Scroll + click "Pokaż kolejne oferty" in one in-page loop.
# Old version gave up after ONE slow click (5 s) or a 2.5 s gap without a button,
# so a single hiccup ended pagination early. This one:
#   - waits up to 15 s for the list to grow after a click,
#   - tolerates 3 consecutive stalled clicks (re-clicks the button),
#   - only decides "that's the end" after the button has been absent for ~3 s
#     AND the card count stopped changing.
LOAD_ALL_JS = """
async ({maxClicks, maxCards}) => {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const count = () => document.querySelectorAll('a.posting-list-item').length;
  const findBtn = () => document.querySelector('button[nfjloadmore]');
  let clicks = 0, stalls = 0, idle = 0;
  while (true) {
    if (maxClicks && clicks >= maxClicks) break;
    if (maxCards && count() >= maxCards) break;
    window.scrollTo(0, document.body.scrollHeight);
    const before = count();
    const btn = findBtn();
    if (!btn) {
      await sleep(500);
      if (findBtn() || count() > before) { idle = 0; continue; }
      if (++idle >= 6) break;            // ~3 s with no button and no growth: really the end
      continue;
    }
    idle = 0;
    btn.scrollIntoView({block: 'center'});
    btn.click(); clicks++;
    let grew = false;
    for (let i = 0; i < 300 && !grew; i++) {   // up to 15 s
      await sleep(50);
      grew = count() > before;
    }
    if (grew) stalls = 0;
    else if (++stalls >= 3) break;
  }
  return {cards: count(), clicks};
}
"""

EXTRACT_JS = """
cards => cards.map(a => {
  const text = e => e ? e.innerText.trim() : "";
  const one = (...sels) => { for (const s of sels) { const t = text(a.querySelector(s)); if (t) return t; } return ""; };
  const many = s => [...a.querySelectorAll(s)].map(text).filter(Boolean);
  return {
    href: a.getAttribute("href") || "",
    title: one('[data-cy="title position on the job offer listing"]', 'h3', 'h2'),
    city: one('[data-cy="location on the job offer listing"]'),
    tech: many('[data-cy="category name on the job offer listing"]'),
    salary: one('[data-cy="salary ranges on the job offer listing"]'),
    badge: one('[data-cy="sup"]'),
  };
})
"""

# Fetch each offer's HTML from inside the page (same cookies as the browser).
FETCH_HTML_JS = """
async (urls) => Promise.all(urls.map(async u => {
  try {
    const r = await fetch(u, {credentials: "include"});
    return r.ok ? await r.text() : null;
  } catch (e) { return null; }
}))
"""

_LD_JSON = re.compile(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)


def _jsonld_jobposting(page_html) -> dict | None:
    """Find the schema.org JobPosting object in an offer page (handles @graph)."""
    for m in _LD_JSON.finditer(page_html or ""):
        try:
            data = json.loads(m.group(1))
        except ValueError:
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if item.get("@type") == "JobPosting":
                    return item
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
    return None


def _jsonld_description(page_html) -> str:
    """Description from the offer page's schema.org JobPosting block, if present."""
    posting = _jsonld_jobposting(page_html)
    return _html_to_text(posting.get("description")) if posting else ""


def _parse_salary_text(raw: str):
    text = _clean(raw).upper()
    salary_type = normalize_salary_type(text)
    text = text.replace("B2B", "")
    if not text:
        return None, None, None, salary_type
    currency = next((code for tok, code in CURRENCIES.items() if tok in text), None)
    text = re.sub(r"(?<=\d)[\s\u00a0\u202f](?=\d)", "", text)
    nums = [float(n.replace(",", ".")) for n in re.findall(r"\d+(?:[.,]\d+)?", text)][:2]
    if not nums:
        return None, None, currency, salary_type
    value = sum(nums) / len(nums)
    if re.search(r"GODZ|/\s*H\b|\bH\b|HOUR", text):
        return int(round(value * HOURS_PER_MONTH)), int(round(value)), currency, salary_type
    return int(round(value)), int(round(value / HOURS_PER_MONTH)), currency, salary_type


def _fetch_via_dom(max_listings, headless, max_clicks=None, fetch_details=True):
    from patchright.sync_api import sync_playwright

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            USER_DATA_DIR, channel="chrome", headless=headless, no_viewport=True
        )
        try:
            # Skip heavy stuff we don't parse. Stylesheets are NOT blocked any more:
            # the Angular app + Cloudflare behave more reliably with a normal page.
            ctx.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in {"image", "font", "media"}
                else route.continue_(),
            )
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector(CARD, timeout=30000)

            stats = page.evaluate(
                LOAD_ALL_JS,
                {"maxClicks": max_clicks or 0, "maxCards": max_listings or 0},
            )
            print(f"[nofluff] DOM: {stats['cards']} cards after {stats['clicks']} 'load more' click(s)")
            cards = page.eval_on_selector_all(CARD, EXTRACT_JS)

            # De-duplicate by URL and apply the limit BEFORE any detail fetching.
            seen, unique = set(), []
            for c in cards:
                url = urljoin(SITE_ROOT, c["href"])
                if c["href"] and url not in seen:
                    seen.add(url)
                    unique.append((url, c))
            if max_listings:
                unique = unique[:max_listings]

            descriptions = [""] * len(unique)
            jsonlds: list[dict | None] = [None] * len(unique)
            if fetch_details and unique:
                urls = [u for u, _ in unique]
                for i in range(0, len(urls), 10):
                    try:
                        pages_html = page.evaluate(FETCH_HTML_JS, urls[i:i + 10])
                    except Exception as e:
                        print(f"[nofluff] DOM detail batch failed: {e!r}")
                        continue
                    for j, h in enumerate(pages_html):
                        posting_jsonld = _jsonld_jobposting(h)
                        if posting_jsonld:
                            jsonlds[i + j] = posting_jsonld
                            descriptions[i + j] = _html_to_text(posting_jsonld.get("description"))
                got = sum(bool(d) for d in descriptions)
                print(f"[nofluff] DOM: descriptions found for {got}/{len(descriptions)} offers")
        finally:
            ctx.close()

    listings = []
    for (url, c), desc, posting_jsonld in zip(unique, descriptions, jsonlds):
        month, hour, currency, salary_type = _parse_salary_text(c["salary"])
        posting_jsonld = posting_jsonld or {}
        organization = posting_jsonld.get("hiringOrganization") or {}
        company = _clean(organization.get("name")) if isinstance(organization, dict) else ""

        employment_type = posting_jsonld.get("employmentType")
        employment_types = (
            [str(item) for item in employment_type if item]
            if isinstance(employment_type, list)
            else [str(employment_type)] if employment_type else []
        )
        raw_benefits = posting_jsonld.get("jobBenefits")
        benefits = [str(item) for item in raw_benefits if item] if isinstance(raw_benefits, list) else []

        city = _clean(c["city"])
        work_mode = None
        if str(posting_jsonld.get("jobLocationType") or "").upper() == "TELECOMMUTE":
            work_mode = "remote"
        elif "remote" in city.lower() or "zdaln" in city.lower():
            work_mode = "remote"

        listings.append(
            flat_listing(
                board="nofluff",
                url=url,
                company=company,
                offer_id=url.rstrip("/").rsplit("/", 1)[-1],
                title=_clean(c["title"]),
                city=city,
                description=desc,
                technologies=[_clean(t) for t in c["tech"] if _clean(t)],
                salary_month=month,
                salary_hour=hour,
                currency=currency,
                salary_type=salary_type,
                employment_types=employment_types,
                work_mode=work_mode,
                application_url=url,
                benefits=benefits,
                date=_clean(c["badge"]) or _date_from_jsonld(posting_jsonld),
            )
        )
    return listings


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------
def fetch_no_fluff_listings(fetch_details=True, max_listings=None, use_api=True, headless=False):
    """Return a list of flat listing dictionaries.

    fetch_details: also pull each offer's description (API detail JSON, or JSON-LD in DOM mode).
    use_api:       set False to force the DOM fallback.
    """
    if use_api:
        try:
            return _fetch_via_api(fetch_details, max_listings, headless)
        except Exception as e:
            print(f"[nofluff] API path failed ({e!r}); falling back to page parsing")
    return _fetch_via_dom(max_listings, headless, fetch_details=fetch_details)


if __name__ == "__main__":
    t0 = time.perf_counter()
    results = fetch_no_fluff_listings()
    print(f"Scraped {len(results)} listings in {time.perf_counter() - t0:.1f}s")
    for listing in results[:3]:
        print({k: (v[:80] if isinstance(v, str) else v) for k, v in listing.items()})