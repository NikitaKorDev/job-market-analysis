"""
JustJoin.it scraper v6 (patchright): API first, HTML as the fallback.

Discovery  : the site's own JSON endpoint (https://justjoin.it/api/candidate-api/offers),
             walked by offset, partitioned by category because every query is capped at
             10,000 results. Pages of a query are fetched in parallel.
             If that is incomplete or unavailable, the old HTML listing crawl takes over.
Per offer  : /api/candidate-api/offers/{slug} (JSON with the description in `body`),
             merged over the listing record. If the API fails for an offer (or API_FAIL_LIMIT
             offers in a row), that offer is fetched as HTML and parsed from JSON-LD exactly
             like v5. Set JJ_NO_API=1 to force the HTML path.

Everything else (incremental cache, adaptive shared throttle, incident-based circuit breaker,
journal-based state, cookie reuse) is unchanged from v5, and cached offers from v4/v5 are
reused as they are. Entries fetched via the API carry "src": "api" in the state file and a
raw record of {"offer_url", "published_raw", "source": "api", "api": <merged API record>}
instead of a "jobposting" block.

NOTE: robots.txt on justjoin.it disallows /api/. The HTML path stays on allowed paths.

Cached HTML-era offers (v4/v5 entries, and any offer whose API detail failed) only hold the
schema.org JobPosting, which has no skills, no experience level and no hybrid/office flag.
Every run therefore attaches the offer's API *listing* record (already downloaded during
discovery, it carries requiredSkills / niceToHaveSkills / experienceLevel / workplaceType) to
such entries as raw["api_listing"]; no extra request is made. The HTML path also keeps the
offer record embedded in the page's React Server Components payload as raw["page_offer"].
Set JJ_MAX_FETCH=N to cap the offer detail requests of one run (e.g. a smoke test).

The API field names were taken from third-party documentation and have NOT been verified
against the live site by the author. Run once with JJ_DEBUG=1: it dumps the first listing
response and the first detail response to debug_api_*.json so you can eyeball the mapping.
"""
import asyncio
import html as htmllib
import json
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright

from scrapers import progress
from scrapers.common import flat_listing

try:
    import lxml  # noqa: F401
    PARSER = "lxml"
except ImportError:
    PARSER = "html.parser"

try:
    from zoneinfo import ZoneInfo
    WARSAW = ZoneInfo("Europe/Warsaw")
except Exception:  # no tzdata (e.g. Windows without the tzdata package): fall back to UTC dates
    WARSAW = None

# ------------------------------------------------------------------ config
# --- API
USE_API = not os.environ.get("JJ_NO_API")
API_BASE = "https://justjoin.it/api/candidate-api"
API_OFFERS = API_BASE + "/offers"
API_PAGE_SIZE = 100             # measured-safe page size (1000 reportedly works too)
API_MAX_FROM = 10_000           # the site serves at most this many results per query
API_PAGE_PARALLEL = 8           # listing pages in flight at once (starts are still throttled)
API_COVERAGE = 0.98             # a query counts as complete if we got >= this share of its total
API_FAIL_LIMIT = 8              # consecutive API detail failures before the API is switched off
MAX_FETCH_PER_RUN = int(os.environ.get("JJ_MAX_FETCH") or 0) or None  # None = no cap
API_SWEEP_UNFILTERED = True     # also walk the unfiltered query (newest 10k) to discover new categories
API_CATEGORIES = (              # partitions that get around the 10k cap; unknown ones are auto-discovered
    "data", "java", "analytics", "pm", "devops", "erp", "architecture", "testing", "ai",
    "security", "other", "python", "net", "admin", "javascript", "support", "mobile", "c",
    "ux", "php", "go", "game", "scala", "ruby", "html",
)

# --- HTML fallback listing
BASE_URL_TEMPLATE = "https://justjoin.it/job-offers/all-locations?page={page}"
MAX_LISTING_PAGES = 300
MAX_SCROLL_PASSES = 60
SCROLL_WAIT_MS = 450
LISTING_RETRIES = 2
LISTING_PARALLEL = 6
LISTING_LOOKAHEAD = 8
HOURS_PER_MONTH = 168
WORKDAYS_PER_MONTH = 21

# --- throttle / safety
CONCURRENCY = 16                # in-flight offer requests (must cover rate x latency)
START_INTERVAL = 0.20
MIN_INTERVAL = 0.08             # fastest the throttle will ever go (~12 req/s). RAISE if you see "throttled".
MAX_INTERVAL = 8.0
JITTER = 0.3
MAX_RETRIES = 3
MAX_CONSECUTIVE_BLOCKS = 5      # circuit breaker (counted per incident, not per failed request)
BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}

REFRESH_AFTER_DAYS = 7
MAX_REFRESH_PER_RUN = 25
PRUNE_AFTER_DAYS = 30

DATA_DIR = Path(os.environ.get("JJ_DATA_DIR", Path(__file__).resolve().parent))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = str(DATA_DIR / "justjoin_state.json")
JOURNAL_FILE = str(DATA_DIR / "justjoin_state.journal")
STORAGE_STATE_FILE = str(DATA_DIR / "justjoin_browser_state.json")
STATE_VERSION = 2               # unchanged: v4/v5 cache entries stay valid
RAW_OUTPUT_FILE = str(DATA_DIR / "justjoin_raw.json")
RAW_OUTPUT_INDENT = None        # None = compact + fast C encoder; 2 = pretty but much slower

HEADLESS = True
BLOCK_HEAVY_RESOURCES = True
BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}
DEBUG = bool(os.environ.get("JJ_DEBUG"))
DEBUG_STATE = {"html": False, "api_detail": False, "api_listing": False}

LINK_SELECTOR = 'a[href*="/job-offer/"], a[href*="/offers/"]'
OFFER_PAGE_BASE = "https://justjoin.it/job-offer/"
LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I
)
NEXT_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.S | re.I
)

SNAPSHOT_JS = """
sel => {
  const els = [...document.querySelectorAll(sel)];
  const out = {
    hrefs: [...new Set(els.map(e => e.href))],
    count: els.length,
    height: document.body.scrollHeight,
  };
  window.scrollTo(0, document.body.scrollHeight);
  return out;
}
"""
PROGRESS_JS = (
    "([h, n, sel]) => document.body.scrollHeight > h "
    "|| document.querySelectorAll(sel).length !== n"
)


# ------------------------------------------------------------------ throttling
class Throttle:
    """Shared adaptive rate limiter (AIMD) with jitter and a global cooldown.
    No lock needed: the slot booking in wait() has no await, so it is atomic on one loop."""

    def __init__(self):
        self.interval = START_INTERVAL
        self.floor = MIN_INTERVAL
        self._next = 0.0
        self.cooldown_until = 0.0
        self.last_penalty = 0.0

    async def wait(self):
        start = max(time.monotonic(), self._next, self.cooldown_until)
        self._next = start + self.interval * random.uniform(1 - JITTER, 1 + JITTER)
        while True:
            delay = max(start, self.cooldown_until) - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    def penalize(self, sent_at, retry_after=None):
        """Back off. Returns True if this starts a new incident, False if it is the tail of
        one we already reacted to (every request in flight during a block fails together)."""
        if sent_at < self.last_penalty:
            return False
        now = time.monotonic()
        self.last_penalty = now
        self.floor = min(MAX_INTERVAL, max(self.floor, self.interval * 1.5))
        self.interval = min(self.interval * 2, MAX_INTERVAL)
        pause = retry_after if retry_after else 20 + random.uniform(0, 10)
        self.cooldown_until = max(self.cooldown_until, now + pause)
        print(f"  ! throttled: pausing {pause:.0f}s, interval now {self.interval:.2f}s "
              f"(won't go below {self.floor:.2f}s for a while)")
        return True

    def reward(self):
        self.floor = max(MIN_INTERVAL, self.floor * 0.999)
        self.interval = max(self.floor, self.interval * 0.97)


class Control:
    """Circuit breaker shared by all workers."""

    def __init__(self):
        self.abort = asyncio.Event()
        self.consecutive_blocks = 0

    def ok(self):
        self.consecutive_blocks = 0

    def block(self):
        self.consecutive_blocks += 1
        if self.consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
            if not self.abort.is_set():
                print("!! Too many blocks in a row: stopping run, progress is saved.")
            self.abort.set()


def parse_retry_after(resp):
    try:
        v = resp.headers.get("retry-after")
        return min(float(v), 300) if v else None
    except (ValueError, AttributeError):
        return None


# ------------------------------------------------------------------ schema / parsing (HTML path)
def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def extract_jobposting(page_html):
    for block in LD_RE.findall(page_html):
        try:
            data = json.loads(block.strip())
        except json.JSONDecodeError:
            continue
        for d in _walk(data):
            t = d.get("@type")
            if t == "JobPosting" or (isinstance(t, list) and "JobPosting" in t):
                return d
    return None


NEXT_F_RE = re.compile(r"self\.__next_f\.push\((\[.*?\])\)</script>", re.S)
PAGE_OFFER_KEYS = (  # structured fields kept from the page's embedded offer record
    "slug", "title", "companyName", "experienceLevel", "workplaceType", "workingTime",
    "requiredSkills", "niceToHaveSkills", "employmentTypes", "multilocation", "city",
    "applyUrl", "publishedAt", "languages", "category",
)


def _flight_text(page_html):
    """Concatenated React Server Components payload (self.__next_f.push chunks)."""
    out = []
    for chunk in NEXT_F_RE.findall(page_html or ""):
        try:
            arr = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if len(arr) > 1 and isinstance(arr[1], str):
            out.append(arr[1])
    return "".join(out)


def extract_page_offer(page_html, slug=None):
    """The offer record the page renders from (requiredSkills, niceToHaveSkills,
    experienceLevel, workplaceType, ...), read from the RSC payload. justjoin.it no longer
    ships __NEXT_DATA__. The page also embeds 'similar offers' in the same shape, so the
    record whose slug matches the page wins; otherwise the first one. None if absent."""
    text = _flight_text(page_html)
    if not text:
        return None
    decoder = json.JSONDecoder()
    first = None
    for m in re.finditer(r'"requiredSkills"\s*:\s*\[', text):
        pos, i = m.start(), m.start()
        while True:  # walk back to the '{' of the object that owns this key
            i = text.rfind("{", max(0, pos - 30000), i)
            if i < 0:
                break
            try:
                obj, end = decoder.raw_decode(text, i)
            except ValueError:
                continue
            if end > pos and isinstance(obj, dict) and "requiredSkills" in obj:
                rec = {k: obj[k] for k in PAGE_OFFER_KEYS if k in obj}
                if slug and rec.get("slug") == slug:
                    return rec
                first = first or rec
                break
    return first


def extract_skills(jp, page_html):
    skills = jp.get("skills") if jp else None
    if isinstance(skills, str):
        skills = [s.strip() for s in re.split(r"[,;]", skills)]
    if isinstance(skills, list):
        out = [s["name"] if isinstance(s, dict) and "name" in s else str(s) for s in skills]
        out = [s for s in out if s]
        if out:
            return out
    m = NEXT_RE.search(page_html)
    if m:
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            return []
        for d in _walk(data):
            for key in ("requiredSkills", "skills"):
                val = d.get(key)
                if isinstance(val, list) and val:
                    names = [s.get("name") if isinstance(s, dict) else str(s) for s in val]
                    names = [n for n in names if n]
                    if names:
                        return names
    offer = extract_page_offer(page_html)
    if offer:
        return _names((offer.get("requiredSkills") or []) + (offer.get("niceToHaveSkills") or []))
    return []


def clean_description(raw, unescape=True):
    """HTML -> plain text. JSON-LD descriptions are HTML-escaped HTML (unescape=True);
    the API's `body` is plain HTML (unescape=False, or &lt;tags&gt; in the text would be eaten)."""
    if not raw:
        return ""
    if unescape:
        raw = htmllib.unescape(raw)
    return BeautifulSoup(raw, PARSER).get_text(separator="\n", strip=True)


def extract_city(jp):
    loc = jp.get("jobLocation")
    locs = loc if isinstance(loc, list) else [loc] if loc else []
    cities = []
    for l in locs:
        addr = l.get("address", {}) if isinstance(l, dict) else {}
        if isinstance(addr, dict):
            c = addr.get("addressLocality")
            if c and c not in cities:
                cities.append(c)
    return ", ".join(cities)


def _to_month(avg, unit):
    u = str(unit or "").upper()
    if "HOUR" in u:
        return avg * HOURS_PER_MONTH
    if "DAY" in u:
        return avg * WORKDAYS_PER_MONTH
    if "YEAR" in u:
        return avg / 12
    if "MONTH" in u:
        return avg
    return avg if avg > 500 else avg * HOURS_PER_MONTH


def salary_from_jsonld(jp):
    bs = jp.get("baseSalary")
    if isinstance(bs, list):
        bs = bs[0] if bs else None
    if not isinstance(bs, dict):
        return 0.0, 0.0, ""

    currency = str(bs.get("currency") or "").upper()
    value = bs.get("value", {})
    if isinstance(value, dict):
        lo, hi = value.get("minValue"), value.get("maxValue")
        single = value.get("value")
        unit = value.get("unitText") or bs.get("unitText") or ""
    else:
        lo = hi = None
        single = value
        unit = bs.get("unitText") or ""

    nums = []
    for n in (lo, hi) if (lo is not None or hi is not None) else (single,):
        try:
            nums.append(float(n))
        except (TypeError, ValueError):
            pass
    if not nums:
        return 0.0, 0.0, currency

    month = _to_month(sum(nums) / len(nums), unit)
    return round(month, 2), round(month / HOURS_PER_MONTH, 2), currency


PUBLISHED_RE = re.compile(
    r"(?:Published|Opublikowano)\s*:?(?:\s|&nbsp;)*(\d{1,2})\.(\d{1,2})\.(\d{4})", re.I
)


def extract_published(page_html):
    """Reads the 'Published: 28.09.2026' chip. Returns (iso_date, raw_text)."""
    m = PUBLISHED_RE.search(page_html)
    if not m:
        return "", ""
    day, month, year = (int(x) for x in m.groups())
    try:
        return datetime(year, month, day).date().isoformat(), m.group(0)
    except ValueError:
        return "", m.group(0)


def build_records(offer_url, jp, page_html):
    published_iso, published_raw = extract_published(page_html)
    date = published_iso or str(jp.get("datePosted") or "")[:10]
    record = {
        "offer_url": offer_url,
        "published_raw": published_raw,
        "date": date,
        "skills": extract_skills(jp, page_html),
        "jobposting": jp,
    }
    page_offer = extract_page_offer(page_html, slug_from_url(offer_url))
    if page_offer:
        record["page_offer"] = page_offer
    return record


def parse_offer(url, body):
    """CPU-bound part of the HTML path; runs in a worker thread."""
    jp = extract_jobposting(body)
    if not jp:
        return None
    return build_records(url, jp, body)


# ------------------------------------------------------------------ API: URLs and mapping
def api_offer_url(slug):
    slug = str(slug or "").strip().strip("/")
    return OFFER_PAGE_BASE + quote(slug, safe="-._~") if slug else None


def slug_from_url(url):
    seg = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    return unquote(seg) or None


def api_listing_url(offset, category=None):
    params = [
        ("from", offset),
        ("itemsCount", API_PAGE_SIZE),
        ("sortBy", "publishedAt"),      # 'published' / 'DESC' are HTTP 400
        ("orderBy", "descending"),
    ]
    if category:
        params.append(("categories", category))
    return API_OFFERS + "?" + urlencode(params)


def api_detail_url(slug):
    # An EMPTY slug must be refused: /offers/ answers 200 with the whole listing.
    slug = str(slug or "").strip().strip("/")
    return API_OFFERS + "/" + quote(slug, safe="-._~") if slug else None


def find_records(payload):
    """The list of offer dicts in a listing response, whatever the wrapper key is."""
    cand = None
    if isinstance(payload, list):
        cand = payload
    elif isinstance(payload, dict):
        for key in ("data", "offers", "items", "results"):
            if isinstance(payload.get(key), list):
                cand = payload[key]
                break
        if cand is None:
            for v in payload.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and "slug" in v[0]:
                    cand = v
                    break
    return [r for r in (cand or []) if isinstance(r, dict) and r.get("slug")]


def unwrap_detail(data):
    """The offer dict of a detail response, or None if it isn't one."""
    if isinstance(data, dict) and "title" not in data and isinstance(data.get("data"), dict):
        data = data["data"]
    if isinstance(data, dict) and data.get("title") and not isinstance(data.get("data"), list):
        return data
    return None


def _num(x):
    if isinstance(x, bool) or x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def salary_from_api(rec):
    """employmentTypes mixes the employer's real figure with site-computed currency
    conversions. Select on currencySource == 'original', never on position. Uses the figure
    in the employer's own unit (fromPerUnit/toPerUnit) so the conversion matches the JSON-LD
    path; falls back to the site's monthly-normalised from/to."""
    entries = [e for e in (rec.get("employmentTypes") or []) if isinstance(e, dict)]
    for e in entries:
        if str(e.get("currencySource") or "original").lower() != "original":
            continue
        lo, hi, unit = _num(e.get("fromPerUnit")), _num(e.get("toPerUnit")), e.get("unit")
        if lo is None and hi is None:
            lo, hi, unit = _num(e.get("from")), _num(e.get("to")), "month"
        nums = [n for n in (lo, hi) if n is not None]
        if not nums:
            continue  # undisclosed: stays 0, never a made-up number
        month = _to_month(sum(nums) / len(nums), unit)
        return round(month, 2), round(month / HOURS_PER_MONTH, 2), str(e.get("currency") or "").upper()
    return 0.0, 0.0, ""


def api_city(rec):
    cities = []
    for key in ("multilocation", "locations"):
        for loc in rec.get(key) or []:
            c = loc.get("city") if isinstance(loc, dict) else loc if isinstance(loc, str) else None
            if c and str(c) not in cities:
                cities.append(str(c))
        if cities:
            break
    if not cities and rec.get("city"):
        cities.append(str(rec["city"]))
    return ", ".join(cities)


def api_skills(rec):
    out = []
    for s in rec.get("requiredSkills") or []:
        name = s.get("name") if isinstance(s, dict) else s
        if name:
            out.append(str(name))
    return out


def api_date(rec):
    """ISO date in Warsaw time, matching the 'Published: dd.mm.yyyy' chip on the page."""
    for key in ("publishedAt", "lastPublishedAt"):
        s = rec.get(key)
        if not s:
            continue
        try:
            dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        except ValueError:
            return str(s)[:10]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if WARSAW is not None:
            dt = dt.astimezone(WARSAW)
        return dt.date().isoformat()
    return ""


def api_build_records(offer_url, rec):
    return {
        "offer_url": offer_url,
        "published_raw": str(rec.get("publishedAt") or ""),
        "date": api_date(rec),
        "source": "api",
        "api": rec,
    }


# ------------------------------------------------------------------ flat output
CONTRACT_NAMES = {
    "permanent": "Umowa o pracę",
    "b2b": "B2B",
    "mandate": "Umowa zlecenie",
    "internship": "Staż",
    "contract": "Kontrakt",
    "other": "Inna",
}

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


def _dedupe(values):
    out = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _names(items):
    """Normalize a list of strings / dicts with 'name' or 'value' to strings."""
    out = []
    for item in items or []:
        if isinstance(item, dict):
            text = item.get("name") or item.get("value") or ""
        else:
            text = item
        text = str(text or "").strip()
        if text:
            out.append(text)
    return _dedupe(out)


def _api_employment_types(rec):
    types = []
    for entry in rec.get("employmentTypes") or []:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("type") or "").lower()
        if key:
            types.append(CONTRACT_NAMES.get(key, key))
    return _dedupe(types)


def _api_salary_type(rec):
    """Salary type of the entry salary_from_api() actually selects."""
    for entry in rec.get("employmentTypes") or []:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("currencySource") or "original").lower() != "original":
            continue
        gross = entry.get("gross")
        if gross is True:
            return "brutto"
        if gross is False:
            return "netto"
    return None


def _html_employment_types(jp):
    value = jp.get("employmentType")
    values = value if isinstance(value, list) else [value] if value else []
    out = []
    for item in values:
        key = str(item or "").strip().lower()
        if key:
            out.append(EMPLOYMENT_NAMES.get(key, str(item)))
    return _dedupe(out)


def _label(value):
    """experienceLevel / workplaceType are plain strings in the API and
    {"label", "value"} objects in the page payload."""
    if isinstance(value, dict):
        value = value.get("value") or value.get("label")
    return str(value or "").strip()


def _record_fields(rec):
    """Skills / seniority / work mode from an API detail, API listing or page offer record.

    justjoin distinguishes required from nice-to-have skills, so both lists are always
    lists (possibly empty) when the record carries either key, None otherwise.
    workplaceType is one of remote (fully remote) / hybrid / office and maps 1:1.
    experienceLevel is intern / junior / mid / senior / manager / c_level
    (c_level -> manager in seniority_levels).
    """
    has_skills = "requiredSkills" in rec or "niceToHaveSkills" in rec
    workplace = _label(rec.get("workplaceType"))
    return {
        "technologies_required": _names(rec.get("requiredSkills")) if has_skills else None,
        "technologies_nice": _names(rec.get("niceToHaveSkills")) if has_skills else None,
        "seniority": _label(rec.get("experienceLevel")) or None,
        "work_mode": workplace or None,
        "work_mode_raw": workplace or None,
    }


def raw_source(raw):
    """Which raw path raw_to_flat() uses: api | page_offer | api_listing | jsonld | None."""
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("api"), dict):
        return "api"
    if not isinstance(raw.get("jobposting"), dict):
        return None
    if isinstance(raw.get("page_offer"), dict):
        return "page_offer"
    if isinstance(raw.get("api_listing"), dict):
        return "api_listing"
    return "jsonld"


def raw_to_flat(raw):
    """Turn one cached raw record into a flat listing dictionary.

    Paths, best first:
      api          -- the detail API record (description in `body`).
      page_offer / api_listing
                   -- HTML-era record: description, company, salary and date from the
                      schema.org JobPosting; skills, seniority and work mode from the offer
                      record embedded in the page or from the API listing record.
      jsonld       -- JobPosting only: no seniority, skills only if the JobPosting lists
                      them (untyped), work mode only remote (TELECOMMUTE) or unknown.
    """
    source = raw_source(raw)
    if source is None:
        return None
    offer_url = str(raw.get("offer_url") or "")

    if source == "api":
        rec = raw["api"]
        month, hour, currency = salary_from_api(rec)
        return flat_listing(
            board="justjoin",
            url=offer_url,
            company=str(rec.get("companyName") or ""),
            offer_id=slug_from_url(offer_url) or "",
            title=str(rec.get("title") or ""),
            city=api_city(rec),
            description=clean_description(str(rec.get("body") or ""), unescape=False),
            salary_month=month,
            salary_hour=hour,
            currency=currency,
            salary_type=_api_salary_type(rec),
            employment_types=_api_employment_types(rec),
            application_url=rec.get("applyUrl") or offer_url,
            benefits=_names(rec.get("benefits")),
            date=raw.get("date") or api_date(rec),
            **_record_fields(rec),
        )

    jp = raw["jobposting"]
    month, hour, currency = salary_from_jsonld(jp)
    organization = jp.get("hiringOrganization") or {}
    company = organization.get("name") if isinstance(organization, dict) else ""
    benefits = jp.get("benefits")
    if not benefits:
        benefits = jp.get("jobBenefits")
    common = dict(
        board="justjoin",
        url=offer_url,
        company=str(company or ""),
        offer_id=slug_from_url(offer_url) or "",
        title=str(jp.get("title") or ""),
        description=clean_description(jp.get("description", "")),
        salary_month=month,
        salary_hour=hour,
        currency=currency,
        benefits=_names(benefits),
        date=raw.get("date") or str(jp.get("datePosted") or "")[:10],
    )

    if source in ("page_offer", "api_listing"):
        # listing record first, the page's own record on top of it
        rec = {**(raw.get("api_listing") or {}), **(raw.get("page_offer") or {})}
        return flat_listing(
            city=api_city(rec) or extract_city(jp),
            technologies=raw.get("skills") or [],
            employment_types=_api_employment_types(rec) or _html_employment_types(jp),
            application_url=rec.get("applyUrl") or offer_url,
            **common,
            **_record_fields(rec),
        )

    location_type = str(jp.get("jobLocationType") or "").strip()
    return flat_listing(
        city=extract_city(jp),
        technologies=raw.get("skills") or extract_skills(jp, ""),
        employment_types=_html_employment_types(jp),
        work_mode="remote" if location_type.upper() == "TELECOMMUTE" else None,
        work_mode_raw=location_type or None,
        application_url=offer_url,
        **common,
    )


# ------------------------------------------------------------------ state
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_state():
    """Snapshot first, then replay the journal a crashed run may have left behind."""
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        state = {"offers": {}}
    state.setdefault("offers", {})
    try:
        with open(JOURNAL_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    url, entry = json.loads(line)
                except (ValueError, TypeError):
                    continue  # torn last line after a crash
                state["offers"][url] = entry
    except FileNotFoundError:
        pass
    return state


def write_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(state, ensure_ascii=False))  # C encoder (json.dump is ~5x slower)
    os.replace(tmp, STATE_FILE)


class Journal:
    """Append-only log of fetched offers; folded into the snapshot at the end of the run."""

    def __init__(self):
        self.f = open(JOURNAL_FILE, "a", encoding="utf-8")

    def append(self, url, entry):
        self.f.write(json.dumps([url, entry], ensure_ascii=False) + "\n")
        self.f.flush()

    def close(self):
        self.f.close()


def attach_listing_record(entry, listing_rec):
    """Keep the API listing record with a cached entry that has no API detail record.

    Root cause of the missing skills / seniority / work mode: HTML-era cache entries
    (schema.org JobPosting only) were reused as they are for weeks, and the listing record
    downloaded during discovery, which has requiredSkills, niceToHaveSkills,
    experienceLevel and workplaceType, was thrown away. Returns True if attached."""
    raw = entry.get("raw") if isinstance(entry, dict) else None
    if not isinstance(raw, dict) or isinstance(raw.get("api"), dict):
        return False
    if not isinstance(listing_rec, dict) or not listing_rec:
        return False
    raw["api_listing"] = listing_rec
    return True


def is_stale(entry):
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
    except (KeyError, ValueError):
        return True
    return datetime.now(timezone.utc) - fetched > timedelta(days=REFRESH_AFTER_DAYS)


# ------------------------------------------------------------------ API network
async def api_get_json(context, url, throttle, ctl):
    """GET a JSON endpoint through the shared throttle. Returns (data, status) with
    status 'ok' | 'gone' (404/410) | 'fail'. Blocks (403/429/5xx) back off globally."""
    for attempt in range(MAX_RETRIES + 1):
        if ctl.abort.is_set():
            return None, "fail"
        await throttle.wait()
        sent_at = time.monotonic()
        status, retry_after, data = None, None, None
        try:
            resp = await context.request.get(
                url, timeout=20000, headers={"Accept": "application/json"}
            )
            status = resp.status
            retry_after = parse_retry_after(resp)
            if status == 200:
                data = json.loads(await resp.text())
        except Exception:
            data = None  # network error or non-JSON body

        if status == 200 and data is not None:
            ctl.ok()
            throttle.reward()
            return data, "ok"
        if status in (404, 410):
            return None, "gone"
        if status in BLOCK_STATUSES:
            if throttle.penalize(sent_at, retry_after):
                ctl.block()
            continue
        if attempt >= 1:  # 400 / junk / network error: not a block, one retry then give up
            return None, "fail"
    return None, "fail"


async def api_crawl_partition(context, throttle, ctl, category, gate, on_page):
    """One query (unfiltered or one category), walked by offset. After the first page tells
    us the total, the remaining pages are fetched in parallel.
    Returns (ordered_slugs_by_offset, ok)."""

    async def get_page(offset):
        async with gate:
            return await api_get_json(context, api_listing_url(offset, category), throttle, ctl)

    data, status = await get_page(0)
    if status != "ok":
        return [], False
    if DEBUG and not DEBUG_STATE["api_listing"]:
        DEBUG_STATE["api_listing"] = True
        sample = {"meta": data.get("meta") if isinstance(data, dict) else None,
                  "first_records": find_records(data)[:3]}
        with open(DATA_DIR / "debug_api_listing.json", "w", encoding="utf-8") as f:
            json.dump(sample, f, ensure_ascii=False, indent=2)

    recs = find_records(data)
    meta = data.get("meta") if isinstance(data, dict) else None
    total = meta.get("totalItems") if isinstance(meta, dict) else None
    expected = min(total, API_MAX_FROM) if isinstance(total, int) and not isinstance(total, bool) else None
    if category and expected == API_MAX_FROM:
        print(f"  !! category '{category}' hit the {API_MAX_FROM} result cap: some offers are unreachable")

    pages = {0: recs}
    ok = True
    step = len(recs)
    on_page(recs)
    if step and expected is not None and step < expected:
        offsets = list(range(step, expected, step))
        results = await asyncio.gather(*(get_page(o) for o in offsets))
        for o, (d, s) in zip(offsets, results):
            if s != "ok":
                ok = False
                continue
            pages[o] = find_records(d)
            on_page(pages[o])
    elif step and expected is None:
        # the response has no total: walk until an empty or all-duplicate page
        seen = {r["slug"] for r in recs}
        offset = step
        while offset < API_MAX_FROM:
            d, s = await get_page(offset)
            if s != "ok":
                ok = False
                break
            r = find_records(d)
            if not [x for x in r if x["slug"] not in seen]:
                break
            seen.update(x["slug"] for x in r)
            pages[offset] = r
            on_page(r)
            offset += len(r)

    slugs = []
    for o in sorted(pages):
        slugs.extend(r["slug"] for r in pages[o])
    if ok and expected is not None and len(set(slugs)) < API_COVERAGE * expected:
        print(f"  !! query {category or '(all)'}: got {len(set(slugs))} of {expected} "
              f"(pagination ignored or list changed)")
        ok = False
    return slugs, ok


async def api_collect_all(context, throttle, ctl, on_new, api_records):
    """Walks the whole board through the API. Returns (urls, complete)."""
    gate = asyncio.Semaphore(API_PAGE_PARALLEL)
    categories_seen = set()

    def on_page(recs):
        fresh = []
        for r in recs:
            u = api_offer_url(r["slug"])
            if u and u not in api_records:
                api_records[u] = r
                fresh.append(u)
            cat = r.get("category")
            key = cat.get("key") if isinstance(cat, dict) else cat if isinstance(cat, str) else None
            if key:
                categories_seen.add(str(key).lower())
        if fresh:
            on_new(fresh)

    queries = ([None] if API_SWEEP_UNFILTERED else []) + list(API_CATEGORIES)
    results = await asyncio.gather(
        *(api_crawl_partition(context, throttle, ctl, c, gate, on_page) for c in queries)
    )
    extras = sorted(categories_seen - set(API_CATEGORIES))
    if extras:
        print(f"Discovered categories not in API_CATEGORIES: {', '.join(extras)}")
        results += await asyncio.gather(
            *(api_crawl_partition(context, throttle, ctl, c, gate, on_page) for c in extras)
        )

    ordered, seen, complete = [], set(), True
    for slugs, ok in results:
        complete = complete and ok
        for s in slugs:
            u = api_offer_url(s)
            if u and u not in seen:
                seen.add(u)
                ordered.append(u)
    if not ordered:
        complete = False
    print(f"API listing: {len(ordered)} offers" + ("" if complete else "  (INCOMPLETE)"))
    return ordered, complete


# ------------------------------------------------------------------ HTML network (fallback)
def normalize_url(href, base):
    u = urlparse(urljoin(base, href))
    return urlunparse((u.scheme, u.netloc, u.path.rstrip("/"), "", "", ""))


async def collect_offer_urls(page_num, context, throttle, ctl):
    """Returns (urls, status). status: 'ok' | 'end' (page loads but has no offers) | 'failed'."""
    url = BASE_URL_TEMPLATE.format(page=page_num)
    await throttle.wait()
    sent_at = time.monotonic()
    page = await context.new_page()
    try:
        print(f"Listing page {page_num}")
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        if resp and resp.status in BLOCK_STATUSES:
            if throttle.penalize(sent_at, parse_retry_after(resp)):
                ctl.block()
            return [], "failed"
        try:
            await page.wait_for_selector(LINK_SELECTOR, timeout=10000)
        except Exception:
            ctl.ok()
            return [], "end"

        found, stable = {}, 0
        for _ in range(MAX_SCROLL_PASSES):
            snap = await page.evaluate(SNAPSHOT_JS, LINK_SELECTOR)
            before = len(found)
            for h in snap["hrefs"]:
                found.setdefault(normalize_url(h, url), None)
            stable = stable + 1 if len(found) == before else 0
            if stable >= 2:
                break
            try:
                await page.wait_for_function(
                    PROGRESS_JS,
                    arg=[snap["height"], snap["count"], LINK_SELECTOR],
                    timeout=SCROLL_WAIT_MS,
                    polling=50,
                )
            except Exception:
                pass
        ctl.ok()
        return list(found), "ok"
    except Exception as e:
        print(f"Listing page {page_num} failed: {e}")
        return [], "failed"
    finally:
        await page.close()


async def load_listing_page(page_num, context, throttle, ctl):
    status, urls = "failed", []
    for attempt in range(LISTING_RETRIES + 1):
        if ctl.abort.is_set():
            break
        urls, status = await collect_offer_urls(page_num, context, throttle, ctl)
        if status != "failed":
            break
        await asyncio.sleep(3 * (attempt + 1) + random.uniform(0, 2))
    return urls, status


async def collect_all_offer_urls(context, throttle, ctl, on_new):
    """HTML listing crawl (sliding window of LISTING_PARALLEL pages). Returns (urls, complete)."""
    ordered, seen = [], set()
    gate = asyncio.Semaphore(LISTING_PARALLEL)
    tasks, started = {}, 0

    async def run(p):
        async with gate:
            return await load_listing_page(p, context, throttle, ctl)

    try:
        for p in range(1, MAX_LISTING_PAGES + 1):
            while started < min(p + LISTING_LOOKAHEAD - 1, MAX_LISTING_PAGES):
                started += 1
                tasks[started] = asyncio.create_task(run(started))
            urls, status = await tasks.pop(p)

            if status == "failed":
                print(f"!! Listing page {p} could not be loaded: the offer list is INCOMPLETE.")
                return ordered, False
            if status == "end" and p == 1:
                print("!! First listing page shows no offers (blocked or layout changed).")
                return ordered, False

            new = [u for u in urls if u not in seen]
            if status == "end" or not new:
                print(f"Reached the end of the listings after {p - 1} pages.")
                return ordered, True
            seen.update(new)
            ordered.extend(new)
            on_new(new)
            print(f"  page {p}: +{len(new)} offers ({len(ordered)} total)")

        print(f"!! Hit MAX_LISTING_PAGES={MAX_LISTING_PAGES}; raise it if the site has more pages.")
        return ordered, False
    finally:
        for t in tasks.values():
            t.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)


async def fetch_via_tab(context, url):
    page = await context.new_page()
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        return (resp.status if resp else None), await page.content()
    except Exception:
        return None, ""
    finally:
        await page.close()


async def fetch_offer_html(context, url, throttle, ctl):
    for attempt in range(MAX_RETRIES + 1):
        if ctl.abort.is_set():
            return None

        await throttle.wait()
        sent_at = time.monotonic()
        status, body, retry_after = None, "", None
        try:
            resp = await context.request.get(url, timeout=20000)
            status = resp.status
            retry_after = parse_retry_after(resp)
            if status == 200:
                body = await resp.text()
        except Exception:
            pass

        if status == 200 and "application/ld+json" in body:
            ctl.ok()
            throttle.reward()
            return body

        if status in (404, 410):
            return None

        if status in BLOCK_STATUSES and status != 403:
            if throttle.penalize(sent_at, retry_after):
                ctl.block()
            continue

        # 403 / 200-without-data / network error: one throttled try in a real browser tab
        await throttle.wait()
        sent_at = time.monotonic()
        tab_status, tab_body = await fetch_via_tab(context, url)
        if tab_status == 200 and "application/ld+json" in tab_body:
            ctl.ok()
            return tab_body
        if throttle.penalize(sent_at, 60 if tab_status == 429 else None):
            ctl.block()
    return None


# ------------------------------------------------------------------ per-offer pipeline
async def build_from_api(context, url, listing_rec, throttle, ctl, api_state):
    """Returns ('ok', raw) | ('gone', None) | ('fallback', None)."""
    detail_url = api_detail_url(slug_from_url(url))

    def failed():
        api_state["fails"] += 1
        if api_state["fails"] >= API_FAIL_LIMIT and api_state["enabled"]:
            api_state["enabled"] = False
            print(f"!! API failed {API_FAIL_LIMIT} offers in a row: using HTML for the rest of the run.")
        return "fallback", None

    if not detail_url:
        return "fallback", None
    data, status = await api_get_json(context, detail_url, throttle, ctl)
    if status == "gone":
        return "gone", None
    if status != "ok":
        return failed()
    detail = unwrap_detail(data)
    if detail is None:
        return failed()
    merged = {**(listing_rec or {}), **detail}  # listing fields fill whatever the detail lacks
    if not str(merged.get("body") or "").strip():
        return failed()
    if DEBUG and not DEBUG_STATE["api_detail"]:
        DEBUG_STATE["api_detail"] = True
        with open(DATA_DIR / "debug_api_detail.json", "w", encoding="utf-8") as f:
            json.dump(detail, f, ensure_ascii=False, indent=2)
    api_state["fails"] = 0
    return "ok", await asyncio.to_thread(api_build_records, url, merged)


async def get_offer_records(context, url, throttle, ctl, api_records, api_state):
    """(raw, src) or None. API first, HTML per offer if the API can't deliver it."""
    if api_state["enabled"]:
        outcome, result = await build_from_api(
            context, url, api_records.get(url), throttle, ctl, api_state
        )
        if outcome == "ok":
            return result, "api"
        if outcome == "gone":
            return None

    body = await fetch_offer_html(context, url, throttle, ctl)
    if not body:
        return None
    if DEBUG and not DEBUG_STATE["html"]:
        DEBUG_STATE["html"] = True
        with open(DATA_DIR / "debug_offer.html", "w", encoding="utf-8") as f:
            f.write(body)
    parsed = await asyncio.to_thread(parse_offer, url, body)
    if parsed is None:
        print(f"No JobPosting data for {url}")
        return None
    if isinstance(api_records.get(url), dict):
        parsed["api_listing"] = api_records[url]
    return parsed, "html"


async def offer_worker(queue, context, throttle, ctl, state, journal, counter, api_records, api_state):
    """Pulls offer URLs off the queue until it gets a None sentinel."""
    while True:
        url = await queue.get()
        if url is None:
            return
        try:
            got = await get_offer_records(context, url, throttle, ctl, api_records, api_state)
            if got is None:
                continue
            raw, src = got

            old = state["offers"].get(url, {})
            now = now_iso()
            entry = {
                "raw": raw,
                "first_seen": old.get("first_seen", now),
                "last_seen": now,
                "fetched_at": now,
                "v": STATE_VERSION,
                "src": src,
            }
            state["offers"][url] = entry
            journal.append(url, entry)
            counter["n"] += 1
            counter[src] += 1
            if counter["n"] % 100 == 0:
                print(f"  ...{counter['n']} offers fetched so far "
                      f"(api {counter['api']}, html {counter['html']}, queue {queue.qsize()}, "
                      f"interval {throttle.interval:.2f}s)")
        except Exception as e:
            print(f"Offer failed {url}: {e}")
        finally:
            progress.advance()


async def _block_heavy(route):
    if route.request.resource_type in BLOCKED_TYPES:
        await route.abort()
    else:
        await route.continue_()


# -----------------------
# ------------------------------------------- main
async def fetch_justjoin_listings():
    state = load_state()
    journal = Journal()
    throttle, ctl = Throttle(), Control()
    counter = {"n": 0, "api": 0, "html": 0}
    api_state = {"enabled": USE_API, "fails": 0}
    api_records = {}
    queue = asyncio.Queue()
    stats = {"new": 0, "backfill": 0, "stale": 0, "cached": 0, "deferred": 0, "enriched": 0}
    scheduled = set()
    listed, complete = [], False
    context, workers = None, []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                locale="pl-PL",
                timezone_id="Europe/Warsaw",
                storage_state=STORAGE_STATE_FILE if os.path.exists(STORAGE_STATE_FILE) else None,
            )
            if BLOCK_HEAVY_RESOURCES:
                await context.route("**/*", _block_heavy)

            workers = [
                asyncio.create_task(
                    offer_worker(queue, context, throttle, ctl, state, journal,
                                 counter, api_records, api_state)
                )
                for _ in range(CONCURRENCY)
            ]

            run_started = now_iso()
            progress.stage("listing + offers", total=0)

            def schedule(urls):
                """Decide per URL what needs a request and queue it right away (idempotent)."""
                for u in urls:
                    if u in scheduled:
                        continue
                    scheduled.add(u)
                    entry = state["offers"].get(u)
                    if entry is None:
                        kind = "new"
                    elif entry.get("v") != STATE_VERSION:
                        kind = "backfill"
                    elif is_stale(entry) and stats["stale"] < MAX_REFRESH_PER_RUN:
                        kind = "stale"
                    else:
                        kind = "cached"
                    if entry is not None:
                        entry["last_seen"] = run_started
                        if attach_listing_record(entry, api_records.get(u)):
                            stats["enriched"] += 1
                    if kind != "cached" and MAX_FETCH_PER_RUN is not None and \
                            stats["new"] + stats["backfill"] + stats["stale"] >= MAX_FETCH_PER_RUN:
                        # JJ_MAX_FETCH reached: new offers wait for a later run, known ones
                        # are served from the cache as they are.
                        if entry is None:
                            stats["deferred"] += 1
                            continue
                        kind = "cached"
                    stats[kind] += 1
                    if kind != "cached":
                        queue.put_nowait(u)
                        progress.add_total(1)

            if USE_API:
                listed, complete = await api_collect_all(context, throttle, ctl, schedule, api_records)
                if not complete and not ctl.abort.is_set():
                    print("!! API listing incomplete or unavailable: falling back to the HTML listing crawl.")
            if (not USE_API or not complete) and not ctl.abort.is_set():
                html_urls, html_complete = await collect_all_offer_urls(context, throttle, ctl, schedule)
                known = set(listed)
                listed = listed + [u for u in html_urls if u not in known]
                complete = html_complete

            print(f"{len(listed)} offers currently listed"
                  + ("" if complete else "  (INCOMPLETE: re-run to pick up the rest)"))
            print(f"{stats['new']} new, {stats['backfill']} to backfill, "
                  f"{stats['stale']} stale refreshes, {stats['cached']} served from cache, "
                  f"{stats['enriched']} cached HTML entries enriched from the API listing, "
                  f"{stats['deferred']} deferred (JJ_MAX_FETCH)")

            progress.label(f"offers ({stats['cached']:,} cached)")
            for _ in workers:
                queue.put_nowait(None)
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_AFTER_DAYS)
            for u in list(state["offers"]):
                try:
                    if datetime.fromisoformat(state["offers"][u]["last_seen"]) < cutoff:
                        del state["offers"][u]
                except (KeyError, ValueError):
                    pass
            journal.close()
            await asyncio.to_thread(write_state, state)
            try:
                os.remove(JOURNAL_FILE)  # only reached once the snapshot is safely on disk
            except FileNotFoundError:
                pass
            if context is not None:
                await context.storage_state(path=STORAGE_STATE_FILE)
            await browser.close()

    print(f"Fetched this run: {counter['api']} via API, {counter['html']} via HTML")
    listings = []
    for url in listed:
        entry = state["offers"].get(url)
        if not entry:
            continue
        listing = raw_to_flat(entry.get("raw"))
        if listing:
            listings.append(listing)
    return listings


def dump_json(path, data, indent=RAW_OUTPUT_INDENT):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=indent))
    os.replace(tmp, path)


if __name__ == "__main__":
    t0 = time.monotonic()
    listings = asyncio.run(fetch_justjoin_listings())
    n = len(listings)
    dump_json(RAW_OUTPUT_FILE, listings)
    print(f"\n{n} listings in {time.monotonic() - t0:.0f}s. Flat data -> {RAW_OUTPUT_FILE}")
    if n:
        ws = sum(1 for item in listings if item["salary-month"])
        wt = sum(1 for item in listings if item["technologies"])
        wd = sum(1 for item in listings if item["description"])
        wdate = sum(1 for item in listings if item["date"])
        print(f"  salary: {ws}/{n}  technologies: {wt}/{n}  description: {wd}/{n}  date: {wdate}/{n}")
        print(listings[0])