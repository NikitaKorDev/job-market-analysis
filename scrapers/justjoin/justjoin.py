"""
JustJoin.it scraper v4 (patchright): same safety design as v3, but faster.

What changed vs v3 (behaviour and output format are unchanged):
  * Listing pages are loaded LISTING_PARALLEL at a time instead of strictly one by one
    (every page start still goes through the shared throttle).
  * Scrolling no longer sleeps a fixed 500 ms per pass: it waits only until the page
    actually grows (or a short timeout), and reads everything in one browser round trip.
  * State is written at most every SAVE_INTERVAL_S seconds / SAVE_EVERY offers, and the
    JSON is written in a worker thread, so the event loop (and all workers) never stall
    while a multi-MB file is serialised.
  * Default request interval lowered a little (BASE_INTERVAL / JITTER); the adaptive
    throttle and circuit breaker still back off on 429/403/5xx exactly as before.

Safety design (what actually keeps you from getting blocked):
  * INCREMENTAL: offers already scraped are read from a local state file and are NOT
    re-fetched (only re-checked after REFRESH_AFTER_DAYS).
  * THROTTLE: one shared rate limiter with jitter for every request.
  * ADAPTIVE: on 429/403/5xx it honours Retry-After, pauses ALL workers, and doubles
    the request interval; it speeds back up slowly after successes.
  * CIRCUIT BREAKER: after several blocks in a row it stops the run, saves progress,
    and exits instead of hammering the site.
  * SESSION REUSE: cookies are saved between runs.

Set JJ_DEBUG=1 to dump the first offer's HTML to debug_offer.html.
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
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright

try:
    import lxml  # noqa: F401
    PARSER = "lxml"
except ImportError:
    PARSER = "html.parser"

# ------------------------------------------------------------------ config
BASE_URL_TEMPLATE = "https://justjoin.it/job-offers/all-locations?page={page}"
MAX_LISTING_PAGES = 300         # safety ceiling only; crawl ends when the site runs out of pages
MAX_SCROLL_PASSES = 60          # per listing page, for lazy-loaded / virtualized lists
SCROLL_WAIT_MS = 450            # max wait for the page to grow after a scroll (returns early on growth)
LISTING_RETRIES = 2             # a failed listing page is retried, never silently skipped
LISTING_PARALLEL = 3            # listing pages loaded concurrently (starts still throttled)
HOURS_PER_MONTH = 168
WORKDAYS_PER_MONTH = 21

CONCURRENCY = 6                 # in-flight offer requests
BASE_INTERVAL = 0.25            # min seconds between ANY two requests (start value)
JITTER = 0.25                   # + random 0..JITTER seconds
MAX_INTERVAL = 8.0              # slowest the adaptive throttle will go
MAX_RETRIES = 3
MAX_CONSECUTIVE_BLOCKS = 5      # circuit breaker
BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}

REFRESH_AFTER_DAYS = 7          # re-fetch an offer after this long (catches edits)
MAX_REFRESH_PER_RUN = 25        # cap so refreshes never cause a burst
PRUNE_AFTER_DAYS = 30           # forget offers not seen in listings for this long

# State lives next to this file (or in JJ_DATA_DIR), NOT in the current working directory.
DATA_DIR = Path(os.environ.get("JJ_DATA_DIR", Path(__file__).resolve().parent))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = str(DATA_DIR / "justjoin_state.json")
STORAGE_STATE_FILE = str(DATA_DIR / "justjoin_browser_state.json")
SAVE_EVERY = 100                # save after this many fetched offers...
SAVE_INTERVAL_S = 60            # ...or after this many seconds, whichever comes first
STATE_VERSION = 2               # bump to force a one-time re-fetch of all cached offers
RAW_OUTPUT_FILE = str(DATA_DIR / "justjoin_raw.json")

HEADLESS = True                 # if you get challenged, try False
BLOCK_HEAVY_RESOURCES = True
BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}
DEBUG = bool(os.environ.get("JJ_DEBUG"))

LINK_SELECTOR = 'a[href*="/job-offer/"], a[href*="/offers/"]'
LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I
)
NEXT_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.S | re.I
)

# One browser round trip per scroll pass: links, element count and page height together.
SNAPSHOT_JS = """
sel => {
  const els = [...document.querySelectorAll(sel)];
  return {
    hrefs: [...new Set(els.map(e => e.href))],
    count: els.length,
    height: document.body.scrollHeight,
  };
}
"""
# Resolves as soon as the page grew or the set of link elements changed.
PROGRESS_JS = (
    "([h, n, sel]) => document.body.scrollHeight > h "
    "|| document.querySelectorAll(sel).length !== n"
)


# ------------------------------------------------------------------ throttling
class Throttle:
    """Shared, adaptive rate limiter with jitter and a global cooldown."""

    def __init__(self):
        self.base = BASE_INTERVAL
        self.interval = BASE_INTERVAL
        self._lock = asyncio.Lock()
        self._next = 0.0
        self.cooldown_until = 0.0

    async def wait(self):
        async with self._lock:
            start = max(time.monotonic(), self._next, self.cooldown_until)
            self._next = start + self.interval + random.uniform(0, JITTER)
        while True:
            delay = max(start, self.cooldown_until) - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    def penalize(self, retry_after=None):
        self.interval = min(self.interval * 2, MAX_INTERVAL)
        pause = retry_after if retry_after else 20 + random.uniform(0, 10)
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + pause)
        print(f"  ! throttled: pausing {pause:.0f}s, interval now {self.interval:.1f}s")

    def reward(self):
        self.interval = max(self.base, self.interval * 0.95)


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


# ------------------------------------------------------------------ schema / parsing
def get_listing_schema():
    return {
        "title": "",
        "city": "",
        "description": "",
        "technologies": [],
        "salary-month": 0.0,
        "salary-hour": 0.0,
        "currency": "",
        "date": "",
    }


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
    return []


def clean_description(raw):
    if not raw:
        return ""
    return BeautifulSoup(htmllib.unescape(raw), PARSER).get_text(separator="\n", strip=True)


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

    avg = sum(nums) / len(nums)
    u = str(unit).upper()
    if "HOUR" in u:
        month = avg * HOURS_PER_MONTH
    elif "DAY" in u:
        month = avg * WORKDAYS_PER_MONTH
    elif "YEAR" in u:
        month = avg / 12
    elif "MONTH" in u:
        month = avg
    else:
        month = avg if avg > 500 else avg * HOURS_PER_MONTH
    return round(month, 2), round(month / HOURS_PER_MONTH, 2), currency


PUBLISHED_RE = re.compile(
    r"(?:Published|Opublikowano)\s*:?(?:\s|&nbsp;)*(\d{1,2})\.(\d{1,2})\.(\d{4})", re.I
)


def extract_published(page_html):
    """Reads the 'Published: 28.09.2026' chip. Returns (iso_date, raw_text).
    Matches on the text, not the hashed MUI classes; also accepts the Polish label."""
    m = PUBLISHED_RE.search(page_html)
    if not m:
        return "", ""
    day, month, year = (int(x) for x in m.groups())
    try:
        return datetime(year, month, day).date().isoformat(), m.group(0)
    except ValueError:
        return "", m.group(0)


def build_records(offer_url, jp, page_html):
    month, hour, currency = salary_from_jsonld(jp)
    published_iso, published_raw = extract_published(page_html)
    # chip first; fall back to JSON-LD datePosted (trimmed to YYYY-MM-DD)
    date = published_iso or str(jp.get("datePosted") or "")[:10]

    schema = get_listing_schema()
    schema.update(
        {
            "title": jp.get("title", ""),
            "city": extract_city(jp),
            "description": clean_description(jp.get("description", "")),
            "technologies": extract_skills(jp, page_html),
            "salary-month": month,
            "salary-hour": hour,
            "currency": currency,
            "date": date,
        }
    )
    raw = {"offer_url": offer_url, "published_raw": published_raw, "jobposting": jp}
    return raw, schema


# ------------------------------------------------------------------ state
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"offers": {}}


def _write_state(snapshot):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)  # atomic: a crash never leaves a corrupt file


class Saver:
    """Writes the state file in a worker thread so serialising a big JSON never blocks
    the event loop. Offer entries are replaced wholesale (never mutated in place) while
    fetching, so a shallow snapshot of the offers dict is safe to dump from a thread."""

    def __init__(self, state):
        self.state = state
        self.lock = asyncio.Lock()
        self.last = time.monotonic()

    def due(self, fetched):
        return fetched % SAVE_EVERY == 0 or time.monotonic() - self.last >= SAVE_INTERVAL_S

    async def save(self, force=False):
        if self.lock.locked() and not force:
            return  # a save is already running; the next trigger will catch up
        async with self.lock:
            snapshot = {**self.state, "offers": dict(self.state["offers"])}
            await asyncio.to_thread(_write_state, snapshot)
            self.last = time.monotonic()


def is_stale(entry):
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
    except (KeyError, ValueError):
        return True
    return datetime.now(timezone.utc) - fetched > timedelta(days=REFRESH_AFTER_DAYS)


# ------------------------------------------------------------------ network
def normalize_url(href, base):
    u = urlparse(urljoin(base, href))
    return urlunparse((u.scheme, u.netloc, u.path.rstrip("/"), "", "", ""))


async def collect_offer_urls(page_num, context, throttle, ctl):
    """Returns (urls, status). status: 'ok' | 'end' (page loads but has no offers) | 'failed'."""
    url = BASE_URL_TEMPLATE.format(page=page_num)
    await throttle.wait()
    page = await context.new_page()
    try:
        print(f"Listing page {page_num}")
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        if resp and resp.status in BLOCK_STATUSES:
            throttle.penalize(parse_retry_after(resp))
            ctl.block()
            return [], "failed"
        try:
            await page.wait_for_selector(LINK_SELECTOR, timeout=10000)
        except Exception:
            ctl.ok()  # page answered 200 but has no offer links: past the last page
            return [], "end"

        # Scroll until no new links appear; accumulate across passes so a
        # virtualized list (which removes off-screen cards) can't lose any.
        found, stable = {}, 0
        for _ in range(MAX_SCROLL_PASSES):
            snap = await page.evaluate(SNAPSHOT_JS, LINK_SELECTOR)
            before = len(found)
            for h in snap["hrefs"]:
                found.setdefault(normalize_url(h, url), None)
            stable = stable + 1 if len(found) == before else 0
            if stable >= 2:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                # returns the moment new content shows up instead of sleeping a fixed time
                await page.wait_for_function(
                    PROGRESS_JS,
                    arg=[snap["height"], snap["count"], LINK_SELECTOR],
                    timeout=SCROLL_WAIT_MS,
                    polling=50,
                )
            except Exception:
                pass  # nothing new within the window: counts toward 'stable'
        ctl.ok()
        return list(found), "ok"
    except Exception as e:
        print(f"Listing page {page_num} failed: {e}")
        return [], "failed"
    finally:
        await page.close()


async def load_listing_page(page_num, context, throttle, ctl):
    """One listing page with retries. A failed page is retried, never silently skipped."""
    status, urls = "failed", []
    for attempt in range(LISTING_RETRIES + 1):
        if ctl.abort.is_set():
            break
        urls, status = await collect_offer_urls(page_num, context, throttle, ctl)
        if status != "failed":
            break
        await asyncio.sleep(3 * (attempt + 1) + random.uniform(0, 2))
    return urls, status


async def collect_all_offer_urls(context, throttle, ctl):
    """Walks EVERY listing page until the site runs out, LISTING_PARALLEL pages at a time.
    Results are consumed in page order, so the output order matches the sequential crawl.
    Returns (urls, complete)."""
    ordered, seen = [], set()
    n = 1
    while n <= MAX_LISTING_PAGES:
        batch = list(range(n, min(n + LISTING_PARALLEL, MAX_LISTING_PAGES + 1)))
        results = await asyncio.gather(
            *(load_listing_page(p, context, throttle, ctl) for p in batch)
        )
        for p, (urls, status) in zip(batch, results):
            if status == "failed":
                print(f"!! Listing page {p} could not be loaded: the offer list is INCOMPLETE.")
                return ordered, False
            if status == "end" and p == 1:
                print("!! First listing page shows no offers (blocked or layout changed).")
                return ordered, False

            new = [u for u in urls if u not in seen]
            if status == "end" or not new:  # no more pages
                print(f"Reached the end of the listings after {p - 1} pages.")
                return ordered, True
            seen.update(new)
            ordered.extend(new)
            print(f"  page {p}: +{len(new)} offers ({len(ordered)} total)")
        n += LISTING_PARALLEL

    print(f"!! Hit MAX_LISTING_PAGES={MAX_LISTING_PAGES}; raise it if the site has more pages.")
    return ordered, False


async def fetch_via_tab(context, url):
    page = await context.new_page()
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        return (resp.status if resp else None), await page.content()
    except Exception:
        return None, ""
    finally:
        await page.close()


async def fetch_offer_html(context, url, throttle, sem, ctl):
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            if ctl.abort.is_set():
                return None

            await throttle.wait()
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

            if status in (404, 410):  # offer gone: normal, not a block
                return None

            if status == 429 or (status in BLOCK_STATUSES and status != 403):
                throttle.penalize(retry_after)
                ctl.block()
                await asyncio.sleep(min(2 ** attempt, 10) + random.uniform(0, 1))
                continue

            # 403 / 200-without-data / network error: one throttled try in a real
            # browser tab (can pass challenges that a bare request can't).
            await throttle.wait()
            tab_status, tab_body = await fetch_via_tab(context, url)
            if tab_status == 200 and "application/ld+json" in tab_body:
                ctl.ok()
                return tab_body
            throttle.penalize(None if tab_status != 429 else 60)
            ctl.block()
        return None


async def process_offer(context, url, throttle, sem, ctl, state, saver, counter, debug_state):
    body = await fetch_offer_html(context, url, throttle, sem, ctl)
    if not body:
        return
    if DEBUG and not debug_state["dumped"]:
        debug_state["dumped"] = True
        with open(DATA_DIR / "debug_offer.html", "w", encoding="utf-8") as f:
            f.write(body)
    jp = extract_jobposting(body)
    if not jp:
        print(f"No JobPosting data for {url}")
        return

    raw, schema = build_records(url, jp, body)
    old = state["offers"].get(url, {})
    now = now_iso()
    # replaced wholesale, never mutated in place (keeps the threaded snapshot safe)
    state["offers"][url] = {
        "raw": raw,
        "schema": schema,
        "first_seen": old.get("first_seen", now),
        "last_seen": now,
        "fetched_at": now,
        "v": STATE_VERSION,
    }
    counter["n"] += 1
    if saver.due(counter["n"]):
        await saver.save()
        print(f"  ...{counter['n']} offers fetched so far")


async def _block_heavy(route):
    if route.request.resource_type in BLOCKED_TYPES:
        await route.abort()
    else:
        await route.continue_()


# ------------------------------------------------------------------ main
async def fetch_justjoin_listings():
    state = load_state()
    saver = Saver(state)
    throttle, ctl = Throttle(), Control()
    counter, debug_state = {"n": 0}, {"dumped": False}

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

            listed, complete = await collect_all_offer_urls(context, throttle, ctl)
            print(f"{len(listed)} offers currently listed"
                  + ("" if complete else "  (INCOMPLETE: re-run to pick up the rest)"))

            # Decide what actually needs a request
            now = now_iso()
            new_urls = [u for u in listed if u not in state["offers"]]
            backfill = [
                u for u in listed
                if u in state["offers"] and state["offers"][u].get("v") != STATE_VERSION
            ]
            backfill_set = set(backfill)
            stale = [
                u for u in listed
                if u in state["offers"] and u not in backfill_set and is_stale(state["offers"][u])
            ]
            for u in listed:
                if u in state["offers"]:
                    state["offers"][u]["last_seen"] = now
            to_fetch = new_urls + backfill + stale[:MAX_REFRESH_PER_RUN]
            random.shuffle(to_fetch)
            print(
                f"{len(new_urls)} new, {len(backfill)} to backfill, "
                f"{min(len(stale), MAX_REFRESH_PER_RUN)} stale refreshes, "
                f"{len(listed) - len(to_fetch)} served from cache"
            )

            sem = asyncio.Semaphore(CONCURRENCY)
            try:
                await asyncio.gather(
                    *(
                        process_offer(
                            context, u, throttle, sem, ctl, state, saver, counter, debug_state
                        )
                        for u in to_fetch
                    ),
                    return_exceptions=True,
                )
            finally:
                # forget long-gone offers, persist everything, keep the session
                cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_AFTER_DAYS)
                for u in list(state["offers"]):
                    try:
                        if datetime.fromisoformat(state["offers"][u]["last_seen"]) < cutoff:
                            del state["offers"][u]
                    except (KeyError, ValueError):
                        pass
                await saver.save(force=True)
                await context.storage_state(path=STORAGE_STATE_FILE)
        finally:
            await browser.close()

    current = [state["offers"][u] for u in listed if u in state["offers"]]
    return [c["raw"] for c in current], [c["schema"] for c in current]


def dump_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


if __name__ == "__main__":
    t0 = time.monotonic()
    raw, schema = asyncio.run(fetch_justjoin_listings())
    n = len(schema)
    dump_json(RAW_OUTPUT_FILE, raw)
    print(f"\n{n} listings in {time.monotonic() - t0:.0f}s. Raw data -> {RAW_OUTPUT_FILE}")
    if n:
        ws = sum(1 for s_ in schema if s_["salary-month"] > 0)
        wt = sum(1 for s_ in schema if s_["technologies"])
        wd = sum(1 for s_ in schema if s_["description"])
        wdate = sum(1 for s_ in schema if s_["date"])
        print(f"  salary: {ws}/{n}  technologies: {wt}/{n}  description: {wd}/{n}  date: {wdate}/{n}")
        print(schema[0])