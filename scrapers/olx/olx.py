import math
import re
import time

from curl_cffi import requests

from scrapers import progress
from scrapers.common import flat_listing, normalize_salary_type, normalize_work_mode

API_URL = "https://www.olx.pl/api/v1/offers/"
API_PAGE_LIMIT = 40
CATEGORY_ID = 56  # OLX "Praca > Informatyka" subcategory (4 = all jobs)
HARD_PAGE_CAP = 100  # safety cap for "walk until empty" mode

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.olx.pl",
    "Referer": "https://www.olx.pl/praca/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

# ------------------------------------------------------------------
# Helpers for Filtering & Parsing
# ------------------------------------------------------------------

IT_KEYWORDS = [
    r"\bprogramist[a|ą|ę|i|ów|om|ami]?\b",
    r"\bdeveloper[a|ów|om|ami]?\b",
    r"\bdevops\b",
    r"\bfrontend\b",
    r"\bbackend\b",
    r"\bfullstack\b",
    r"\btester\b",
    r"\bqa\b",
    r"\bsoftware\b",
    r"\bpython\b",
    r"\bjava\b",
    r"\bjavascript\b",
    r"\breact\b",
    r"\bnode\b",
    r"\bc\#\b",
    r"\bc\+\+\b",
    r"\bsql\b",
    r"\bdata\b",
    r"\bsysadmin\b",
    r"\bit\b",
    r"\bweb\b",
    r"\bcloud\b",
    r"\binżynier oprogramowania\b",
    r"\bcoder\b",
    r"\bvibecod\w*\b",
]

EXCLUDE_KEYWORDS = [
    r"\bautomatyk\b",
    r"\bcnc\b",
    r"\belektryk\b",
    r"\bmonter\b",
    r"\bkierowca\b",
    r"\bmagazynier\b",
    r"\boperator\b",
]


def is_it_related(title: str) -> bool:
    """Filter out non-IT / industrial automation jobs."""
    title_lower = title.lower()
    for exc in EXCLUDE_KEYWORDS:
        if re.search(exc, title_lower):
            if not any(
                re.search(kw, title_lower)
                for kw in [r"python", r"java\b", r"c\+\+", r"c\#", r"software"]
            ):
                return False
    return any(re.search(kw, title_lower) for kw in IT_KEYWORDS)


def clean_description(text: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", " ", str(text))
    return " ".join(clean.split())


# A conservative list: only names that are unambiguous in a job description.
TECH_KEYWORDS = [
    "Python", "JavaScript", "TypeScript", "Java", "Kotlin", "Scala", "Swift",
    "C++", "C#", "PHP", "Ruby", "Go", "Rust", "SQL", "MySQL", "PostgreSQL",
    "MongoDB", "Redis", "Docker", "Kubernetes", "AWS", "Azure", "GCP",
    "Git", "React", "Angular", "Vue.js", "Next.js", "Node.js", "Django",
    "Flask", "FastAPI", "Spring", "Laravel", "WordPress", "HTML", "CSS",
    "REST API", "GraphQL", "Linux", "Terraform", "Ansible", "Jenkins",
    "GitHub Actions", "Machine Learning", "n8n",
]


def extract_technologies(text: str) -> list[str]:
    """Best-effort technology tags from a free-text description."""
    if not text:
        return []
    found = []
    for name in TECH_KEYWORDS:
        pattern = r"(?<![\w])" + re.escape(name) + r"(?![\w])"
        if re.search(pattern, text, re.IGNORECASE) and name not in found:
            found.append(name)
    return found


def _param(item: dict, key: str) -> dict:
    for param in item.get("params", []):
        if param.get("key") == key:
            return param
    return {}


def _param_labels(item: dict, keys: tuple[str, ...]) -> list[str]:
    labels = []
    for param in item.get("params", []):
        if param.get("key") not in keys:
            continue
        value = param.get("value")
        if isinstance(value, dict):
            label = value.get("label")
            if isinstance(label, list):
                labels.extend(str(part) for part in label if part)
            elif label:
                labels.append(str(label))
        elif isinstance(value, str) and value:
            labels.append(value)
    out = []
    for label in labels:
        for part in label.split(","):
            part = part.strip()
            if part and part not in out:
                out.append(part)
    return out


def extract_currency(item: dict) -> str:
    value = _param(item, "salary").get("value")
    if isinstance(value, dict) and value.get("currency"):
        return str(value["currency"]).upper()
    return ""


def extract_salary_type(item: dict) -> str | None:
    salary = _param(item, "salary")
    return normalize_salary_type(salary.get("name") or salary.get("label"))


def extract_date(item: dict) -> str:
    """Prefer the creation date, fall back to the last refresh, ISO date only."""
    raw = item.get("created_time") or item.get("last_refresh_time") or ""
    return str(raw)[:10]


def extract_company(item: dict) -> str:
    user = item.get("user") or {}
    contact = item.get("contact") or {}
    return str(
        user.get("company_name") or user.get("name") or contact.get("name") or ""
    ).strip()


def extract_employment_types(item: dict) -> list[str]:
    return _param_labels(item, ("type", "agreement"))


def extract_work_mode(item: dict) -> tuple[str | None, str | None]:
    """(work_mode, work_mode_raw) from the ``workplace`` param.

    OLX labels: "W siedzibie firmy" (on_site) -> office, "Praca hybrydowa" -> hybrid,
    "Praca zdalna" (remote_work_possibility) alone -> remote; several options
    together (e.g. on-site + remote) -> hybrid, see normalize_work_mode.
    ``remote_recruitment`` ("Rekrutacja online") is about the interview, not the
    work, and is ignored.
    """
    labels = _param_labels(item, ("workplace",))
    raw = ", ".join(labels) or None
    return normalize_work_mode(labels), raw


def extract_numeric_salary(item: dict) -> tuple[int, int]:
    """
    Extracts salary numbers directly without converting between hourly and monthly rates.
    Returns (salary_month, salary_hour); 0 means not disclosed.
    """
    params = item.get("params", [])
    s_from, s_to = None, None
    is_hourly = False

    # 1. Check structured API params
    for p in params:
        if p.get("key") == "salary":
            val = p.get("value")
            if isinstance(val, dict):
                s_from = val.get("from")
                s_to = val.get("to")

                # Check for explicit hourly indicators in type or label
                type_str = str(val.get("type", "")).lower()
                label_str = str(val.get("label", "")).lower()
                if "hour" in type_str or "godz" in type_str or "/h" in label_str or "zł/h" in label_str:
                    is_hourly = True

    # 2. Regex fallback on title if numeric bounds were not found in params
    if s_from is None and s_to is None:
        title = item.get("title", "")
        match = re.search(
            r"(\d+[\d\s]*)\s*(?:-\s*(\d+[\d\s]*))?\s*(?:zł|PLN|EUR|USD)?\s*(?:/|\bza\b|\bna\b)?\s*(h|godz)?",
            title,
            re.IGNORECASE,
        )
        if match:
            s_from = match.group(1).replace(" ", "") if match.group(1) else None
            s_to = match.group(2).replace(" ", "") if match.group(2) else None
            if match.group(3):
                is_hourly = True

    # Calculate average integer value if a range is present
    if s_from is not None or s_to is not None:
        val_from = float(s_from) if s_from is not None else float(s_to)
        val_to = float(s_to) if s_to is not None else val_from
        calc_val = int(round((val_from + val_to) / 2))

        if is_hourly:
            return 0, calc_val
        else:
            return calc_val, 0

    return 0, 0


# ------------------------------------------------------------------
# Main Scraper Function
# ------------------------------------------------------------------

def fetch_olx_jobs(query: str | None = None, max_pages: int | None = None) -> list[dict]:
    """Fetch IT-related OLX job offers as flat listing dictionaries.

    Broad mode (default): the OLX IT category, no salary requirement, no search
    phrase, all API pages walked until the result set is exhausted (the API caps
    at ~1000 offers). ``max_pages`` is only a safety/debug limit.
    """
    limit = API_PAGE_LIMIT
    listings_out: list[dict] = []
    seen_ids: set[str] = set()
    page = 0
    cap = max_pages if max_pages is not None else HARD_PAGE_CAP
    progress.stage("API pages", total=max_pages)

    while page < cap:
        offset = page * limit
        params = {"offset": offset, "limit": limit, "category_id": CATEGORY_ID}
        if query:
            params["query"] = query

        print(f"Fetching page {page + 1} (offset={offset})...")

        try:
            response = requests.get(
                API_URL,
                headers=HEADERS,
                params=params,
                impersonate="chrome124",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            page_items = data.get("data", [])
            if not page_items:
                break

            for item in page_items:
                title = item.get("title", "")
                offer_id = item.get("id")

                # 0. Skip duplicates (promoted offers repeat across pages)
                ident = str(offer_id or item.get("url") or "")
                if ident and ident in seen_ids:
                    continue
                if ident:
                    seen_ids.add(ident)

                # 1. Filter out non-IT jobs (by title)
                if not is_it_related(title):
                    continue

                # 2. Extract salary; 0/0 becomes None in flat_listing
                salary_month, salary_hour = extract_numeric_salary(item)
                description = clean_description(item.get("description", ""))
                techs = extract_technologies(description)
                work_mode, work_mode_raw = extract_work_mode(item)

                listings_out.append(
                    flat_listing(
                        board="olx",
                        url=str(item.get("url") or ""),
                        company=extract_company(item),
                        offer_id=offer_id or "",
                        title=title,
                        city=str((item.get("location") or {}).get("city", {}).get("name") or ""),
                        description=description,
                        technologies=techs,
                        salary_month=salary_month,
                        salary_hour=salary_hour,
                        currency=extract_currency(item),
                        salary_type=extract_salary_type(item),
                        employment_types=extract_employment_types(item),
                        work_mode=work_mode,
                        work_mode_raw=work_mode_raw,
                        application_url=item.get("external_url") or item.get("url"),
                        date=extract_date(item),
                    )
                )

            # Stop once the API's advertised total has been covered.
            meta = data.get("metadata") or {}
            total = meta.get("total_elements")
            progress.advance()
            if page == 0 and isinstance(total, int) and total > 0:
                progress.set_total(min(cap, math.ceil(total / limit)))
            if isinstance(total, int) and offset + len(page_items) >= total:
                break

            page += 1
            time.sleep(1)

        except Exception as e:
            print(f"Error fetching page {page + 1}: {e}")
            break

    return listings_out


if __name__ == "__main__":
    jobs = fetch_olx_jobs(query="programista", max_pages=2)

    print(f"\nRetrieved {len(jobs)} listings:\n")
    for idx, job in enumerate(jobs, 1):
        print(f"{idx}. {job['title']}")
        print(f"   City:         {job['city']}")
        print(f"   Salary Month: {job['salary-month']}")
        print(f"   Salary Hour:  {job['salary-hour']}")
        print(f"   URL:          {job['url']}")
        print(f"   Company:      {job['company']}")
        print(f"   Description:  {job['description'][:100]}...\n")
