import re
import time
from curl_cffi import requests

API_URL = "https://www.olx.pl/api/v1/offers/"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.olx.pl",
    "Referer": "https://www.olx.pl/praca/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

# ------------------------------------------------------------------
# Schema & Validation
# ------------------------------------------------------------------

def get_listing_schema():
    return {
        "title": "",
        "city": "",
        "description": "",
        "salary-month": 0,
        "salary-hour": 0
    }


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


def extract_numeric_salary(item: dict) -> tuple[int, int]:
    """
    Extracts salary numbers directly without converting between hourly and monthly rates.
    Returns (salary_month, salary_hour) as integers.
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

def fetch_olx_jobs(query: str = "programista", max_pages: int = 3):
    limit = 40
    valid_listings = []

    for page in range(max_pages):
        offset = page * limit
        params = {"offset": offset, "limit": limit, "category_id": 4, "query": query}

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

            listings = data.get("data", [])
            if not listings:
                break

            for item in listings:
                title = item.get("title", "")

                # 1. Filter out non-IT jobs
                if not is_it_related(title):
                    continue

                # 2. Extract numeric salary without making cross-conversions
                salary_month, salary_hour = extract_numeric_salary(item)
                if salary_month == 0 and salary_hour == 0:
                    continue

                # 3. Construct dictionary strictly conforming to updated schema types
                listing = {
                    "title": str(title or ""),
                    "city": str(item.get("location", {}).get("city", {}).get("name") or ""),
                    "description": clean_description(item.get("description", "")),
                    "salary-month": int(salary_month),
                    "salary-hour": int(salary_hour),
                }

                # 4. Verify against schema validator
                if check_listing(listing):
                    valid_listings.append(listing)
                else:
                    print(f"Warning: Listing failed schema check: {title}")

            time.sleep(1)

        except Exception as e:
            print(f"Error fetching page {page + 1}: {e}")
            break

    return valid_listings


if __name__ == "__main__":
    jobs = fetch_olx_jobs(query="programista", max_pages=2)

    print(f"\nRetrieved {len(jobs)} listings matching schema:\n")
    for idx, job in enumerate(jobs[:5], 1):
        print(f"{idx}. {job['title']}")
        print(f"   City:         {job['city']}")
        print(f"   Salary Month: {job['salary-month']}")
        print(f"   Salary Hour:  {job['salary-hour']}")
        print(f"   Description:  {job['description'][:100]}...\n")