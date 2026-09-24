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

# Software development & IT keywords
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

# Non-software industrial roles to exclude
EXCLUDE_KEYWORDS = [
    r"\bautomatyk\b",
    r"\bcnc\b",
    r"\belektryk\b",
    r"\bmonter\b",
    r"\bkierowca\b",
    r"\bmagazynier\b",
    r"\boperator\b",
    r"\bserwisant\b",
]


def is_it_related(title: str) -> bool:
    """Ensure title is strictly IT software/hardware related and not PLC/automation."""
    title_lower = title.lower()

    for exc in EXCLUDE_KEYWORDS:
        if re.search(exc, title_lower):
            if not any(
                re.search(kw, title_lower)
                for kw in [r"python", r"java\b", r"c\+\+", r"c\#", r"software"]
            ):
                return False

    return any(re.search(kw, title_lower) for kw in IT_KEYWORDS)


def parse_salary(item: dict) -> str:
    """Extract salary from API JSON or title regex. Returns None if absent."""
    params = item.get("params", [])

    for p in params:
        if p.get("key") == "salary":
            val = p.get("value")
            if isinstance(val, dict):
                if val.get("label"):
                    return val.get("label")

                s_from = val.get("from")
                s_to = val.get("to")
                currency = val.get("currency", "PLN")
                if s_from and s_to:
                    return f"{s_from} - {s_to} {currency}"
                elif s_from:
                    return f"od {s_from} {currency}"
                elif s_to:
                    return f"do {s_to} {currency}"

            elif isinstance(val, (str, int, float)):
                return str(val)

    # Regex Fallback for wages written directly in title
    title = item.get("title", "")
    match = re.search(
        r"(\d+[\d\s,.]*\s*(?:-\s*\d+[\d\s,.]*)?\s*(?:zł|PLN|EUR|USD)(?:\s*/\s*(?:h|godz|mies|m-c|msc))?)",
        title,
        re.IGNORECASE,
    )
    if match:
        return f"{match.group(0).strip()} (from title)"

    return None


def fetch_filtered_olx_jobs(
    query: str = "programista", category_id: int = 4, max_pages: int = 3
):
    limit = 40
    filtered_jobs = []

    for page in range(max_pages):
        offset = page * limit
        params = {"offset": offset, "limit": limit, "category_id": category_id}
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

            listings = data.get("data", [])
            if not listings:
                print("No more listings found.")
                break

            for item in listings:
                title = item.get("title", "")

                # 1. Filter out listings without salary
                salary = parse_salary(item)
                if not salary:
                    continue

                # 2. Filter out non-IT jobs
                if not is_it_related(title):
                    continue

                job_info = {
                    "id": item.get("id"),
                    "title": title,
                    "url": item.get("url"),
                    "city": item.get("location", {}).get("city", {}).get("name"),
                    "created_at": item.get("created_time"),
                    "salary": salary,
                }
                filtered_jobs.append(job_info)

            time.sleep(1)

        except Exception as e:
            print(f"Error fetching page {page + 1}: {e}")
            break

    return filtered_jobs


if __name__ == "__main__":
    jobs = fetch_filtered_olx_jobs(query="programista", max_pages=3)

    print(f"\nRetrieved {len(jobs)} filtered IT jobs with salary data:\n")

    for idx, job in enumerate(jobs, 1):
        print(f"{idx}. {job['title']}")
        print(f"   Location: {job['city']}")
        print(f"   Salary:   {job['salary']}")
        print(f"   URL:      {job['url']}\n")