import json
from datetime import date
import hashlib
import duckdb
import pandas as pd

import scrapers.scraper_hub as scraper_hub

DB_FILE = 'job_listings.duckdb'


def safe_float(val) -> float | None:
    """Safely converts salary values (numbers or string representations) to float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        # Strip spaces and currency symbols, keep digits and dots
        cleaned = ''.join(c for c in val if c.isdigit() or c == '.')
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None
    return None


def get_job_id(item: dict) -> str | None:
    """Extracts a unique job identifier from common keys, with MD5 fallback."""
    possible_keys = [
        'job_id', 'id', 'offer_id', 'url', 'link', 'href', 
        'offer_url', 'job_url', 'redirect_url'
    ]
    for key in possible_keys:
        val = item.get(key)
        # Check non-null and non-empty string (handling numeric ID 0 safely)
        if val is not None and str(val).strip() != '':
            return str(val).strip()
    
    # Fallback: Hash title + company if no explicit ID/URL key is found
    title = item.get('title') or item.get('position')
    company = item.get('company') or item.get('employer')
    if title and company:
        return hashlib.md5(f"{title}_{company}".encode('utf-8')).hexdigest()
    
    return None


def extract_all_job_dicts(obj) -> list[dict]:
    """Recursively traverses any nested dictionary/list structure to find raw job dicts."""
    results = []
    if isinstance(obj, list):
        for item in obj:
            results.extend(extract_all_job_dicts(item))
    elif isinstance(obj, dict):
        # Common keys present in individual job postings
        job_indicators = {
            'title', 'position', 'url', 'link', 'href', 
            'job_id', 'id', 'offer_id', 'company', 'employer'
        }
        has_indicator = any(key in obj for key in job_indicators)
        
        # If this dict looks like an individual job listing rather than a board wrapper
        if has_indicator and not any(isinstance(v, list) and v and isinstance(v[0], dict) for v in obj.values()):
            results.append(obj)
        else:
            # Traversal for wrappers like {"justjoin": [...], "pracuj": {"data": [...]}}
            for value in obj.values():
                results.extend(extract_all_job_dicts(value))
    return results


def parse_and_flatten_data(raw_data) -> tuple[dict | list, list[dict]]:
    """Decodes JSON, recursively extracts job records, and standardizes schema keys."""
    data = raw_data

    # Handle doubly-encoded JSON strings safely
    while isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            break

    # Recursively locate all job dictionary records across all scrapers
    raw_listings = extract_all_job_dicts(data)

    cleaned_listings = []
    for item in raw_listings:
        if not isinstance(item, dict):
            continue

        job_id = get_job_id(item)
        if not job_id:
            continue  # Skip records lacking any ID, URL, or title/company

        # Extract board name
        board = item.get('job_board') or item.get('board') or item.get('source') or ''

        # Normalize skills / technologies into a list of strings
        techs = item.get('technologies') or item.get('skills') or item.get('tags') or []
        if isinstance(techs, str):
            techs = [t.strip() for t in techs.split(',') if t.strip()]
        elif isinstance(techs, list):
            techs = [str(t).strip() for t in techs if t]
        else:
            techs = []

        cleaned_listings.append({
            'job_id': str(job_id),
            'job_board': str(board),
            'title': str(item.get('title') or item.get('position') or ''),
            'company': str(item.get('company') or item.get('employer') or ''),
            'salary_min': safe_float(item.get('salary_min') or item.get('salaryFrom') or item.get('salary_from')),
            'salary_max': safe_float(item.get('salary_max') or item.get('salaryTo') or item.get('salary_to')),
            'technologies': techs
        })

    return data, cleaned_listings


def run_scd2_pipeline(db_path: str, listings: list[dict]):
    """Executes Slowly Changing Dimension Type 2 (SCD2) incremental merge in DuckDB."""
    if not listings:
        print("No valid listings found in current batch to process.")
        return

    today = date.today().isoformat()
    conn = duckdb.connect(db_path)

    # 1. Create target dimension table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_job_listings (
            surrogate_key VARCHAR PRIMARY KEY,
            job_id VARCHAR NOT NULL,
            job_board VARCHAR,
            title VARCHAR,
            company VARCHAR,
            salary_min DOUBLE,
            salary_max DOUBLE,
            technologies VARCHAR[],
            hash_diff VARCHAR,
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        );
    """)

    # 2. Stage incoming batch into DuckDB memory space using Pandas
    df_staged = pd.DataFrame(listings)
    conn.register("raw_staged", df_staged)

    # 3. Create a staging view with an MD5 hash across tracked attributes
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW v_staged_listings AS
        SELECT 
            job_id,
            job_board,
            title,
            company,
            salary_min,
            salary_max,
            technologies,
            md5(concat_ws('|', 
                COALESCE(title, ''), 
                COALESCE(company, ''), 
                COALESCE(CAST(salary_min AS VARCHAR), ''), 
                COALESCE(CAST(salary_max AS VARCHAR), ''),
                COALESCE(array_to_string(technologies, ','), '')
            )) AS hash_diff,
            DATE '{today}' AS effective_from
        FROM raw_staged;
    """)

    # 4. Expire currently active records in DB if attributes changed today
    conn.execute(f"""
        UPDATE dim_job_listings
        SET 
            effective_to = DATE '{today}' - INTERVAL 1 DAY,
            is_current = FALSE
        WHERE is_current = TRUE
          AND job_id IN (
              SELECT s.job_id 
              FROM v_staged_listings s
              JOIN dim_job_listings t ON s.job_id = t.job_id AND t.is_current = TRUE
              WHERE s.hash_diff != t.hash_diff
          );
    """)

    # 5. Insert new records (brand-new listings OR updated versions)
    conn.execute(f"""
        INSERT INTO dim_job_listings
        SELECT 
            md5(s.job_id || '_' || CAST(s.effective_from AS VARCHAR)) AS surrogate_key,
            s.job_id,
            s.job_board,
            s.title,
            s.company,
            s.salary_min,
            s.salary_max,
            s.technologies,
            s.hash_diff,
            s.effective_from,
            DATE '9999-12-31' AS effective_to,
            TRUE AS is_current
        FROM v_staged_listings s
        LEFT JOIN dim_job_listings t 
            ON s.job_id = t.job_id AND t.is_current = TRUE
        WHERE t.job_id IS NULL             -- Brand-new listings
           OR s.hash_diff != t.hash_diff;  -- Updated listings
    """)

    conn.close()
    print(f"[{today}] SCD Type 2 ETL pipeline completed successfully.")


if __name__ == '__main__':
    print("Fetching listings from scrapers...")
    raw_data = scraper_hub.fetch_all_listings()

    # Decode JSON & normalize schema
    decoded_data, clean_listings = parse_and_flatten_data(raw_data)

    # Save raw JSON backup
    with open('job_data_raw.json', 'w', encoding='utf-8') as file:
        json.dump(decoded_data, file, indent=2, ensure_ascii=False)
        file.write('\n')

    print(f"Raw data backed up. Processed {len(clean_listings)} standardized job listings.")

    # Run DuckDB SCD Type 2 Ingestion
    run_scd2_pipeline(DB_FILE, clean_listings)