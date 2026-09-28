import asyncio
import json
# Use full package paths relative to the project root
from scrapers.olx.olx import fetch_olx_jobs    
from scrapers.pracuj.pracuj import fetch_pracuj_listings
from scrapers.justjoin import fetch_justjoin_listings
from scrapers.nofluffjobs import fetch_no_fluff_listings
from scrapers.praca import fetch_praca_listings
from scrapers.bulldogjobs import fetch_bulldog_jobs

def fetch_all_listings():
    olx_data = []
    pracuj_data = []

    try:
        olx_data = fetch_olx_jobs()
    except Exception as e:
        print(f"Failed to fetch OLX listings ({e}). Proceeding.")

    try:
        pracuj_data = asyncio.run(fetch_pracuj_listings(max_pages=2))
    except Exception as e:
        print(f"Failed to fetch Pracuj.pl listings ({e}). Proceeding.")

    try:
        justjoin_data_raw, justjoin_data = asyncio.run(fetch_justjoin_listings())
        try:
            with open("justjoin_raw.json", 'a') as file:
                json.dump(justjoin_data_raw, file)
        except Exception as e:
            print("Failed to write down the raw data into a file, proceeding...")
    except Exception as e:
        print("Fuck you the justjoin scraper returned an error, here it is: ", e)
    try:
        nofluff_data = fetch_no_fluff_listings()
    except Exception as e:
        print("Failed to fetch NoFluffJobs.com, proceeding/n", e)
    try:
        praca_data = fetch_praca_listings()
    except Exception as e:
        print(f"Failed to fetch data from praca.pl, proceeding. /nStacktrace: {e}")
    try:
        bulldogjobs_data = fetch_bulldog_jobs()
    except Exception as e:
        print(f"Failed to fetch data from bulldogjobs, proceeding. /nStacktrace: {e}")

    print(f"OLX Listings ({len(olx_data)}):", olx_data)
    print("\n\n\n")
    print(f"Pracuj Listings ({len(pracuj_data)}):", pracuj_data)

    return {
        "olx": olx_data,
        "pracuj": pracuj_data,
        "justjoin": justjoin_data,
        "nofluff": nofluff_data,
        "praca": praca_data,
        "bulldogjobs": bulldogjobs_data
    }