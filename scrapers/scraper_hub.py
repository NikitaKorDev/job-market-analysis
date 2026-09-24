import asyncio
# Use full package paths relative to the project root
from scrapers.olx.olx import fetch_olx_jobs    
from scrapers.pracuj.pracuj import fetch_pracuj_listings


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

    print(f"OLX Listings ({len(olx_data)}):", olx_data)
    print("\n\n\n")
    print(f"Pracuj Listings ({len(pracuj_data)}):", pracuj_data)

    return {
        "olx": olx_data,
        "pracuj": pracuj_data
    }