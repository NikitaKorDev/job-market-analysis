from .olx import olx    
import pracuj.pracuj as pracuj
def fetch_all_listings():
    try:
        olx_data = olx.fetch_olx_jobs()
    except Exception as e:
        print("Failed to fetch OLX listings. Proceeding.")

    try:
        pracuj_data = pracuj.run_scraper()
    except Exception as e:
        print("Failed to fetch Pracuj.pl listings. Proceeding.")