import requests
import scrapers.scraper_hub as scraper_hub
job_boards = {
    "Pracuj.pl": "https://www.pracuj.pl",
    "OLX Praca": "https://www.olx.pl/praca/",
    "JustJoin.it": "https://justjoin.it",
    "NoFluffJobs": "https://nofluffjobs.com",
    "Praca.pl": "https://www.praca.pl",
    "Bulldogjob": "https://bulldogjob.pl"
}

if __name__ == '__main__':
    scraper_hub.fetch_all_listings()

