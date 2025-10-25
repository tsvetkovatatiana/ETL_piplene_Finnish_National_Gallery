import requests


API_URL = "https://www.kansallisgalleria.fi/api/v1/objects"

MAX_RECORDS = 80_000
PAGE_SIZE = 1000


def fetch_page(page: int, page_size: int = PAGE_SIZE):
    params = {
        "page": page,
        "limit": page_size
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()
