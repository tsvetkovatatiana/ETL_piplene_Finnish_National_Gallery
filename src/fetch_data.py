import requests

from test_api_connection import headers

API_URL = "https://www.kansallisgalleria.fi/api/v1/objects"

MAX_RECORDS = 80_000
PAGE_SIZE = 1000


def fetch_page(page: int, updated_after=None):
    """Fetch one page of artworks, optionally filtered by updated_after date."""
    url = f"{API_URL}?page={page}&pageSize=100"
    if updated_after:
        url += f"&updated_after={updated_after.isoformat()}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()