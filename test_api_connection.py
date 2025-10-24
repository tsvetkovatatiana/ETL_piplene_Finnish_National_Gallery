import requests
import json
from dotenv import load_dotenv
import os

url = "https://www.kansallisgalleria.fi/api/v1/objects"

load_dotenv()
api_key = os.getenv("API_KEY")

headers = {
    "accept": "application/json",
    "apikey": api_key
}

params = {"limit": 100, "offset": 200}

response = requests.get(url, headers=headers, params=params)

print("Status code:", response.status_code)

if response.status_code == 200:
    data = response.json()
    print("Type of data:", type(data))
    print(json.dumps(data[:5], indent=2))
else:
    print("Error:", response.text)
