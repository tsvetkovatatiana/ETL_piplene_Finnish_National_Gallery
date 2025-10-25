import os
import time
import pandas as pd
from transform import *
from fetch_data import *
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()
SUPABASE_DB_URL = os.getenv("DATABASE_URL")

engine = create_engine(SUPABASE_DB_URL)

def run_etl():
    print("---Starting data extraction from FNG API...")
    all_records = []
    page = 1
    total_fetched = 0

    while total_fetched < MAX_RECORDS:
        data = fetch_page(page)
        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            print("---No more pages.")
            break

        # Filter + transform
        valid_items = [flatten_record(i) for i in items if is_valid_record(i)]
        all_records.extend(valid_items)

        total_fetched += len(valid_items)
        print(f"Fetched page {page}, total valid records: {total_fetched}")

        if total_fetched >= MAX_RECORDS:
            break

        page += 1
        time.sleep(0.3)

    print(f"---Finished fetching {len(all_records)} records.")
    df = pd.DataFrame(all_records)


    print("----Loading data into Supabase...")
    df.to_sql(
        "artworks",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )
    print("---Data successfully uploaded to Supabase!")


if __name__ == "__main__":
    run_etl()