import os
import time
import pandas as pd
from transform import *
from fetch_data import *
from sqlalchemy import create_engine, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table

from dotenv import load_dotenv

load_dotenv()
SUPABASE_DB_URL = os.getenv("DATABASE_URL")

engine = create_engine(SUPABASE_DB_URL)


def get_latest_updated_timestamp():
    """Return the most recent updated_at timestamp from Supabase."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(updated_at) FROM artworks;")).scalar()
        return result
    except Exception as e:
        print(f"Warning: could not query artworks table yet ({e})")
        return None


def upsert_to_supabase(df, table_name="artworks", batch_size=5000):
    """Upsert records into Supabase efficiently in batches."""
    if df.empty:
        print("No new records to upsert.")
        return

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    print(f"---Upserting {len(df)} records into {table_name} in batches of {batch_size}...")

    with engine.begin() as conn:
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size].to_dict(orient="records")

            stmt = insert(table).values(batch)

            update_dict = {
                c.name: stmt.excluded[c.name]
                for c in table.c
                if c.name not in ['object_id']  # skip the PK
            }
            update_dict["harvested_at"] = func.now()

            stmt = stmt.on_conflict_do_update(
                index_elements=['object_id'],
                set_=update_dict
            )

            conn.execute(stmt)
            print(f"   → Upserted rows {start}–{start + len(batch)}")

    print("---All batches upserted successfully.")


def run_etl():
    print("---Starting incrementing from Finnish National Gallery API...")

    latest_ts = get_latest_updated_timestamp()
    if latest_ts:
        print(f"Latest updated record in DB: {latest_ts}")
    else:
        print("No existing data found — full load will be performed.")

    all_records = []
    page = 1
    total_fetched = 0
    empty_pages = 0  # safety counter

    while total_fetched < MAX_RECORDS:
        data = fetch_page(page)
        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            print("---No more pages.")
            break

        valid_items = []
        for i in items:
            if not is_valid_record(i):
                continue
            flat = flatten_record(i)

            # Incremental filtering — skip old records
            if latest_ts and flat.get("updated_at") and flat["updated_at"] <= latest_ts:
                continue

            valid_items.append(flat)

        if not valid_items:
            empty_pages += 1
            print(f"---No new updates found on page {page}. ({empty_pages} empty pages in a row)")
            if empty_pages >= 3:
                break
        else:
            all_records.extend(valid_items)
            total_fetched += len(valid_items)
            print(f"Fetched page {page}, total new/updated: {total_fetched}")
            empty_pages = 0

        if total_fetched >= MAX_RECORDS:
            break

        page += 1
        time.sleep(0.3)

    print(f"---Finished fetching {len(all_records)} incremental records.")

    # load if something new is found
    if all_records:
        df = pd.DataFrame(all_records)
        print("----Upserting data into Supabase...")
        upsert_to_supabase(df)
        print("---Data successfully upserted to Supabase!")
    else:
        print("No new data found.")

    with engine.begin() as conn:
        conn.execute(
            text("""
                 INSERT INTO etl_log (records_fetched, latest_timestamp)
                 VALUES (:records, :ts)
                 """),
            {"records": len(all_records), "ts": latest_ts}
        )


if __name__ == "__main__":
    run_etl()