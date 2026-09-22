import requests
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()
NYC_RESTURANT_ENDPOINT_DATA="https://data.cityofnewyork.us/resource/43nn-pn8j.json"


def get_data(total_records = 294841, page_size = 1000):
    headers={
        "X-App-Token":os.getenv("APP_TOKEN")
    }
    all_records = []
    offset = 0
    params = {
        "$offset": 0,
        "$limit": 1000
        }

    while True:
        params.update({"$offset": offset , "$limit": page_size})
        response = requests.get(NYC_RESTURANT_ENDPOINT_DATA, headers=headers, timeout=40, params=params)
        response.raise_for_status()
        page = response.json()
        all_records.extend(page)
        print(f"Total Records received: {len(page)}, total rows: {len(all_records)}, offset: {offset}")
        if len(page) < page_size:
            break
        offset = len(all_records)

    return all_records


DATA_LOCATION = Path("data/raw")
DATA_LOCATION.mkdir(parents=True, exist_ok=True)
records = get_data(page_size=50000)
df = pd.DataFrame(records)
df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
df.to_parquet(DATA_LOCATION / "inspections.parquet", index=False)





