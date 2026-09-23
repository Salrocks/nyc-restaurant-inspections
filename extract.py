import requests
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import date
import logging
load_dotenv()
NYC_RESTURANT_ENDPOINT_DATA="https://data.cityofnewyork.us/resource/43nn-pn8j.json"
logger = logging.getLogger(__name__)


def get_data(page_size = 1000):
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
        params.update({"$offset": offset , "$limit": page_size, "$order": ":id"})
        response = requests.get(NYC_RESTURANT_ENDPOINT_DATA, headers=headers, timeout=40, params=params)
        response.raise_for_status()
        page = response.json()
        all_records.extend(page)
        logger.info("page=%d total=%d offset=%d", len(page), len(all_records), offset)
        if len(page) < page_size:
            break
        offset = len(all_records)

    return all_records

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_LOCATION = PROJECT_ROOT / "data" / "raw"

def write_raw(records, load_date=None):
    load_date = load_date or date.today().isoformat()
    partition = DATA_LOCATION / f"load_date={load_date}"
    partition.mkdir(parents=True, exist_ok=True)

    path = partition / "inspections.parquet"
    df = pd.DataFrame(records)
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
    df.to_parquet(path, index=False)

    logger.info("wrote %d rows to %s (%.1f MB)",
                len(df), path, path.stat().st_size / 1_000_000)
    return path


def latest_raw_path():
    """Path to the most recent raw partition."""
    partitions = sorted(DATA_LOCATION.glob("load_date=*"))
    if not partitions:
        raise FileNotFoundError(f"no raw partitions under {DATA_LOCATION}")
    return partitions[-1] / "inspections.parquet"

def main():
    records = get_data(page_size=50_000)
    return write_raw(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    main()



