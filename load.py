import json
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from transform import RAW_PATH, transform
load_dotenv()

logger = logging.getLogger(__name__)

TRUNCATE_ORDER = ["fct_violation", "fct_inspection", "quarantine", "dim_restaurant"]

# Parent first: the facts cannot reference a restaurant that is not there yet.
LOAD_ORDER = ["dim_restaurant", "fct_inspection", "fct_violation", "quarantine"]

def connection_status():
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_database = os.getenv("POSTGRES_DB")
    postgres_port = os.getenv("POSTGRES_PORT")

    missing = [k for k, v in {"POSTGRES_USER": postgres_user, "POSTGRES_PASSWORD": postgres_password,
                             "POSTGRES_DB":postgres_database, "POSTGRES_HOST": postgres_host,
                             "POSTGRES_PORT": postgres_port}.items() if not v]

    if missing:
        raise ValueError(f"Environment variables not set correctly. {missing}")

    return create_engine(f'postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')


def prepare_quarantine(quarantine):
    """Reshape quarantine rows to match the quarantine table.

    The rejected rows keep every original column, so the extras are packed
    into a JSONB column rather than discarded -- the point of quarantine is
    to be able to look at exactly what was rejected.
    """
    if quarantine.empty:
        return pd.DataFrame(columns=["camis", "dba", "inspection_date",
                                     "reject_reason", "raw_record"])

    out = pd.DataFrame({
        "camis": quarantine["camis"].astype("string"),
        "dba": quarantine["dba"].astype("string"),
        "inspection_date": quarantine["inspection_date"].astype("string"),
        "reject_reason": quarantine["reject_reason"].astype("string"),
    })
    out["raw_record"] = [
        json.dumps(rec, default=str)
        for rec in quarantine.to_dict(orient="records")
    ]
    return out


def normalize_for_load(df):
    """Categoricals and pandas nullable types confuse the driver, so flatten
    them to plain objects with None for nulls."""
    df = df.copy()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype("object")
    return df.astype(object).where(pd.notna(df), None)


def load_tables(engine, tables):
    """Truncate and reload every table in one transaction.

    tables: dict of table name -> DataFrame
    """
    with engine.begin() as conn:  # commits on success, rolls back on error
        for name in TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE {name} RESTART IDENTITY CASCADE"))
        logger.info("truncated %d tables", len(TRUNCATE_ORDER))

        for name in LOAD_ORDER:
            df = normalize_for_load(tables[name])
            df.to_sql(name, conn, if_exists="append", index=False,
                      method="multi", chunksize=5_000)
            logger.info("loaded %-15s %7d rows", name, len(df))


def verify(engine, tables):
    """Confirm what landed in Postgres matches what was sent."""
    with engine.connect() as conn:
        for name in LOAD_ORDER:
            actual = conn.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar()
            expected = len(tables[name])
            status = "OK " if actual == expected else "MISMATCH"
            logger.info("%s %-15s expected=%d actual=%d",
                        status, name, expected, actual)
            if actual != expected:
                raise RuntimeError(
                    f"{name}: expected {expected} rows, found {actual}")


def main():
    raw = pd.read_parquet(RAW_PATH)
    dim, insp, viol, quarantine = transform(raw)

    tables = {
        "dim_restaurant": dim,
        "fct_inspection": insp,
        "fct_violation": viol,
        "quarantine": prepare_quarantine(quarantine),
    }

    engine = connection_status()
    load_tables(engine, tables)
    verify(engine, tables)
    logger.info("load complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    main()






