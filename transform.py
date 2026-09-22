"""Cleaning and dimensional modeling for NYC DOHMH restaurant inspection data.

Reads the raw Socrata pull, applies column-level cleaning rules, quarantines
rows that cannot be trusted, and returns three modeled tables:

    dim_restaurant  - one row per camis (restaurant)
    fct_inspection  - one row per camis + inspection_date + inspection type
    fct_violation   - one row per violation
    quarantine      - rejected rows with a reject_reason
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

VALID_BOROS = ["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"]
VALID_FLAGS = ["Critical", "Not Critical", "Not Applicable"]
VALID_GRADES = list("ABCNPZ")
VALID_NYC_BOX = {"lat": (40.4, 41.0), "lon": (-74.3, -73.6)}

GEO_COLS = ["council_district", "community_board", "census_tract",
            "bin", "bbl", "nta"]

PROJECT_ROOT = Path(__file__).resolve().parent
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "inspections.parquet"


# --------------------------------------------------------------------------
# Structural cleaning
# --------------------------------------------------------------------------

def clean_columns(data):
    """Normalize column names: strip whitespace, lowercase."""
    data.columns = data.columns.str.strip().str.lower()
    return data


def drop_nested_columns(data):
    """Drop columns holding dicts or lists (Socrata `location`, computed regions).

    These are unhashable, so drop_duplicates fails while they are present.
    """
    nested = [c for c in data.columns
              if data[c].dropna().apply(lambda x: isinstance(x, (dict, list))).any()]
    if nested:
        logger.info("dropped %d nested columns: %s", len(nested), nested)
        data = data.drop(columns=nested)
    return data


# --------------------------------------------------------------------------
# Column cleaners
# --------------------------------------------------------------------------

def clean_camis(data):
    """Restaurant ID: string, digits only. Non-numeric values are nulled so
    quarantine_rows can reject those rows."""
    data["camis"] = data["camis"].astype("string").str.strip().replace("", pd.NA)

    bad = data["camis"].notna() & ~data["camis"].str.fullmatch(r"\d+", na=False)
    if bad.any():
        logger.warning("camis: nulling %d non-numeric values", bad.sum())
        data.loc[bad, "camis"] = pd.NA

    logger.info("camis: %d null of %d rows", data["camis"].isna().sum(), len(data))
    return data


def clean_dba(data):
    """Restaurant name: uppercase, trimmed, backfilled from the same camis."""
    data["dba"] = (data["dba"].astype("string")
                     .str.strip()
                     .str.replace(r"\s+", " ", regex=True)
                     .str.upper()
                     .replace("", pd.NA))

    before = data["dba"].isna().sum()
    data["dba"] = data["dba"].fillna(
        data.groupby("camis")["dba"].transform("first"))
    after = data["dba"].isna().sum()

    logger.info("dba: backfilled %d names, %d still null", before - after, after)
    return data


def clean_boro(data):
    """Borough: only the five valid values survive; '0' and anything else -> null."""
    data["boro"] = data["boro"].astype("string").str.strip()
    invalid = data["boro"].notna() & ~data["boro"].isin(VALID_BOROS)
    logger.info("boro: nulled %d invalid values", invalid.sum())
    data["boro"] = data["boro"].where(data["boro"].isin(VALID_BOROS), pd.NA)
    return data


def clean_building_street(data):
    """Address fields: trimmed, single-spaced, uppercased."""
    for col in ["building", "street"]:
        data[col] = (data[col].astype("string")
                       .str.strip()
                       .str.replace(r"\s+", " ", regex=True)
                       .str.upper()
                       .replace("", pd.NA))
    return data


def clean_zipcode(data):
    """Zip: 5-digit string, anything else nulled, then backfilled from same camis."""
    data["zipcode"] = data["zipcode"].astype("string").str.strip()

    valid = data["zipcode"].str.fullmatch(r"\d{5}", na=False)
    logger.info("zipcode: nulled %d invalid values",
                (data["zipcode"].notna() & ~valid).sum())
    data["zipcode"] = data["zipcode"].where(valid, pd.NA)

    data["zipcode"] = data["zipcode"].fillna(
        data.groupby("camis")["zipcode"].transform("first"))
    return data


def clean_lat_long(data):
    """Coordinates: numeric, zeros and out-of-NYC values nulled as a pair."""
    data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
    data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")

    lat_ok = data["latitude"].between(*VALID_NYC_BOX["lat"])
    lon_ok = data["longitude"].between(*VALID_NYC_BOX["lon"])
    outside = ~(lat_ok & lon_ok)

    logger.info("lat/long: nulled %d out-of-range pairs", outside.sum())
    data.loc[outside, ["latitude", "longitude"]] = np.nan
    return data


def clean_community_info(data):
    """Geography codes: strings, not numbers. Zeros treated as missing."""
    for col in GEO_COLS:
        if col not in data.columns:
            continue
        data[col] = (data[col].astype("string")
                       .str.strip()
                       .str.replace(r"\.0$", "", regex=True)
                       .replace(["", "0"], pd.NA))
    return data


def clean_phone(data):
    """Phone: exactly 10 digits -> xxx-xxx-xxxx, anything else nulled."""
    digits = (data["phone"].astype("string")
                .str.strip()
                .str.replace(r"\D", "", regex=True))

    data["phone"] = digits.where(digits.str.fullmatch(r"\d{10}", na=False))
    data["phone"] = data["phone"].str.replace(
        r"^(\d{3})(\d{3})(\d{4})$", r"\1-\2-\3", regex=True)

    logger.info("phone: %d null after formatting", data["phone"].isna().sum())
    return data


def clean_cuisine(data):
    """Cuisine: trimmed only. Values are kept as the city reports them."""
    data["cuisine_description"] = (data["cuisine_description"].astype("string")
                                     .str.strip()
                                     .str.replace(r"\s+", " ", regex=True)
                                     .replace("", pd.NA))
    return data


def clean_inspection_date(data):
    """Inspection date: datetime. 1900-01-01 means the restaurant has not been
    inspected yet, so it is flagged and the date nulled. Rejection of bad dates
    is left to quarantine_rows."""
    parsed = pd.to_datetime(data["inspection_date"], errors="coerce")

    data["is_uninspected"] = parsed == pd.Timestamp("1900-01-01")
    data["inspection_date"] = parsed.mask(data["is_uninspected"])

    unparseable = parsed.isna() & data["inspection_date"].notna()
    future = data["inspection_date"] > pd.Timestamp.today().normalize()
    logger.info("inspection_date: %d uninspected, %d unparseable, %d future",
                data["is_uninspected"].sum(), unparseable.sum(), future.sum())
    return data


def clean_inspection_type(data):
    """Split 'Cycle Inspection / Initial Inspection' into program + type."""
    parts = (data["inspection_type"]
               .astype("string")
               .str.split(" / ", n=1, expand=True)
               .reindex(columns=[0, 1])
               .astype("string"))

    data["inspection_program"] = (parts[0].str.strip()
                                    .replace("", pd.NA).astype("category"))
    data["inspection_type"] = (parts[1].str.strip()
                                 .replace("", pd.NA).astype("category"))
    return data


def clean_action(data):
    """Action text, plus a derived is_closed flag."""
    data["action"] = (data["action"].astype("string")
                        .str.strip()
                        .str.replace(r"\s+", " ", regex=True)
                        .replace("", pd.NA))

    data["is_closed"] = data["action"].str.contains(
        r"closed by dohmh", case=False, na=False)

    logger.info("action: %d closure rows", data["is_closed"].sum())
    return data


def clean_score(data):
    """Score: numeric, negatives nulled. Nulls are never filled -- 0 is the
    best possible score, so filling would misrepresent missing inspections."""
    data["score"] = pd.to_numeric(data["score"], errors="coerce")
    negatives = (data["score"] < 0).sum()
    if negatives:
        logger.warning("score: nulling %d negative values", negatives)
    data["score"] = data["score"].mask(data["score"] < 0)
    return data


def clean_grade(data):
    """Grade: only A, B, C, N, P, Z survive."""
    grade = data["grade"].astype("string").str.strip().str.upper()
    invalid = grade.notna() & ~grade.isin(VALID_GRADES)
    if invalid.any():
        logger.warning("grade: nulling %d invalid values", invalid.sum())
    data["grade"] = grade.where(grade.isin(VALID_GRADES), pd.NA).astype("category")
    return data


def clean_grade_record_date(data):
    """Secondary dates: parsed with coerce, failures counted rather than hidden."""
    for col in ["grade_date", "record_date"]:
        if col not in data.columns:
            continue
        raw = data[col]
        parsed = pd.to_datetime(raw, errors="coerce")

        failed = parsed.isna() & raw.notna()
        logger.info("%s: %d failed to parse, %d already null",
                    col, failed.sum(), raw.isna().sum())
        if failed.any():
            logger.warning("%s: sample unparseable values: %s",
                           col, raw[failed].value_counts().head(5).to_dict())

        data[col] = parsed
    return data


def clean_violation(data):
    """Violation code and description. A null code means an inspection with no
    violations recorded, which is meaningful and kept."""
    for col in ["violation_code", "violation_description"]:
        data[col] = (data[col].astype("string")
                       .str.strip()
                       .str.replace(r"\s+", " ", regex=True)
                       .replace("", pd.NA))

    code = data["violation_code"].notna()
    desc = data["violation_description"].notna()
    logger.info("violations: %d none recorded, %d code w/o description, "
                "%d description w/o code",
                (~code & ~desc).sum(), (code & ~desc).sum(), (~code & desc).sum())
    return data


def clean_critical_flag(data):
    """Critical flag limited to known values, plus a boolean is_critical."""
    flag = data["critical_flag"].astype("string").str.strip()
    data["critical_flag"] = flag.where(flag.isin(VALID_FLAGS), pd.NA).astype("category")

    data["is_critical"] = pd.Series(pd.NA, index=data.index, dtype="boolean")
    data.loc[data["critical_flag"] == "Critical", "is_critical"] = True
    data.loc[data["critical_flag"] == "Not Critical", "is_critical"] = False
    return data


# Execution order. Structural steps run in transform() before this list.
CLEANERS = (
    clean_camis,
    clean_dba,
    clean_boro,
    clean_building_street,
    clean_zipcode,
    clean_lat_long,
    clean_community_info,
    clean_phone,
    clean_cuisine,
    clean_inspection_date,
    clean_inspection_type,
    clean_action,
    clean_score,
    clean_grade,
    clean_grade_record_date,
    clean_violation,
    clean_critical_flag,
)


# --------------------------------------------------------------------------
# Quarantine
# --------------------------------------------------------------------------

def quarantine_rows(df):
    """Split out rows that cannot be trusted. Returns (kept, rejected).

    This is the single place a row's fate is decided. Cleaners fix fields;
    they never reject rows.
    """
    reasons = pd.Series(pd.NA, index=df.index, dtype="object")

    reasons = reasons.mask(df["camis"].isna(), "invalid_camis")
    reasons = reasons.mask(
        reasons.isna()
        & df["inspection_date"].isna()
        & ~df["is_uninspected"],
        "invalid_inspection_date",
    )
    reasons = reasons.mask(
        reasons.isna() & (df["inspection_date"] > pd.Timestamp.today().normalize()),
        "future_inspection_date",
    )

    bad = reasons.notna()
    rejected = df[bad].assign(reject_reason=reasons[bad])

    if len(rejected):
        logger.info("quarantined %d rows: %s",
                    len(rejected),
                    rejected["reject_reason"].value_counts().to_dict())
    else:
        logger.info("quarantined 0 rows")

    return df[~bad].copy(), rejected


# --------------------------------------------------------------------------
# Dimensional model
# --------------------------------------------------------------------------

def build_dim_restaurant(df):
    """One row per restaurant, attributes as of its most recent inspection."""
    cols = ["camis", "dba", "boro", "building", "street", "zipcode",
            "phone", "cuisine_description", "latitude", "longitude"]
    return (df.sort_values("inspection_date", na_position="first")
              .drop_duplicates("camis", keep="last")[cols]
              .reset_index(drop=True))


def build_fct_inspection(df):
    """One row per inspection. Input is violation grain, so duplicates on the
    key are the same inspection repeated once per violation."""
    keys = ["camis", "inspection_date", "inspection_program", "inspection_type"]
    cols = keys + ["action", "score", "grade", "grade_date",
                   "is_closed", "is_uninspected"]
    return df.drop_duplicates(keys)[cols].reset_index(drop=True)


def build_fct_violation(df):
    """One row per violation. Inspections with no violation contribute none."""
    keys = ["camis", "inspection_date", "inspection_program", "inspection_type"]
    cols = keys + ["violation_code", "violation_description",
                   "critical_flag", "is_critical"]
    return df.dropna(subset=["violation_code"])[cols].reset_index(drop=True)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def transform(raw_df):
    """Clean and model raw inspection data.

    Returns (dim_restaurant, fct_inspection, fct_violation, quarantine).
    """
    raw_rows = len(raw_df)
    df = raw_df.copy()

    # structural first: nested columns must go before drop_duplicates,
    # which cannot hash dicts or lists
    df = clean_columns(df)
    df = drop_nested_columns(df)

    df = df.drop_duplicates()
    dropped_dupes = raw_rows - len(df)
    logger.info("dropped %d exact duplicate rows", dropped_dupes)

    for step in CLEANERS:
        df = step(df)

    df, quarantine = quarantine_rows(df)

    # every raw row is accounted for: kept, quarantined, or an exact duplicate
    assert len(df) + len(quarantine) + dropped_dupes == raw_rows, (
        f"row reconciliation failed: {raw_rows} raw vs "
        f"{len(df)} clean + {len(quarantine)} quarantined + {dropped_dupes} dupes"
    )

    dim = build_dim_restaurant(df)
    insp = build_fct_inspection(df)
    viol = build_fct_violation(df)

    assert dim["camis"].is_unique, "dim_restaurant grain broken"
    assert not insp.duplicated(
        ["camis", "inspection_date", "inspection_program", "inspection_type"]
    ).any(), "fct_inspection grain broken"

    logger.info("dim_restaurant=%d fct_inspection=%d fct_violation=%d quarantine=%d",
                len(dim), len(insp), len(viol), len(quarantine))

    return dim, insp, viol, quarantine


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    raw = pd.read_parquet(RAW_PATH)
    dim, insp, viol, quar = transform(raw)

    print("dim_restaurant:", dim.shape)
    print("fct_inspection:", insp.shape)
    print("fct_violation: ", viol.shape)
    print("quarantine:    ", quar.shape)