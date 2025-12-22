# src/transforms/salesforce/impressions.py
import pandas as pd
import numpy as np
from pathlib import Path

from src.common.logging import info, warn, ok
from src.common.franchise import enrich_franchise_columns
from src.common.paths import raw_dir, processed_dir, rejected_dir

from src.common.io import read_csv_robust
from src.common.dates import to_date
from src.common.paths import processed_source_dir
from src.common.casting import to_int_nullable
from src.common.normalize import normalize_name
from src.common.crypto import md5_id
from src.common.strings import snake_case_columns

SOURCE = "salesforce"
DATASET = "impressions"


def _get_digit_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    if col_name in df.columns:
        return pd.to_numeric(df[col_name], errors="coerce")
    return pd.Series(index=df.index, dtype="float64")


def pick_latest_csv(folder: Path) -> Path:
    files = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No CSV files found in {folder}")
    return files[0]


def run_impressions_etl(accounts_clean_csv: Path, input_csv: Path | None = None) -> pd.DataFrame:
    in_dir = raw_dir(SOURCE, DATASET)
    out_dir = processed_dir(SOURCE)
    rej_dir = rejected_dir(SOURCE)

    out_dir.mkdir(parents=True, exist_ok=True)
    rej_dir.mkdir(parents=True, exist_ok=True)
    
    if input_csv is None:
        input_csv = pick_latest_csv(in_dir)

    info(f"start etl source={SOURCE} dataset={DATASET}")
    info(f"input={input_csv}")
    info(f"accounts_xref={accounts_clean_csv}")

    # --- load accounts xref
    if not accounts_clean_csv.exists():
        raise FileNotFoundError(f"Missing accounts file: {accounts_clean_csv}")

    xref = pd.read_csv(
        accounts_clean_csv,
        usecols=["account_id", "account_name_norm", "franchise_number", "account_owner_id", "account_owner"],
        dtype=str,
    )
    xref["franchise_number"] = to_int_nullable(xref["franchise_number"])
    xref = xref.drop_duplicates()
    info(f"accounts_xref rows={len(xref):,}")

    xref["base_name_key"] = xref["account_name_norm"].str.replace(r"\s+\d{3}$", "", regex=True)
    multi_fr = (
        xref.groupby("base_name_key")["franchise_number"]
        .nunique()
        .reset_index(name="frcount")
    )
    multi_fr_bases = set(multi_fr.loc[multi_fr["frcount"] > 1, "base_name_key"])

    # --- load raw
    df = read_csv_robust(input_csv)
    info(f"raw rows={len(df):,} cols={len(df.columns)}")

    df["account_name_raw"] = df.get("Company / Account", "").astype(str).str.strip()
    df = df.drop(columns=["Account Owner"], errors="ignore")

    df["date"] = to_date(df.get("Date", ""))

    d2 = _get_digit_series(df, "Franchise Digit 2")
    d3 = _get_digit_series(df, "Franchise Digit 3")
    d4 = _get_digit_series(df, "Franchise Digit 4")
    df["franchise_code"] = (d2 * 100 + d3 * 10 + d4).round().astype("Int64")

    # franchise enrichment via common
    df = enrich_franchise_columns(
        df.rename(columns={"franchise_code": "franchise"}),
        franchise_col="franchise",
    )
    df = df.rename(columns={"franchise": "franchise_code"})  # keep output name

    before = len(df)
    df = df[df["franchise_name"].notna()].copy()
    removed = before - len(df)
    if removed:
        warn(f"removed invalid franchise rows={removed:,}")

    # normalize + multi-franchise suffix rule
    df["account_name_norm_base"] = normalize_name(df.get("Company / Account", ""))
    base = df["account_name_norm_base"]
    fr = df["franchise_code"]
    df["account_name_norm"] = base.copy()

    mask_valid = base.notna() & fr.notna()
    mask_multi = mask_valid & base.isin(multi_fr_bases)
    df.loc[mask_multi, "account_name_norm"] = (
        base[mask_multi] + " " + fr[mask_multi].astype("int64").astype(str)
    )

    df["household_flag"] = df["account_name_norm"].str.contains(
        "HOUSEHOLD", case=False, na=False
    ).astype("Int8")

    # join accounts
    df = df.merge(
        xref[["account_id", "account_name_norm", "franchise_number", "account_owner_id", "account_owner"]],
        how="left",
        left_on=["account_name_norm", "franchise_code"],
        right_on=["account_name_norm", "franchise_number"],
        suffixes=("", "_dim"),
    )

    df = df.drop(columns=["franchise_number", "account_name_norm_base"], errors="ignore")
    df = df.drop_duplicates().reset_index(drop=True)

    df = df.rename(columns={"account_name_raw": "account_name"})

    # snake case cols
    df = snake_case_columns(df)

    if "account_owner_id" not in df.columns:
        df["account_owner_id"] = np.nan
    if "account_owner" not in df.columns:
        df["account_owner"] = np.nan

    df["hcc_id"] = df["account_owner_id"]
    df["hcc_name"] = df["account_owner"]
    df = df.drop(columns=["account_owner_id", "account_owner"], errors="ignore")

    cols_to_remove = [
        "assigned_role",
        "franchise_digit_1",
        "franchise_digit_2",
        "franchise_digit_3",
        "franchise_digit_4",
        "task",
        "closed",
        "opportunity_stage",
        "contact_lead_source",
        "opportunity_lead_source",
        "account_name_norm",
    ]
    df = df.drop(columns=[c for c in cols_to_remove if c in df.columns], errors="ignore")

    def build_impression_id(row):
        key = "|".join(
            [
                str(row.get("date", "")),
                str(row.get("account_id", "")),
                str(row.get("subject", "")),
                str(row.get("activity_type", "")),
                str(row.get("franchise_code", "")),
            ]
        )
        return md5_id(key)

    df["impression_id"] = df.apply(build_impression_id, axis=1)

    priority = [
        "impression_id",
        "date",
        "account_id",
        "account_name",
        "household_flag",
        "franchise_code",
        "franchise_name",
        "franchise_acro",
        "hcc_id",
        "hcc_name",
        "activity_type",
        "subject",
        "event_status",
        "status",
        "assigned",
    ]
    ordered = [c for c in priority if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]
    df = df.loc[:, ~df.columns.duplicated()].copy()

    out_csv = out_dir / "impressions_clean.csv"
    df.to_csv(out_csv, index=False)
    ok(f"done output={out_csv} rows={len(df):,} cols={len(df.columns)}")

    return df
