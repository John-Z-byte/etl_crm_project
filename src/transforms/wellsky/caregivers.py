# src/transforms/wellsky/caregivers.py
from __future__ import annotations

import base64
import hashlib
import re
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd
from tabulate import tabulate

from src.common.paths import raw_source_dir, processed_source_dir
from src.common.schema_orders import CAREGIVERS_COL_ORDER
from src.common.wellsky_base import run_wellsky_job

AS_OF = date.today()
DAYS_60 = pd.Timedelta(days=60)

# ======================
# CAREGIVER RULES
# ======================
def filter_future_hires(df: pd.DataFrame) -> pd.DataFrame:
    if "hire_date" not in df.columns:
        return df
    hd = pd.to_datetime(df["hire_date"], errors="coerce")
    future = hd.dt.date > AS_OF
    return df[~future].copy()


def normalize_inactive(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize inactive to pandas nullable boolean."""
    if "inactive" not in df.columns:
        return df

    truthy = {"true", "t", "yes", "y", "1"}
    falsy = {"false", "f", "no", "n", "0", ""}

    s = df["inactive"].astype("string").str.strip().str.lower().fillna("")
    inactive_bool = s.isin(truthy)

    unknown = ~s.isin(truthy | falsy)
    if unknown.any():
        inactive_bool = inactive_bool.where(~unknown, False)

    df["inactive"] = inactive_bool.astype("boolean")
    return df


def reconcile_active_vs_termination(df: pd.DataFrame) -> pd.DataFrame:
    """
    60-day rule:
    - If ACTIVE but has termination_date:
        - If last care log is recent (<=60d): clear termination_date
        - If last care log is old/missing (>60d): set inactive=True
    """
    last_col = (
        "last_care_log_date"
        if "last_care_log_date" in df.columns
        else ("last_carelog_date" if "last_carelog_date" in df.columns else None)
    )
    if last_col is None:
        return df

    if not {"inactive", "termination_date"}.issubset(df.columns):
        return df

    term = pd.to_datetime(df["termination_date"], errors="coerce")
    last = pd.to_datetime(df[last_col], errors="coerce")
    thr = pd.Timestamp(AS_OF) - DAYS_60

    active_with_term = (~df["inactive"]) & term.notna()
    old_or_missing = active_with_term & (last.isna() | (last <= thr))
    recent = active_with_term & (last > thr)

    df.loc[old_or_missing, "inactive"] = True
    df.loc[recent, "termination_date"] = pd.NaT
    return df


def detect_admin_staff(df: pd.DataFrame) -> pd.DataFrame:
    if "caregiver_tags" not in df.columns:
        df["role_type"] = "Caregiver"
        return df

    mask = df["caregiver_tags"].astype("string").str.contains("admin staff", case=False, na=False)
    df["role_type"] = mask.map({True: "Admin Staff", False: "Caregiver"})
    return df


def fix_future_termination(df: pd.DataFrame) -> pd.DataFrame:
    """If termination_date is in the future: clear it and set inactive=False."""
    if "termination_date" not in df.columns:
        return df

    term = pd.to_datetime(df["termination_date"], errors="coerce")
    future = term.dt.date > AS_OF

    if future.any():
        df.loc[future, "termination_date"] = pd.NaT
        if "inactive" in df.columns:
            df.loc[future, "inactive"] = False

    return df


def enforce_no_active_with_termination(df: pd.DataFrame) -> pd.DataFrame:
    """Hard invariant: If termination_date exists => inactive must be True."""
    if not {"inactive", "termination_date"}.issubset(df.columns):
        return df

    term = pd.to_datetime(df["termination_date"], errors="coerce")
    bad = (~df["inactive"]) & term.notna()
    if bad.any():
        df.loc[bad, "inactive"] = True
    return df


def build_caregiver_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build caregiver_name from first_name + last_name (if caregiver_name not present).
    Keeps caregiver_name if it already exists.
    Drops first_name/last_name only when caregiver_name is created here.
    """
    if "caregiver_name" in df.columns:
        return df

    if {"first_name", "last_name"}.issubset(df.columns):
        first = df["first_name"].astype("string").fillna("").str.strip()
        last = df["last_name"].astype("string").fillna("").str.strip()
        df["caregiver_name"] = (first + " " + last).str.strip()
        df = df.drop(columns=["first_name", "last_name"])

    return df


# ======================
# KEY GENERATION (16 chars)
# ======================
def _norm_name(s: str) -> str:
    """Normalize ONLY for key building (does not modify caregiver_name)."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _hash16(text: str) -> str:
    """Deterministic 16-char Base32 key (A-Z2-7)."""
    h = hashlib.blake2s(text.encode("utf-8"), digest_size=10).digest()  # 80 bits
    b32 = base64.b32encode(h).decode("ascii").rstrip("=")  # A-Z2-7
    return b32[:16]


def add_caregiver_profile_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    caregiver_profile_key (deterministic, 16 chars):
      - Primary: franchise|caregiver_id
      - Fallback: franchise|normalized(caregiver_name) (only if caregiver_id blank)
    """
    if "franchise" not in df.columns:
        raise ValueError("Missing required column: franchise")

    # caregiver_id may be missing / blank
    if "caregiver_id" in df.columns:
        cid = df["caregiver_id"].astype("string").str.strip().fillna("")
    else:
        cid = pd.Series([""] * len(df), index=df.index, dtype="string")

    # caregiver_name preferred for fallback
    if "caregiver_name" in df.columns:
        cname = df["caregiver_name"].astype("string").fillna("")
    else:
        fn = df["first_name"].astype("string").fillna("") if "first_name" in df.columns else ""
        ln = df["last_name"].astype("string").fillna("") if "last_name" in df.columns else ""
        cname = (fn + " " + ln).astype("string")

    franchise = df["franchise"].astype("string").str.strip().fillna("")

    base_primary = franchise + "|" + cid
    base_fallback = franchise + "|" + cname.map(_norm_name)

    use_fallback = cid.eq("")
    base = base_primary.where(~use_fallback, base_fallback)

    df["caregiver_profile_key"] = base.map(_hash16)
    return df


# ======================
# QUALITY / PRINT
# ======================
def report_duplicate_profile_keys(df: pd.DataFrame, limit: int = 25) -> None:
    if "caregiver_profile_key" not in df.columns:
        return
    dup = df[df["caregiver_profile_key"].duplicated(keep=False)]
    if dup.empty:
        return
    print(f"[QUALITY] Duplicate caregiver_profile_key: {dup['caregiver_profile_key'].nunique()} keys / {len(dup)} rows")
    cols = [c for c in ["caregiver_profile_key", "franchise", "caregiver_id", "caregiver_name"] if c in dup.columns]
    print(dup[cols].head(limit))

def dedupe_by_profile_key_last_carelog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep 1 row per caregiver_profile_key:
      - Prefer the most recent last_care_log_date
      - Tie-breaker: keep the row with more non-null fields
    """
    if "caregiver_profile_key" not in df.columns:
        return df

    # Parse last care log to datetime for sorting
    if "last_care_log_date" in df.columns:
        last = pd.to_datetime(df["last_care_log_date"], errors="coerce")
    else:
        last = pd.Series([pd.NaT] * len(df), index=df.index)

    # Completeness score (more filled fields wins ties)
    completeness = df.notna().sum(axis=1)

    tmp = df.copy()
    tmp["_last_care_log_dt"] = last
    tmp["_completeness"] = completeness

    # Sort so "best" row is first per key
    tmp = tmp.sort_values(
        by=["caregiver_profile_key", "_last_care_log_dt", "_completeness"],
        ascending=[True, False, False],
        kind="mergesort",  # stable
    )

    before = len(tmp)
    tmp = tmp.drop_duplicates(subset=["caregiver_profile_key"], keep="first").copy()
    removed = before - len(tmp)
    if removed:
        print(f"[DEDUP] Removed {removed} duplicate rows by caregiver_profile_key (kept latest last_care_log_date).")

    return tmp.drop(columns=["_last_care_log_dt", "_completeness"])


def report_missing_caregiver_id(df: pd.DataFrame, limit: int = 25) -> None:
    if "caregiver_id" not in df.columns:
        return

    s = df["caregiver_id"].astype("string").str.strip().fillna("")
    missing = df[s.eq("")].copy()
    if missing.empty:
        return

    print(f"[QUALITY] Caregivers missing ID: {len(missing)}")
    cols = [c for c in ["franchise", "franchise_name", "caregiver_name", "hire_date"] if c in missing.columns]
    print(missing[cols].head(limit))


def print_final_schema(df: pd.DataFrame) -> None:
    schema_df = pd.DataFrame({"column": df.columns, "dtype": [str(df[c].dtype) for c in df.columns]})
    print("\nFINAL OUTPUT SCHEMA")
    print(tabulate(schema_df, headers="keys", tablefmt="github", showindex=True))


def reorder_final(df: pd.DataFrame) -> pd.DataFrame:
    existing = [c for c in CAREGIVERS_COL_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    return df[existing + rest]


# ======================
# RUN
# ======================
def run() -> None:
    source = "wellsky"

    in_folders = [raw_source_dir(source) / "caregivers"]
    out_path: Path = processed_source_dir(source) / "caregivers" / "wellsky_caregivers.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) Base ETL writes schema-normalized CSV (function does NOT take out_path in your repo)
    run_wellsky_job(
        in_folders=in_folders,
        schema_path="config/schemas/wellsky/wellsky_caregivers.yaml",
        franchise_source_col="location",
    )

    # 2) Load base output
    df = pd.read_csv(out_path)
    if df.empty:
        print("[WARN] caregivers input is empty")
        return

    # 3) Business rules
    df = filter_future_hires(df)
    df = normalize_inactive(df)
    df = reconcile_active_vs_termination(df)
    df = detect_admin_staff(df)
    df = fix_future_termination(df)
    df = enforce_no_active_with_termination(df)

    # 4) Name + key
    df = build_caregiver_name(df)
    df = add_caregiver_profile_key(df)

    # 5) Dedup by latest last care log
    df = dedupe_by_profile_key_last_carelog(df)

    # 6) Quality checks (after dedup)
    report_duplicate_profile_keys(df)
    report_missing_caregiver_id(df)

    # 7) Final order + write once
    df = reorder_final(df)
    df.to_csv(out_path, index=False)

    print(f"[OK] caregivers finalized -> {out_path}")
    print_final_schema(df)


if __name__ == "__main__":
    run()
