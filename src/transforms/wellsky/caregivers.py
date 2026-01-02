# src/transforms/wellsky/caregivers.py
from __future__ import annotations

from datetime import date

import pandas as pd

from src.common.paths import raw_source_dir, processed_source_dir
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
    """
    Normalize Inactive to nullable boolean.
    Treat anything truthy as True; everything else -> False.
    """
    if "inactive" not in df.columns:
        return df

    truthy = {"true", "t", "yes", "y", "1"}
    falsy = {"false", "f", "no", "n", "0", ""}

    s = df["inactive"].astype("string").str.strip().str.lower().fillna("")
    inactive_bool = s.isin(truthy)

    # Unknown tokens -> False (keeps behavior predictable)
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
    # Accept both naming variants
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
    """
    If termination_date is in the future: clear it and set inactive=False.
    """
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
    """
    Hard invariant:
    If termination_date exists => inactive must be True.
    """
    if not {"inactive", "termination_date"}.issubset(df.columns):
        return df

    term = pd.to_datetime(df["termination_date"], errors="coerce")
    bad = (~df["inactive"]) & term.notna()
    if bad.any():
        df.loc[bad, "inactive"] = True
    return df


def report_missing_caregiver_id(df: pd.DataFrame, limit: int = 25) -> None:
    if "caregiver_id" not in df.columns:
        return

    s = df["caregiver_id"].astype("string").str.strip().fillna("")
    missing = df[s.eq("")].copy()

    if missing.empty:
        return

    print(f"[QUALITY] Caregivers missing ID: {len(missing)}")
    cols = [c for c in ["franchise", "franchise_name", "first_name", "last_name", "hire_date"] if c in missing.columns]
    print(missing[cols].head(limit))


# ======================
# RUN
# ======================
def run():
    source = "wellsky"

    in_folders = [raw_source_dir(source) / "caregivers"]
    out_path = processed_source_dir(source) / "wellsky" / "caregivers" / "wellsky_caregivers.csv"

    # Base ETL (schema-only columns + franchise enrich)
    run_wellsky_job(
        in_folders=in_folders,
        out_path=out_path,
        schema_path="config/schemas/wellsky/wellsky_caregivers.yaml",
        franchise_source_col="location",
    )

    # Load for caregiver-specific rules
    df = pd.read_csv(out_path)

    # Apply rules (order matters)
    df = filter_future_hires(df)
    df = normalize_inactive(df)
    df = reconcile_active_vs_termination(df)
    df = detect_admin_staff(df)
    df = fix_future_termination(df)
    df = enforce_no_active_with_termination(df)

    report_missing_caregiver_id(df)

    # Write final
    df.to_csv(out_path, index=False)
    print(f"[OK] caregivers finalized -> {out_path}")


if __name__ == "__main__":
    run()
