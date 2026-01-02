from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import fnmatch
import re

import pandas as pd
import yaml
from tabulate import tabulate

from src.common.io import safe_read_excel_all_sheets
from src.common.normalize import to_snake, strip_object_cols
from src.common.franchises import enrich_franchise_columns

# =========================
# BUSINESS MAPS / RULES
# =========================
STATE_BY_FRANCHISE = {
    "Gadsden": "AL",
    "Clarksville": "TN",
    "Frankfort": "KY",
    "Bowling Green": "KY",
    "Nashville": "TN",
    "Franklin": "TN",
    "Goodlettsville": "TN",
    "Green Bay": "WI",
    "Appleton": "WI",
    "Madison": "WI",
    "Cedarburg": "WI",
    "Sheboygan": "WI",
    "Racine": "WI",
    "Burlington": "WI",
    "Stevens Point": "WI",
}

CITY_FIX = {
    "Elizabethtown": "Elizabethtown City",
    "Shepherdsville": "Shepherdsville City",
    "Radcliff": "Radcliff City",
    "Georgetown": "Georgetown City",
    "Frankfort City": "Frankfort City",
    "Owenton City": "Owenton City",
}

# Output column order (tags at end, no __source_* columns)
COLUMN_ORDER = [
    "franchise", "franchise_name", "franchise_acro", "client_id",
    "full_name",
    "city", "state", "postal_code",
    "address", "address_line_2",
    "start_date", "first_carelog_date", "last_carelog_date", "date_deactivated",
    "deactivated", "deactivation_reason", "client_referrer", "referral_source_type", "city_tax",
    "location",
    "tags",
]

# =========================
# DATATYPES
# =========================
@dataclass
class RunResult:
    output_path: Path
    rows: int
    files_loaded: int
    files_rejected: int

# =========================
# HELPERS
# =========================
def _load_schema(schema_path: Path) -> dict:
    data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Schema YAML inválido (no es un dict).")
    return data


def _match_files(raw_dir: Path, patterns: list[str]) -> list[Path]:
    """
    Match files based on schema patterns (supports .xlsx and .xls etc).
    """
    files: list[Path] = []
    for pat in patterns or ["*.xlsx"]:
        files.extend(raw_dir.glob(pat))
    # de-dupe + stable sort
    return sorted({p.resolve() for p in files})


def _write_rejected(rejected_dir: Path, file_path: Path, reason: str) -> None:
    rejected_dir.mkdir(parents=True, exist_ok=True)
    out = rejected_dir / f"{file_path.stem}__rejected.csv"
    pd.DataFrame([{"source_file": file_path.name, "reason": reason}]).to_csv(
        out, index=False, encoding="utf-8-sig"
    )


def _extract_franchise_from_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive franchise from a location-like column.

    Expectation after to_snake(): one of:
      - location
      - client_location
    """
    if "location" in df.columns:
        col = "location"
    elif "client_location" in df.columns:
        col = "client_location"
    else:
        return df

    df = df.copy()
    df["franchise"] = (
        df[col]
        .astype("string")
        .str.strip()
        .str.extract(r"(\d{3})", expand=False)
        .astype("Int64")
    )
    return df


def _coerce_bool_nullable(s: pd.Series) -> pd.Series:
    return (
        s.astype("string").str.strip().str.lower()
        .map({
            "true": True, "false": False,
            "1": True, "0": False,
            "yes": True, "no": False,
            "y": True, "n": False,
            "": pd.NA, "nan": pd.NA, "none": pd.NA,
        })
        .astype("boolean")
    )


def _apply_city_tax(tags: pd.Series, franchise: pd.Series) -> pd.Series:
    def extract_city(tag: str) -> str | None:
        m = re.search(r"\bTax\s+([^,]+)", str(tag), flags=re.I)
        if not m:
            return None
        city = m.group(1).strip()
        return CITY_FIX.get(city, city)

    def rule(city: str | None, fr: str) -> str | None:
        if not city:
            return None
        if city == "None":
            if fr in {"434", "629"}:
                return "Kentucky No local Tax"
            if fr == "780":
                return "ALNL"
            return "Tax None"
        return city

    extracted = tags.astype("string").apply(extract_city)
    frs = franchise.astype("string")
    return pd.Series(
        [rule(extracted.iat[i], str(frs.iat[i])) for i in range(len(extracted))],
        dtype="string",
    )


def _fill_state_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "state" not in df.columns:
        df["state"] = pd.NA

    s = df["state"].astype("string").str.strip()
    missing = df["state"].isna() | s.eq("") | s.str.lower().isin({"nan", "none"})

    if missing.any() and "franchise_name" in df.columns:
        derived = df["franchise_name"].map(lambda x: STATE_BY_FRANCHISE.get(x, "Unknown"))
        df.loc[missing, "state"] = derived.loc[missing]

    return df


def _make_full_name(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    first = df.get("first_name", pd.Series([""] * len(df))).astype("string").fillna("").str.strip()
    last = df.get("last_name", pd.Series([""] * len(df))).astype("string").fillna("").str.strip()

    df["full_name"] = (first + " " + last).str.replace(r"\s+", " ", regex=True).str.strip()
    df.loc[df["full_name"].str.lower().isin(["", "nan", "none"]), "full_name"] = pd.NA

    df.drop(columns=["first_name", "last_name"], errors="ignore", inplace=True)
    return df


def _report_missing_client_id(df: pd.DataFrame) -> None:
    if "client_id" not in df.columns:
        print("[QUALITY] Missing client_id column entirely.")
        return

    cid = df["client_id"].astype("string").str.strip()
    miss = df["client_id"].isna() | cid.eq("") | cid.str.lower().isin({"nan", "none"})
    missing_df = df.loc[miss].copy()

    total = len(missing_df)
    print(f"[QUALITY] Missing client_id: {total}")
    if total == 0:
        return

    cols: list[str] = []
    rename: dict[str, str] = {}

    if "franchise_name" in missing_df.columns:
        cols.append("franchise_name")
        rename["franchise_name"] = "Franchise"

    if "full_name" in missing_df.columns:
        cols.append("full_name")
        rename["full_name"] = "Full Name"

    view = missing_df[cols].rename(columns=rename)
    print(tabulate(view, headers="keys", tablefmt="simple", showindex=False))


def _print_output_schema(df: pd.DataFrame, output_path: Path) -> None:
    print("FILE FOUND" if output_path.exists() else "FILE NOT FOUND")
    print(f"TOTAL ROWS: {len(df):,}")
    print(f"TOTAL COLS: {df.shape[1]:,}")
    print(f"OUTPUT: {output_path.resolve()}\n")

    schema_df = pd.DataFrame({
        "column": df.columns,
        "dtype": [str(t) for t in df.dtypes],
    })

    print("OUTPUT SCHEMA")
    print(tabulate(schema_df, headers="keys", tablefmt="pipe", showindex=True))

# =========================
# MAIN ENTRYPOINT
# =========================
def run_wellsky_clients(datalake_dir: Path, as_of: str | None = None) -> RunResult:
    schema = _load_schema(Path("config/schemas/wellsky/wellsky_clients.yaml"))

    raw_dir = datalake_dir / "raw" / "wellsky" / "clients"
    processed_dir = datalake_dir / "processed" / "wellsky" / "clients"
    rejected_dir = datalake_dir / "rejected" / "wellsky" / "clients"
    processed_dir.mkdir(parents=True, exist_ok=True)

    today = pd.Timestamp(as_of).normalize() if as_of else pd.Timestamp.today().normalize()

    patterns = schema.get("file_patterns", ["*.xlsx", "*.xls"])
    files = _match_files(raw_dir, patterns)
    if not files:
        raise FileNotFoundError(f"No input Excel files found in {raw_dir}")

    dfs: list[pd.DataFrame] = []
    rejected = 0

    required = schema.get("required_columns", []) or []

    for p in files:
        df_part, error = safe_read_excel_all_sheets(p)
        if error:
            rejected += 1
            _write_rejected(rejected_dir, p, error)
            continue

        if df_part is None or df_part.empty:
            print(f"[WARN] {p.name}: empty")
            continue

        df_part = strip_object_cols(df_part)

        # validate required columns (before snake_case)
        missing = [c for c in required if c not in df_part.columns]
        if missing:
            rejected += 1
            _write_rejected(rejected_dir, p, f"Missing required columns: {missing}")
            continue

        dfs.append(df_part)

    if not dfs:
        raise RuntimeError("All input files failed validation or were empty/corrupt.")

    df = pd.concat(dfs, ignore_index=True)

    # ---- Transformations ----

    # normalize headers to snake_case
    df.columns = [to_snake(c) for c in df.columns]

    # derive franchise from location-like field (after snake_case)
    df = _extract_franchise_from_location(df)

    # Ensure location stays TEXT (avoid "nan" strings)
    if "location" in df.columns:
        df["location"] = df["location"].astype("string")

    # client_id as nullable int (avoid float64)
    if "client_id" in df.columns:
        df["client_id"] = pd.to_numeric(df["client_id"], errors="coerce").astype("Int64")

    # full_name (and drop first/last)
    df = _make_full_name(df)

    # enrich franchise_name / franchise_acro
    if "franchise" in df.columns:
        df = enrich_franchise_columns(df, franchise_col="franchise")

    # dates (guarded)
    for c in ("start_date", "first_carelog_date", "last_carelog_date", "date_deactivated"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # deactivated boolean (guarded)
    if "deactivated" in df.columns:
        df["deactivated"] = _coerce_bool_nullable(df["deactivated"])

    # 60-day inactivity (guarded)
    if {"deactivated", "last_carelog_date"}.issubset(df.columns):
        active = ~df["deactivated"].fillna(False)
        old = df["last_carelog_date"].notna() & (
            df["last_carelog_date"] <= today - pd.Timedelta(days=60)
        )
        flip = active & old

        flagged = int(flip.sum())
        if flagged:
            df.loc[flip, "deactivated"] = True
            if "date_deactivated" in df.columns:
                df.loc[flip & df["date_deactivated"].isna(), "date_deactivated"] = (
                    df.loc[flip, "last_carelog_date"] + pd.Timedelta(days=60)
                )
        print(f"[TRANSFORM] 60-day inactivity flagged: {flagged}")

    # city_tax derived (guarded)
    if {"tags", "franchise"}.issubset(df.columns):
        df["city_tax"] = _apply_city_tax(df["tags"], df["franchise"])
    else:
        df["city_tax"] = pd.NA

    # state: keep input, fill if missing
    df = _fill_state_if_missing(df)

    # ---- Final output ----
    final_cols = [c for c in COLUMN_ORDER if c in df.columns]
    df = df[final_cols].copy()

    output_path = processed_dir / "wellsky_clients.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # Console QA
    _report_missing_client_id(df)
    _print_output_schema(df, output_path)

    print(f"[LOAD] rows={len(df):,} -> {output_path}")
    return RunResult(output_path, len(df), len(dfs), rejected)


if __name__ == "__main__":
    run_wellsky_clients(Path("datalake"))
