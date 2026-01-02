# src/common/wellsky_base.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml
from tabulate import tabulate

from src.common.normalize import normalize_headers
from src.common.franchises import enrich_franchise_columns


# -------------------------
# Schema loading
# -------------------------
def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _norm_col(name: str) -> str:
    """
    Convert schema column labels (often Title Case w/ spaces) into
    the same style that normalize_headers produces.
    """
    # normalize_headers already exists; we just need a deterministic approximation
    s = str(name).strip().lower()
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    s2 = "".join(out).strip("_")
    while "__" in s2:
        s2 = s2.replace("__", "_")
    return s2


@dataclass(frozen=True)
class WellSkySchema:
    required: list[str]
    optional: list[str]
    file_patterns: list[str]
    allow_extra_columns: bool


def load_wellsky_schema(schema_path: Path) -> WellSkySchema:
    cfg = _load_yaml(schema_path)
    return WellSkySchema(
        required=[_norm_col(c) for c in (cfg.get("required_columns", []) or [])],
        optional=[_norm_col(c) for c in (cfg.get("optional_columns", []) or [])],
        file_patterns=list(cfg.get("file_patterns", []) or ["*.xlsx", "*.xls"]),
        allow_extra_columns=bool(cfg.get("allow_extra_columns", True)),
    )


# -------------------------
# File IO
# -------------------------
def _find_files(folders: Iterable[Path], patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for folder in folders:
        if not folder.exists():
            continue
        for pat in patterns:
            files.extend(sorted(folder.glob(pat)))
    # de-dupe preserve order
    seen: set[Path] = set()
    out: list[Path] = []
    for f in files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def read_concat_excels(
    folders: list[Path],
    *,
    patterns: list[str],
    sheet_name: str | int | None = 0,
) -> pd.DataFrame:
    files = _find_files(folders, patterns)
    if not files:
        return pd.DataFrame()

    dfs: list[pd.DataFrame] = []
    for fp in files:
        df = pd.read_excel(fp, sheet_name=sheet_name)
        df = normalize_headers(df)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True, sort=False)


# -------------------------
# Franchise handling
# -------------------------
def derive_franchise_left3(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
    """
    franchise = VALUE(LEFT(source_col, 3))
    Assumes franchise id is always 3 digits.
    """
    if source_col not in df.columns:
        return df

    left3 = df[source_col].astype("string").str.strip().str.slice(0, 3)
    df["franchise"] = pd.to_numeric(left3, errors="coerce").astype("Int64")
    return df


# -------------------------
# Selection + ordering
# -------------------------
def _select_schema_columns_only(df: pd.DataFrame, schema_cols: list[str]) -> pd.DataFrame:
    keep = [c for c in schema_cols if c in df.columns]
    return df[keep].copy()


def _order_franchise_first(df: pd.DataFrame, schema_cols: list[str]) -> pd.DataFrame:
    start = [c for c in ["franchise", "franchise_name", "franchise_acro"] if c in df.columns]
    schema_rest = [c for c in schema_cols if c in df.columns and c not in start]
    rest = [c for c in df.columns if c not in (set(start) | set(schema_rest))]
    return df[start + schema_rest + rest]


# -------------------------
# Public runner
# -------------------------
def run_wellsky_job(
    *,
    in_folders: list[Path],
    out_path: Path,
    schema_path: Path,
    franchise_source_col: str,  # e.g. "location" or "client_location"
    sheet_name: str | int | None = 0,
    drop_unmapped_franchise: bool = True,
) -> None:
    out_path = Path(out_path)
    schema = load_wellsky_schema(Path(schema_path))

    df = read_concat_excels(in_folders, patterns=schema.file_patterns, sheet_name=sheet_name)
    if df.empty:
        print("FILE NOT FOUND")
        for p in in_folders:
            print(f"Path: {p}")
        return

    # Validate required columns exist (post-normalization)
    missing = [c for c in schema.required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Derive franchise from LEFT(3) and enrich
    df = derive_franchise_left3(df, franchise_source_col)
    if "franchise" in df.columns:
        df = enrich_franchise_columns(df, franchise_col="franchise")

        # keep only mapped franchises
        if drop_unmapped_franchise and {"franchise_name", "franchise_acro"}.issubset(df.columns):
            df = df[
                df["franchise"].notna()
                & df["franchise_name"].notna()
                & df["franchise_acro"].notna()
            ].copy()

    # Select ONLY schema columns (required + optional) plus franchise trio
    schema_cols = schema.required + schema.optional
    wanted = ["franchise", "franchise_name", "franchise_acro"] + schema_cols
    df = _select_schema_columns_only(df, wanted)

    # Order columns starting with franchise trio
    df = _order_franchise_first(df, wanted)

    # Write + print schema
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print("FILE FOUND")
    print(f"TOTAL ROWS: {df.shape[0]:,}")
    print(f"TOTAL COLS: {df.shape[1]:,}")
    print(f"OUTPUT: {out_path}")

    schema_df = pd.DataFrame({"column": df.columns, "dtype": [str(df[c].dtype) for c in df.columns]})
    print("\nOUTPUT SCHEMA")
    print(tabulate(schema_df, headers="keys", tablefmt="github", showindex=True))
