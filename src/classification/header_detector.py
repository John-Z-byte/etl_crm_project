from __future__ import annotations

from pathlib import Path
from typing import List
import csv

import pandas as pd


def _normalize_cell(value) -> str:
    """Convert any cell value to a clean string."""
    if value is None:
        return ""
    # Handle pandas NaN
    try:
        import math
        if isinstance(value, float) and math.isnan(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def read_rows_from_csv(path: Path, max_rows: int = 50) -> List[List[str]]:
    """
    Read up to `max_rows` rows from a CSV file.
    Returns a list of rows, each row is a list of strings.
    """
    rows: List[List[str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append([_normalize_cell(cell) for cell in row])
    return rows


def read_rows_from_excel(path: Path, max_rows: int = 50) -> List[List[str]]:
    """
    Read up to `max_rows` rows from an Excel file (.xlsx / .xls).
    Uses pandas + openpyxl under the hood.
    """
    try:
        df = pd.read_excel(
            path,
            header=None,   # we do NOT assume which row is the header
            nrows=max_rows,
            engine="openpyxl",
        )
    except ImportError as exc:
        raise ImportError(
            "Reading Excel files requires 'openpyxl'. "
            "Install it with: pip install openpyxl"
        ) from exc

    rows: List[List[str]] = []
    for _, row in df.iterrows():
        rows.append([_normalize_cell(val) for val in row.tolist()])
    return rows


def read_rows(path: Path, max_rows: int = 50) -> List[List[str]]:
    """
    Read up to `max_rows` rows from a file (CSV or Excel).
    Returns a list of rows, each row is a list of strings.

    This function does NOT decide which row is the "header".
    The classifier/matcher will inspect these rows and decide
    which one matches the required_columns of a schema.
    """
    ext = path.suffix.lower()

    if ext == ".csv":
        return read_rows_from_csv(path, max_rows=max_rows)

    if ext in (".xlsx", ".xls"):
        return read_rows_from_excel(path, max_rows=max_rows)

    raise ValueError(f"Unsupported file extension: {ext} for path {path}")
