from __future__ import annotations
from pathlib import Path
import pandas as pd

def read_excel_all_sheets(path: Path) -> pd.DataFrame:
    x = pd.ExcelFile(path)
    parts: list[pd.DataFrame] = []
    for sh in x.sheet_names:
        d = pd.read_excel(path, sheet_name=sh, dtype=str, keep_default_na=False)
        d["__source_file"] = path.name
        d["__source_sheet"] = sh
        parts.append(d)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def safe_read_excel_all_sheets(path: Path) -> tuple[pd.DataFrame | None, str | None]:
    """
    Returns (df, error). If error is not None, df is None.
    """
    try:
        df = read_excel_all_sheets(path)
        return df, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
