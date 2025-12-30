from __future__ import annotations
import re
import pandas as pd

def to_snake(s: str) -> str:
    s = str(s).strip().replace("\u00A0", " ")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_").replace("/", "_").replace("*", "")
    return s.lower()

def strip_object_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str).str.replace("\u00A0", " ").str.strip()
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-compatible helper used by older modules.
    Normaliza los headers a snake_case.
    """
    df = df.copy()
    df.columns = [to_snake(c) for c in df.columns]
    return df
