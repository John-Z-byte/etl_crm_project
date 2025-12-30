# src/common/normalize.py
import numpy as np
import pandas as pd
import unicodedata


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza headers del DataFrame a snake_case técnico.
    Solo afecta nombres de columnas, no valores.
    """
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" / ", "_", regex=False)
          .str.replace(" ", "_")
          .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def normalize_name(s: pd.Series) -> pd.Series:
    """
    Normaliza texto humano (nombres, etiquetas).
    NO usar para IDs, fechas, enums o claves técnicas.
    """
    s = s.copy()
    mask = s.notna()
    s = s.where(mask)

    s.loc[mask] = (
        s.loc[mask]
          .astype(str)
          .str.strip()
          .str.replace(r"\s+", " ", regex=True)
          .str.upper()
    )

    s.loc[mask] = s.loc[mask].apply(
        lambda x: unicodedata.normalize("NFKD", x)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

    garbage_mask = s.loc[mask].str.fullmatch(r"[-_.]+")
    s.loc[mask] = s.loc[mask].where(~garbage_mask, np.nan)

    s = s.replace({"": np.nan})
    return s
