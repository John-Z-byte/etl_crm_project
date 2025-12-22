import numpy as np
import pandas as pd
import unicodedata

def normalize_name(s: pd.Series) -> pd.Series:
    s = s.copy()
    mask = s.notna()
    s = s.where(mask)

    s[mask] = (
        s[mask].astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.upper()
    )

    s[mask] = s[mask].apply(
        lambda x: unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("ascii")
    )

    garbage_mask = s[mask].str.fullmatch(r"[-_.]+")
    s[mask] = s[mask].where(~garbage_mask, np.nan)

    s = s.replace({"": np.nan})
    return s
