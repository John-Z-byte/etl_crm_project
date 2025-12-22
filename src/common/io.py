from pathlib import Path
import pandas as pd

def read_csv_robust(path: Path) -> pd.DataFrame:
    for enc in ["utf-8", "cp1252", "latin1"]:
        try:
            df = pd.read_csv(path, encoding=enc, dtype=str, keep_default_na=True)
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception:
            continue
    raise ValueError(f"Cannot read {path}")
