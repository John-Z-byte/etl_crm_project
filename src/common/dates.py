import pandas as pd

def to_date(s: pd.Series) -> pd.Series:
    try:
        dt = pd.to_datetime(s, errors="coerce", format="mixed")
    except TypeError:
        dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date
