import pandas as pd

def to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",","",regex=False).str.replace("$","",regex=False).str.strip(), errors="coerce")

def safe_strip(s: pd.Series) -> pd.Series:
    return s.astype(str).fillna("").str.strip()

def monday_week_start(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")

