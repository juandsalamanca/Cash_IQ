import pandas as pd
from src.general_preprocessing import to_numeric, safe_strip

def parse_account_header(series: pd.Series) -> pd.Series:
    s = safe_strip(series)
    return s.where(s.str.match(r"^\d+\s+-", na=False))

def extract_acct_code(name: pd.Series) -> pd.Series:
    return pd.to_numeric(name.astype(str).str.extract(r"^\s*(\d+)\s+-", expand=False), errors="coerce").astype("Int64")

def load_and_clean_gl(XERO_GL_PATH):
    df = pd.read_excel(XERO_GL_PATH)
    df.columns = df.loc[3].to_list()
    raw = df[5:].reset_index(drop=True)
    raw.columns = raw.columns.str.strip()
    raw["Date_raw"] = raw["Date"]
    raw["account_header"] = parse_account_header(raw["Date_raw"])
    raw["account_name"] = raw["account_header"].ffill()
    raw["account_code"] = extract_acct_code(raw["account_name"])
    raw["date"] = pd.to_datetime(raw["Date_raw"], errors="coerce")

    tx = raw[raw["date"].notna()].copy()

    for c in ["Debit","Credit","Running Balance"]:
        tx[c] = to_numeric(tx[c])

    # Signed movement
    tx["amount"] = tx["Debit"].fillna(0.0) - tx["Credit"].fillna(0.0)

    tx["contact"] = safe_strip(tx["Contact"])
    tx["description"] = safe_strip(tx["Description"])
    tx["related_account"] = safe_strip(tx["Related account"])
    tx["related_code"] = pd.to_numeric(tx["related_account"].str.extract(r"^\s*(\d+)\s+-", expand=False), errors="coerce").astype("Int64")

    return tx


def week_windows(date_start):

    N_ACTUAL_WEEKS = 4
    N_PROJ_WEEKS = 13
    LOOKBACK_WEEKS_TS = 52
    LOOKBACK_MONTHS_CADENCE = 12
    TOP_N_INFLOW_LINES = 30
    TOP_N_OUTFLOW_LINES = 60

    BANK_CODE_MIN = 10000
    BANK_CODE_MAX = 10999
    BANK_NAME_KEYWORDS = ["bank", "checking", "chequ", "cheque", "cash", "op ex", "opex", "operating", "savings"]

    # Anchor and week buckets
    anchor = pd.Timestamp(date_start)
    actual_starts = pd.to_datetime([anchor - pd.Timedelta(days=7*i) for i in range(N_ACTUAL_WEEKS, 0, -1)])
    proj_starts   = pd.to_datetime([anchor + pd.Timedelta(days=7*i) for i in range(0, N_PROJ_WEEKS)])
    all_starts    = pd.to_datetime(list(actual_starts) + list(proj_starts))

    hist_start = anchor - pd.Timedelta(days=7*LOOKBACK_WEEKS_TS)
    hist_starts = pd.date_range(start=hist_start, end=actual_starts[-1], freq="7D")

    cadence_start = (anchor - pd.DateOffset(months=LOOKBACK_MONTHS_CADENCE)).normalize()
    cadence_end   = (anchor - pd.Timedelta(days=1)).normalize()
    proj_end_date = (proj_starts[-1] + pd.Timedelta(days=7))

    return (anchor, actual_starts, proj_starts, all_starts, hist_starts, cadence_start, cadence_end, proj_end_date,
            TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS)