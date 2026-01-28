import pandas as pd

def safe_strip(s):
    return s.astype(str).str.strip()


def to_numeric(series):
    return pd.to_numeric(series, errors="coerce")

def monday_week_start(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")


# =========================
# LOAD & CLEAN COA
# =========================

def load_and_clean_coa(COA_PATH):
    coa = pd.read_excel(COA_PATH, skiprows=3, names=["full_name","type","detail_type","description","total_balance"])
    coa = coa[(coa["type"].notna()) & (coa["full_name"].notna())].copy()
    coa["full_name"] = safe_strip(coa["full_name"])
    coa["type"] = safe_strip(coa["type"])
    coa["detail_type"] = safe_strip(coa["detail_type"].fillna(""))

    global bank_accounts
    global cc_accounts

    bank_accounts = set(coa.loc[coa["type"].eq("Bank"), "full_name"])
    cc_accounts   = set(coa.loc[coa["type"].eq("Credit Card"), "full_name"])

    return coa, bank_accounts, cc_accounts

# =========================
# LOAD GL (QB Transaction Detail by Account)
# =========================

def load_and_clean_gl(GL_PATH, coa):

    gl = pd.read_excel(
        GL_PATH,
        skiprows=4,
        names=["account_section","date","txn_type","num","name","memo","split_account","amount","balance"],
    )

    gl["account_name"] = gl["account_section"].ffill()
    gl["account_name"] = safe_strip(gl["account_name"].fillna(""))
    gl["split_account"] = safe_strip(gl["split_account"].fillna(""))

    gl["date"] = pd.to_datetime(gl["date"], errors="coerce")
    gl = gl[gl["date"].notna()].copy()

    gl["amount"] = to_numeric(gl["amount"])
    gl = gl[gl["amount"].notna()].copy()

    # attach split account type for grouping
    gl = gl.merge(
        coa[["full_name","type","detail_type"]],
        how="left",
        left_on="split_account",
        right_on="full_name",
    )
    gl.rename(columns={"type":"split_type","detail_type":"split_detail_type"}, inplace=True)
    gl["split_type"] = gl["split_type"].fillna("Unmapped")
    gl["split_detail_type"] = gl["split_detail_type"].fillna("")

    gl["week_start"] = monday_week_start(gl["date"])
    
    return gl

# =========================
# DEFINE WEEK WINDOWS
# =========================

def week_windows(date_strt):

    PROJ_WEEK1_START = pd.Timestamp(date_strt)   # Monday
    N_ACTUAL_WEEKS   = 4
    N_PROJ_WEEKS     = 13

    # Lookbacks
    LOOKBACK_WEEKS_LINE_TS = 52     # weekly time-series history
    LOOKBACK_MONTHS_CADENCE = 12    # cadence inference window
    CC_MIX_ROLLING_WEEKS    = 8     # rolling window for allocating CC payment categories
    CC_SPEND_TS_WEEKS       = 26

    # Output controls
    TOP_N_INFLOW_LINES  = 30
    TOP_N_OUTFLOW_LINES = 60
    TOP_N_CC_CATS       = 40

    actual_week_starts = pd.date_range(
        start=PROJ_WEEK1_START - pd.Timedelta(weeks=N_ACTUAL_WEEKS),
        periods=N_ACTUAL_WEEKS,
        freq="W-MON",
    )
    proj_week_starts = pd.date_range(
        start=PROJ_WEEK1_START,
        periods=N_PROJ_WEEKS,
        freq="W-MON",
    )
    all_week_starts = list(actual_week_starts) + list(proj_week_starts)

    # history for weekly TS modeling
    hist_week_starts = pd.date_range(
        start=PROJ_WEEK1_START - pd.Timedelta(weeks=LOOKBACK_WEEKS_LINE_TS),
        end=actual_week_starts[-1],
        freq="W-MON",
    )

    cadence_start = (PROJ_WEEK1_START - pd.DateOffset(months=LOOKBACK_MONTHS_CADENCE)).normalize()
    cadence_end   = (PROJ_WEEK1_START - pd.Timedelta(days=1)).normalize()
    proj_end_date = (proj_week_starts[-1] + pd.Timedelta(days=7))
    
    return (PROJ_WEEK1_START, CC_MIX_ROLLING_WEEKS, CC_SPEND_TS_WEEKS, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, 
            TOP_N_CC_CATS, actual_week_starts, proj_week_starts, all_week_starts, hist_week_starts, cadence_start, 
            cadence_end, proj_end_date)

