import pandas as pd

def to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",","",regex=False).str.replace("$","",regex=False).str.strip(), errors="coerce")

def safe_strip(s: pd.Series) -> pd.Series:
    return s.astype(str).fillna("").str.strip()

def monday_week_start(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")

# =========================
# LOAD & CLEAN COA
# =========================

def load_and_clean_coa(COA_PATH):
    coa = pd.read_excel(COA_PATH, skiprows=3)
    # If there is an account # we concatenate it with the account name
    if "Account #" in coa.columns:
        coa["Account #"] = coa["Account #"].fillna('')
        coa["Full name"] = coa["Account #"].astype(str) + " " + coa["Full name"]
        coa = coa.drop(columns=["Account #"])
    coa.columns = ["full_name","type","detail_type","description","total_balance"]
    coa = coa[(coa["type"].notna()) & (coa["full_name"].notna())].copy()
    coa["full_name"] = safe_strip(coa["full_name"])
    coa["type"] = safe_strip(coa["type"])
    coa["detail_type"] = safe_strip(coa["detail_type"].fillna(""))

    bank_accounts = set(coa.loc[coa["type"].eq("Bank"), "full_name"])
    bank_accounts.add("Undeposited Funds")
    cc_accounts   = set(coa.loc[coa["type"].eq("Credit Card"), "full_name"])

    return coa, bank_accounts, cc_accounts

# =========================
# LOAD GL (QB Transaction Detail by Account)
# =========================

def load_and_clean_gl(GL_PATH, coa):

    gl = pd.read_excel(GL_PATH)

    col_names1 = ["account_section","date","txn_type","num","name"]
    col_names2 = ["memo","split_account","amount","balance"]

    for col in gl.loc[3].to_list():
        if pd.isna(col) == False:
            if "Store" in col:
                col_names1 += ["store"]
            if "Class" in col:
                col_names1 += ["class"]
                
    col_names = col_names1 + col_names2

    gl = pd.read_excel(
        GL_PATH,
        skiprows=4,
        names=col_names,
    )

    gl["account_name"] = gl["account_section"].ffill()
    gl["split_account"] = gl["split_account"].fillna(gl["name"])
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