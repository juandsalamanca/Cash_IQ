import pandas as pd
import numpy as np
import re
from src.projections import project_weekly_pattern, project_cadenced_events, is_weekly_flow, week_of_month

# --------------------------------------------
# Auxiliary Cash functions
# --------------------------------------------

def looks_like_bank(name, code, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS):
    name_l = str(name).lower()
    if pd.notna(code) and BANK_CODE_MIN <= int(code) <= BANK_CODE_MAX:
        return True
    return any(k in name_l for k in BANK_NAME_KEYWORDS)

def related_is_bank(rel_name, rel_code, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS):
    if pd.notna(rel_code) and BANK_CODE_MIN <= int(rel_code) <= BANK_CODE_MAX:
        return True
    rel_l = str(rel_name).lower()
    return any(k in rel_l for k in BANK_NAME_KEYWORDS) and bool(rel_l.strip())

def strip_leading_code(name: str) -> str:
    # "61440 - Travel- Air" -> "Travel- Air"
    if name is None:
        return ""
    s = str(name).strip()
    s = re.sub(r"^\s*\d+\s*-\s*", "", s)
    return s.strip()

def normalize_line_item(s: str) -> str:
    s = strip_leading_code(s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*-\s*", " - ", s)
    return s.strip()


def week_bucket_start(dates: pd.Series, anchor: pd.Timestamp) -> pd.Series:
    dates = pd.to_datetime(dates)
    delta_days = (dates - anchor).dt.total_seconds() / (24*3600)
    k = np.floor(delta_days / 7.0).astype(int)
    return anchor + pd.to_timedelta(k * 7, unit="D")

def allocate_to_weeks(dates, amounts, week_starts, anchor):
    dates = pd.to_datetime(pd.Series(dates))
    wk = week_bucket_start(dates, anchor)
    s = pd.Series(amounts, index=wk)
    out = s.groupby(level=0).sum()
    return out.reindex(week_starts, fill_value=0.0)

def build_weekly_series(df_txn, week_index, anchor):
    wk = week_bucket_start(df_txn["date"], anchor)
    s = df_txn.groupby(wk)["amount"].sum()
    return s.reindex(week_index, fill_value=0.0)

def detect_bank_accounts(tx, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS):

    acct_info = tx[["account_name","account_code"]].drop_duplicates().copy()
    acct_info["is_bank"] = acct_info.apply(lambda r: looks_like_bank(r["account_name"], r["account_code"], BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS), axis=1)
    bank_accounts = set(acct_info.loc[acct_info["is_bank"], "account_name"])

    # Cash tx from bank accounts; exclude bank-to-bank transfers
    cash_tx = tx[tx["account_name"].isin(bank_accounts)].copy()
    cash_tx["related_is_bank"] = cash_tx.apply(lambda r: related_is_bank(r["related_account"], r["related_code"], BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS), axis=1)
    cash_tx = cash_tx[~cash_tx["related_is_bank"]].copy()

    # Choose line item source, then normalize (strip codes)
    cash_tx["line_item_raw"] = cash_tx["related_account"]
    cash_tx.loc[cash_tx["line_item_raw"].eq(""), "line_item_raw"] = cash_tx["contact"]
    cash_tx.loc[cash_tx["line_item_raw"].eq(""), "line_item_raw"] = cash_tx["description"]
    cash_tx.loc[cash_tx["line_item_raw"].eq(""), "line_item_raw"] = "Uncategorized"

    cash_tx["line_item"] = cash_tx["line_item_raw"].apply(normalize_line_item)

    # ------------------------------------------------------------
    # NEW: Split "Accounts Payable" into payees actually being paid
    # ------------------------------------------------------------
    # Normalize the AP label for comparison
    ap_label = "accounts payable"
    is_ap = cash_tx["line_item"].str.lower().eq(ap_label)

    # Build payee name: prefer Contact, fallback to Description
    payee = cash_tx["contact"].astype(str).str.strip()
    payee = payee.where(payee.ne(""), cash_tx["description"].astype(str).str.strip())
    payee = payee.where(payee.ne(""), "Unknown")

    # Clean payee text a bit (optional but helps)
    payee = payee.str.replace(r"\s+", " ", regex=True)

    # Set AP line item to "AP - <Payee>"
    cash_tx.loc[is_ap, "line_item"] = "AP - " + payee.loc[is_ap]

    return acct_info, bank_accounts, cash_tx

def create_cash_pivot(anchor, cash_tx, all_starts, bank_accounts, tx):
    # Beginning cash
    asof_date = anchor - pd.Timedelta(days=1)
    beg_bal_by_bank = {}
    for acct in bank_accounts:
        rows = tx[(tx["account_name"] == acct) & (tx["date"] <= asof_date)].sort_values("date")
        beg_bal_by_bank[acct] = float(rows.loc[rows["Running Balance"].notna(), "Running Balance"].iloc[-1]) if (len(rows) and rows["Running Balance"].notna().any()) else 0.0
    beginning_cash_balance = float(np.nansum(list(beg_bal_by_bank.values())))

    # Weekly actual by line_item ONLY (combines across accounts)
    cash_tx["week_start"] = week_bucket_start(cash_tx["date"], anchor)
    weekly = cash_tx.groupby(["line_item","week_start"])["amount"].sum().reset_index()

    pivot_actual = weekly.pivot_table(index=["line_item"], columns="week_start", values="amount", aggfunc="sum", fill_value=0.0)
    for w in all_starts:
        if w not in pivot_actual.columns:
            pivot_actual[w] = 0.0
    pivot_actual = pivot_actual[all_starts]

    return beginning_cash_balance, pivot_actual

def project_cash(cash_tx, pivot_actual, anchor, all_starts, cadence_start, cadence_end, proj_end_date, hist_starts, actual_starts, proj_starts):
    # Projections per line_item
    hist_cash = cash_tx[(cash_tx["date"] >= cadence_start) & (cash_tx["date"] <= cadence_end)].copy()
    proj_mat = pd.DataFrame(0.0, index=pivot_actual.index, columns=proj_starts)

    for line_item, df_line in hist_cash.groupby(["line_item"]):
        df_line = df_line.sort_values("date")
        s_hist = build_weekly_series(df_line[["date","amount"]], hist_starts, anchor)

        if is_weekly_flow(s_hist, threshold=0.60):
            proj_series = project_weekly_pattern(s_hist, proj_starts)
        else:
            future = project_cadenced_events(df_line["date"], df_line["amount"], anchor, proj_end_date, cadence_start, cadence_end)
            if future:
                dts, amts = zip(*future)
                proj_series = allocate_to_weeks(dts, amts, proj_starts, anchor)
            else:
                tail = s_hist.iloc[-26:] if len(s_hist) else s_hist
                wom = pd.Series([week_of_month(w) for w in tail.index], index=tail.index)
                wom_med = tail.groupby(wom).median()
                overall = float(tail.median()) if len(tail) else 0.0
                proj_series = pd.Series([float(wom_med.get(week_of_month(w), overall)) for w in proj_starts], index=proj_starts)

        proj_mat.loc[line_item, proj_starts] = proj_series.values

    combined = pd.concat([pivot_actual[actual_starts], proj_mat[proj_starts]], axis=1).fillna(0.0)
    combined = combined.reindex(columns=all_starts, fill_value=0.0)

    return combined