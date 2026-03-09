import pandas as pd
import numpy as np

# ============================================================
# BEGINNING CASH (bank balances as of day before proj start)
# ============================================================

def begin_cash(gl, coa, PROJ_WEEK1_START, bank_accounts):
    asof_date = PROJ_WEEK1_START - pd.Timedelta(days=1)

    # Pull balances from GL for bank accounts; fallback to COA total_balance
    beg_bal_by_bank = {}
    for acct_norm in bank_accounts:
        acct_rows = gl[gl["account_name"].eq(acct_norm)].copy()
        fallback = coa.loc[coa["full_name"].eq(acct_norm), "total_balance"]
        fallback = float(fallback.iloc[0]) if len(fallback) and pd.notna(fallback.iloc[0]) else 0.0

        acct_rows = acct_rows[acct_rows["date"] <= asof_date].sort_values("date")
        if acct_rows["balance"].notna().any():
            beg_bal_by_bank[acct_norm] = float(acct_rows.loc[acct_rows["balance"].notna(), "balance"].iloc[-1])
        else:
            beg_bal_by_bank[acct_norm] = fallback

    beginning_cash_balance = float(np.nansum(list(beg_bal_by_bank.values())))

    bank_tx = gl[gl["account_name"].isin(bank_accounts)].copy()
    bank_tx = bank_tx[~bank_tx["split_account"].isin(bank_accounts)].copy()

    # Vendor/Customer naming preference
    bank_tx["line_item"] = np.where(
        bank_tx["name"].str.strip().ne(""),
        bank_tx["name"],
        bank_tx["split_account"]
    )

    # Index includes account hint to keep specificity if same vendor used for multiple things
   

    return bank_tx, beginning_cash_balance


# ============================================================
# ACTUALS (weekly pivot)
# ============================================================

def buil_actual_weekly_cash(bank_tx, all_week_starts):

    idx_names = ["line_item", "split_account", "txn_type"]

    bank_weekly = (
        bank_tx.groupby(idx_names + ["week_start"], dropna=False)["amount"]
        .sum()
        .reset_index()
    )

    actual_pivot = bank_weekly.pivot_table(
        index=idx_names,
        columns="week_start",
        values="amount",
        aggfunc="sum",
        fill_value=0.0,
    )


    for w in all_week_starts:
        if w not in actual_pivot.columns:
            actual_pivot[w] = 0.0

    actual_pivot = actual_pivot[all_week_starts]

    return actual_pivot, idx_names