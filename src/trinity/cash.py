import numpy as np
import pandas as pd
from src.trinity.projections import (build_weekly_series, project_weekly_pattern, project_cadenced_events, 
                                     allocate_to_weeks, replicate_last_year_transactions, week_of_month, 
                                     is_weekly_flow)


# =========================
# BEGINNING CASH (bank balances as of day before projection start)
# =========================

def last_nonnull_balance(df_acct, asof_date, fallback=0.0):
    df_acct = df_acct[df_acct["date"] <= asof_date].sort_values("date")
    if "balance" in df_acct.columns and df_acct["balance"].notna().any():
        return float(df_acct.loc[df_acct["balance"].notna(), "balance"].iloc[-1])
    return float(fallback)

def begin_cash(gl, coa, PROJ_WEEK1_START, bank_accounts, cc_accounts):
    asof_date = PROJ_WEEK1_START - pd.Timedelta(days=1)

    beg_bal_by_bank = {}
    for acct in bank_accounts:
        acct_rows = gl[gl["account_name"] == acct].copy()
        fallback = coa.loc[coa["full_name"].eq(acct), "total_balance"]
        fallback = float(fallback.iloc[0]) if len(fallback) and pd.notna(fallback.iloc[0]) else 0.0
        beg_bal_by_bank[acct] = last_nonnull_balance(acct_rows, asof_date, fallback=fallback)

    beginning_cash_balance = float(np.nansum(list(beg_bal_by_bank.values())))

    # =========================
    # CASHFLOW BASE: BANK TRANSACTIONS ONLY
    #   - remove bank->bank transfers (no net cash)
    #   - keep bank->CC payments (cash outflow)
    # =========================
    bank_tx = gl[gl["account_name"].isin(bank_accounts)].copy()
    bank_tx = bank_tx[~bank_tx["split_account"].isin(bank_accounts)].copy()

    # explicitly label bank->CC as Credit Card for split_type (if not already)
    bank_tx.loc[bank_tx["split_account"].isin(cc_accounts), "split_type"] = "Credit Card"

    return bank_tx, beginning_cash_balance, asof_date


def buil_actual_weekly_cash(bank_tx, all_week_starts):

    idx_names = ["split_account","split_type","split_detail_type"]

    bank_weekly = (
        bank_tx.groupby(idx_names + ["week_start"], dropna=False)["amount"]
        .sum()
        .reset_index()
    )

    bank_actual_pivot = bank_weekly.pivot_table(
        index=idx_names,
        columns="week_start",
        values="amount",
        aggfunc="sum",
        fill_value=0.0,
    )

    for w in all_week_starts:
        if w not in bank_actual_pivot.columns:
            bank_actual_pivot[w] = 0.0
    bank_actual_pivot = bank_actual_pivot[all_week_starts]

    return bank_actual_pivot, idx_names

def project_cash(bank_actual_pivot, bank_tx, cadence_start, cadence_end, cc_accounts, proj_week_starts, PROJ_WEEK1_START, proj_end_date, hist_week_starts, idx_names):

    # =========================
    # PROJECT BANK CASH LINES (non-CC-payment lines + CC payments separately)
    # =========================
    hist_bank_tx = bank_tx[(bank_tx["date"] >= cadence_start) & (bank_tx["date"] <= cadence_end)].copy()

    # Separate CC payments (bank -> CC account)
    hist_ccpay_bank = hist_bank_tx[hist_bank_tx["split_account"].isin(cc_accounts)].copy()
    hist_noncc_bank = hist_bank_tx[~hist_bank_tx["split_account"].isin(cc_accounts)].copy()

    # Build projection matrix for all bank lines
    proj_bank = pd.DataFrame(0.0, index=bank_actual_pivot.index, columns=proj_week_starts)

    for key, df_line in hist_noncc_bank.groupby(idx_names):
        df_line = df_line.sort_values("date")
        s_hist = build_weekly_series(df_line[["date","amount"]], hist_week_starts)

        # If series exists and more than half values are non zero, return true, else return false
        if is_weekly_flow(s_hist):
            # Get projections based on linear slopes for eahc week of the month
            # These projections therefore get weekly cyclical trends and linear long term trends
            proj_series = project_weekly_pattern(s_hist, proj_week_starts)
        else:

            future_events = project_cadenced_events(df_line["date"], df_line["amount"], PROJ_WEEK1_START, proj_end_date, cadence_start, cadence_end)
            if future_events:
                dts, amts = zip(*future_events)
                proj_series = allocate_to_weeks(dts, amts, proj_week_starts)
            else:
                tail = s_hist.iloc[-26:] if len(s_hist) else s_hist
                wom = pd.Series([week_of_month(w) for w in tail.index], index=tail.index)
                wom_median = tail.groupby(wom).median()
                found = False
                for amnt in wom_median:
                    if amnt != 0.0:
                        found = True
                        break

                if found:
                    overall = float(tail.median()) if len(tail) else 0.0
                    proj_series = pd.Series(
                        [float(wom_median.get(week_of_month(w), overall)) for w in proj_week_starts],
                        index=proj_week_starts
                    )
                # If all medians are zero we replicate last year tendencies
                else:
                    proj_series = replicate_last_year_transactions(s_hist, proj_week_starts)

        if key in proj_bank.index:
            proj_bank.loc[key, proj_week_starts] = proj_series.values
        else:
            proj_bank.loc[key] = 0.0
            proj_bank.loc[key, proj_week_starts] = proj_series.values

    return hist_ccpay_bank, proj_bank