from src.trinity.preprocessing import monday_week_start
from src.trinity.projections import week_of_month, project_weekly_pattern
import pandas as pd


def begin_cc(gl):

    # =========================
    # CREDIT CARD SPEND (NOT CASH): CC ACCOUNT TRANSACTIONS
    # =========================

    cc_tx = gl[gl["account_name"].isin(cc_accounts)].copy()

    # Transaction-level CC spend sheet wants "all CC spend"
    # Define CC spend as CC account rows where split_account is NOT a bank and NOT a credit card.
    cc_spend_txn = cc_tx[~cc_tx["split_account"].isin(bank_accounts)].copy()
    cc_spend_txn = cc_spend_txn[~cc_spend_txn["split_account"].isin(cc_accounts)].copy()

    return cc_spend_txn


def get_cc_debt_history(cc_spend_txn, asof_date):
    # =========================
    # PROJECT CREDIT CARD SPEND (LIABILITY) + PROJECT CC PAYMENTS (CASH) + ALLOCATE CC PAYMENTS
    # =========================

    # 1) Weekly CC spend by category (historical) for projecting CC spend pattern (NOT cash)
    cc_spend_hist_start = PROJ_WEEK1_START - pd.Timedelta(weeks=CC_SPEND_TS_WEEKS)
    cc_spend_hist = cc_spend_txn[(cc_spend_txn["date"] >= cc_spend_hist_start) & (cc_spend_txn["date"] <= asof_date)].copy()

    # category = split_account (expense accounts etc.)
    cc_spend_hist["cat"] = cc_spend_hist["split_account"].fillna("Uncategorized")

    # weekly totals per category across ALL CC accounts
    cc_spend_hist["week_start"] = monday_week_start(cc_spend_hist["date"])
    cc_spend_week_cat = cc_spend_hist.groupby(["cat","week_start"])["amount"].sum().reset_index()

    cc_spend_cat_pivot = cc_spend_week_cat.pivot_table(
        index=["cat"],
        columns="week_start",
        values="amount",
        aggfunc="sum",
        fill_value=0.0
    )

    return cc_spend_cat_pivot, cc_spend_hist_start

def project_cc_debt(cc_spend_cat_pivot, cc_spend_hist_start):

    # ensure week columns for CC spend history
    cc_hist_weeks = pd.date_range(start=cc_spend_hist_start, end=actual_week_starts[-1], freq="W-MON")
    for w in cc_hist_weeks:
        if w not in cc_spend_cat_pivot.columns:
            cc_spend_cat_pivot[w] = 0.0
    cc_spend_cat_pivot = cc_spend_cat_pivot[cc_hist_weeks]

    # Keep top CC cats, collapse rest
    cc_abs_totals = cc_spend_cat_pivot.abs().sum(axis=1).sort_values(ascending=False)
    top_cc_cats = cc_abs_totals.head(TOP_N_CC_CATS).index.tolist()
    cc_spend_cat_pivot_top = cc_spend_cat_pivot.loc[top_cc_cats].copy()
    if len(cc_spend_cat_pivot) > len(top_cc_cats):
        other = cc_spend_cat_pivot.drop(index=top_cc_cats).sum(axis=0)
        cc_spend_cat_pivot_top.loc["Other CC Categories"] = other

    # Project CC spend weekly by category using weekly-flow projection (NOT flat)
    cc_spend_proj_cat = pd.DataFrame(0.0, index=cc_spend_cat_pivot_top.index, columns=proj_week_starts)
    for cat in cc_spend_cat_pivot_top.index:
        s_hist = cc_spend_cat_pivot_top.loc[cat]
        # treat CC spend categories as weekly-flow by default; if mostly zeros, use wom median fallback
        nz_rate = (s_hist != 0).mean() if len(s_hist) else 0.0
        if nz_rate >= 0.40:
            proj = project_weekly_pattern(s_hist, proj_week_starts)
        else:
            tail = s_hist.iloc[-26:] if len(s_hist) else s_hist
            wom = pd.Series([week_of_month(pd.Timestamp(w)) for w in tail.index], index=tail.index)
            wom_median = tail.groupby(wom).median()
            overall = float(tail.median()) if len(tail) else 0.0
            proj_vals = []
            for w in proj_week_starts:
                proj_vals.append(float(wom_median.get(week_of_month(pd.Timestamp(w)), overall)))
            proj = pd.Series(proj_vals, index=proj_week_starts, dtype=float)
        cc_spend_proj_cat.loc[cat] = proj.values

    return cc_spend_proj_cat, cc_spend_cat_pivot_top


def project_cc_payments(hist_ccpay_bank, asof_date):

        # 2) Project CC payments (cash) on realistic cadence inferred from historical bank->CC payments
    #    - Determine typical payment cadence & timing from bank-side payments
    #    - Determine payment amount as "statement-like": pay prior month's projected CC spend (absolute), on typical payment day-of-month
    #    - Allocate the payment into categories based on rolling spend mix (last CC_MIX_ROLLING_WEEKS before the payment)
    #
    # Payment timing inference:
    ccpay_dates = hist_ccpay_bank["date"].sort_values()
    ccpay_kind = classify_cadence(ccpay_dates) if len(ccpay_dates) else "monthly"

    # Typical day-of-month for payment (use mode)
    if len(ccpay_dates):
        dti = pd.DatetimeIndex(pd.to_datetime(ccpay_dates))
        dom_mode = int(pd.Series(dti.day).value_counts().idxmax())
    else:
        dom_mode = 15

    # We will schedule ONE payment per month (or per cadence) in projection.
    # If cadence is weekly/biweekly (rare), we schedule by that cadence, but amount still follows recent spend.
    payment_event_dates = []
    horizon_end = proj_end_date

    if ccpay_kind in {"weekly","biweekly"}:
        step = pd.Timedelta(days=7 if ccpay_kind == "weekly" else 14)
        d = asof_date
        while d < horizon_end:
            d = d + step
            if d >= PROJ_WEEK1_START:
                payment_event_dates.append(d)
    else:
        # monthly/semimonthly/irregular -> treat as monthly statement payment
        months = pd.date_range(start=PROJ_WEEK1_START.normalize(), end=horizon_end.normalize(), freq="MS")
        for m in months:
            d = m + pd.Timedelta(days=dom_mode - 1)
            if d.month != m.month:
                d = m + pd.offsets.MonthEnd(0)
            if PROJ_WEEK1_START <= d < horizon_end:
                payment_event_dates.append(pd.Timestamp(d))

    payment_event_dates = sorted(payment_event_dates)

    return payment_event_dates

def spend_mix_for_window(window_week_starts, cc_spend_cat_pivot_top):
    """
    rolling mix over a set of week_starts: use actual if available, else projected.
    returns shares by category over that window
    """
    mix_vals = []
    for cat in cc_spend_cat_pivot_top.index:
        # actual + projected per category weekly
        s_cat = pd.concat([cc_spend_cat_pivot_top.loc[cat], cc_spend_proj_cat.loc[cat]])
        mix_vals.append(s_cat.reindex(window_week_starts, fill_value=0.0).abs().sum())
    mix = pd.Series(mix_vals, index=cc_spend_cat_pivot_top.index)
    if mix.sum() == 0:
        return pd.Series({"Uncategorized": 1.0})
    return mix / mix.sum()

def allocate_payments(cc_spend_proj_cat, cc_spend_cat_pivot_top, payment_event_dates):
    # Compute monthly "statement" amount from projected CC spend:
    # For each payment date, pay the prior month's total projected CC spend magnitude.
    # We compute CC spend totals from cc_spend_proj_cat (weekly) and/or last actual weeks for the first payment.
    cc_spend_week_all = cc_spend_cat_pivot_top.sum(axis=0)  # historical weekly total across cats
    cc_spend_proj_week_all = cc_spend_proj_cat.sum(axis=0)  # projected weekly total across cats

    # Helper: get CC spend by week (actual+proj)
    cc_spend_total_week = pd.concat([cc_spend_week_all, cc_spend_proj_week_all]).groupby(level=0).sum()

    # Build a projected cash-outflow table for CC payments by category (signed negative)
    cc_payment_alloc = pd.DataFrame(0.0,
                                index=pd.MultiIndex.from_tuples([], names=idx_names),
                                columns=proj_week_starts)

    # Prebuild combined per-category weekly spend (actual+proj) for window lookups
    cc_cat_week_combined = {}
    for cat in cc_spend_cat_pivot_top.index:
        cc_cat_week_combined[cat] = pd.concat([cc_spend_cat_pivot_top.loc[cat], cc_spend_proj_cat.loc[cat]]).groupby(level=0).sum()

    # Also store CC payment schedule
    cc_payment_schedule_rows = []

    for pay_date in payment_event_dates:
        # Determine "prior month" window relative to payment date
        pay_date = pd.Timestamp(pay_date)
        prior_month_end = (pay_date.replace(day=1) - pd.Timedelta(days=1)).normalize()
        prior_month_start = prior_month_end.replace(day=1)

        # weeks in prior month (Mon week starts that intersect that month)
        weeks_in_prior_month = pd.date_range(
            start=monday_week_start(pd.Series([prior_month_start]))[0],
            end=monday_week_start(pd.Series([prior_month_end]))[0],
            freq="W-MON"
        )

        # Statement amount = sum of weekly CC spend totals in prior month (absolute)
        stmt_amt = cc_spend_total_week.reindex(weeks_in_prior_month, fill_value=0.0).abs().sum()

        # If statement amount is tiny/zero, skip
        if float(stmt_amt) == 0.0:
            continue

        # Allocate across categories based on rolling mix window ending before payment date
        mix_end_week = monday_week_start(pd.Series([pay_date - pd.Timedelta(days=1)]))[0]
        mix_window_weeks = pd.date_range(end=mix_end_week, periods=CC_MIX_ROLLING_WEEKS, freq="W-MON")
        shares = spend_mix_for_window(mix_window_weeks, cc_spend_cat_pivot_top)

        # allocate into the payment's week bucket
        pay_week = monday_week_start(pd.Series([pay_date]))[0]
        if pay_week not in proj_week_starts:
            continue

        # Add rows for each category
        for cat, share in shares.items():
            line = (f"CC Payment - {cat}", "Credit Card Payment", "")
            if line not in cc_payment_alloc.index:
                cc_payment_alloc.loc[line, :] = 0.0
            cc_payment_alloc.loc[line, pay_week] += -float(stmt_amt * share)  # signed cash outflow

        cc_payment_schedule_rows.append({
            "payment_date": pay_date,
            "payment_week_start": pay_week,
            "estimated_payment_total": float(stmt_amt),
            "allocation_window_weeks": f"{mix_window_weeks.min().date()} to {mix_window_weeks.max().date()}",
            "prior_month": f"{prior_month_start.date()} to {prior_month_end.date()}",
            "cadence_inferred": ccpay_kind,
            "typical_dom": dom_mode,
        })

    cc_payment_schedule = pd.DataFrame(cc_payment_schedule_rows).sort_values("payment_date") if len(cc_payment_schedule_rows) else pd.DataFrame(
        columns=["payment_date","payment_week_start","estimated_payment_total","allocation_window_weeks","prior_month","cadence_inferred","typical_dom"]
    )

    return cc_payment_schedule, cc_payment_alloc