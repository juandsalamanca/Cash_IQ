import pandas as pd
import numpy as np
from src.trinity.preprocessing import monday_week_start

def week_of_month(dt: pd.Timestamp) -> int:
    return ((dt.day - 1) // 7) + 1

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def allocate_to_weeks(dates, amounts, week_starts):
    s = pd.Series(amounts, index=pd.to_datetime(dates))
    wk = monday_week_start(s.index.to_series())
    out = s.groupby(wk).sum()
    return out.reindex(week_starts, fill_value=0.0)

def project_weekly_pattern(series_hist, proj_weeks):
    """
    Project a weekly-flow line:
    - Use week-of-month seasonality (avg by week_in_month: 1..5)
    - Add mild trend based on last 12 weeks slope, clamped
    """
    hist_weeks = series_hist.index
    hist_vals = series_hist.values.astype(float)

    # Seasonality by week-of-month
    wom = np.array([week_of_month(pd.Timestamp(w)) for w in hist_weeks])
    df = pd.DataFrame({"wom": wom, "y": hist_vals})
    wom_means = df.groupby("wom")["y"].mean()

    # Baseline from seasonality (fallback to overall mean)
    overall_mean = float(df["y"].mean())

    # Trend from last 12 weeks (simple linear regression)
    tail_n = min(12, len(hist_vals))
    if tail_n >= 6:
        y = hist_vals[-tail_n:]
        x = np.arange(tail_n)
        # slope via least squares
        slope = float(np.polyfit(x, y, 1)[0])
    else:
        slope = 0.0

    # Clamp trend so we don't explode
    # Convert weekly slope into a per-week multiplier relative to mean magnitude
    denom = max(1.0, np.nanmean(np.abs(hist_vals[-tail_n:])) if tail_n else 1.0)
    slope_ratio = slope / denom
    slope_ratio = clamp(slope_ratio, -0.15, 0.15)  # cap to +/-15% per week equivalent
    # Apply cumulative trend
    proj = []
    for i, w in enumerate(proj_weeks, start=1):
        wom_i = week_of_month(pd.Timestamp(w))
        base = float(wom_means.get(wom_i, overall_mean))
        proj_val = base * (1.0 + slope_ratio * i)
        proj.append(proj_val)

    return pd.Series(proj, index=proj_weeks, dtype=float)


def project_cadenced_events(dates, amounts, proj_start, proj_end):
    """
    Schedule future events based on cadence kind; return list of (date, amount_signed)
    Amount uses median of past event amounts (signed).
    """
    dates = pd.to_datetime(dates).dropna().sort_values()
    amounts = pd.Series(amounts).astype(float)

    if len(dates) == 0:
        return []

    kind = classify_cadence(dates)
    amt_med = float(pd.Series(amounts).replace(0, np.nan).dropna().median()) if (pd.Series(amounts) != 0).any() else 0.0
    last_date = pd.Timestamp(dates.max())

    future = []

    if kind == "weekly":
        step = pd.Timedelta(days=7)
        d = last_date
        while d < proj_end:
            d = d + step
            if d >= proj_start:
                future.append((d, amt_med))

    elif kind == "biweekly":
        step = pd.Timedelta(days=14)
        d = last_date
        while d < proj_end:
            d = d + step
            if d >= proj_start:
                future.append((d, amt_med))

    elif kind == "monthly":
        d = last_date
        while d < proj_end:
            d = d + pd.DateOffset(months=1)
            if d >= proj_start:
                future.append((d, amt_med))

    elif kind == "quarterly":
        d = last_date
        while d < proj_end:
            d = d + pd.DateOffset(months=3)
            if d >= proj_start:
                future.append((d, amt_med))

    elif kind == "annual":
        d = last_date
        while d < proj_end:
            d = d + pd.DateOffset(years=1)
            if d >= proj_start:
                future.append((d, amt_med))

    elif kind == "semimonthly":
        # Choose two most common days of month from history
        dti = pd.DatetimeIndex(pd.to_datetime(dates))
        dom = dti.day
        top_days = pd.Series(dom).value_counts().head(2).index.tolist()
        if len(top_days) == 1:
            top_days = [top_days[0], min(28, top_days[0] + 14)]
        top_days = sorted(top_days)

        months = pd.date_range(start=proj_start.normalize(), end=proj_end.normalize(), freq="MS")
        for m in months:
            for day in top_days:
                d = m + pd.Timedelta(days=day - 1)
                # clamp to month end
                if d.month != m.month:
                    d = m + pd.offsets.MonthEnd(0)
                if proj_start <= d < proj_end:
                    future.append((d, amt_med / 2.0))

    else:
        # irregular: no scheduled events; return empty (will be handled by weekly TS method if needed)
        return []

    return future

def is_weekly_flow(series_hist):
    """
    Decide whether a line behaves like a weekly-flow series.
    If it has non-zero activity in >= 60% of weeks, treat as weekly-flow.
    """
    nz_rate = (series_hist != 0).mean() if len(series_hist) else 0.0
    return nz_rate >= 0.60

def build_weekly_series(transactions_df, week_index):
    """
    transactions_df has columns: date, amount
    returns weekly sum series indexed by week_index (Mon starts)
    """
    wk = monday_week_start(transactions_df["date"])
    s = transactions_df.groupby(wk)["amount"].sum()
    return s.reindex(week_index, fill_value=0.0)

def classify_cadence(date_series: pd.Series):

    # We need to add the cadence end and start dates to the ds variable
    # This way the take into account the whole year and not get isolated events passed as weekly, monthy, etc
    date_series = pd.concat([pd.Series(cadence_start), date_series, pd.Series(cadence_end)])
    ds = pd.to_datetime(date_series).dropna().sort_values().unique()
    if len(ds) < 3: return "irregular"
    diffs = np.diff(ds).astype("timedelta64[D]").astype(int)
    diffs = diffs[diffs > 0]
    std = np.std(diffs)
    if len(diffs) < 2: return "irregular"
    med = float(np.median(diffs))
    if 12 <= med <= 17 and std/14 <0.2:
        months = pd.to_datetime(ds).to_period("M")
        counts = pd.Series(months).value_counts()
        if (counts >= 2).mean() >= 0.55: return "semimonthly"
        return "biweekly"
    if 24 <= med <= 37 and std/30 <0.2: return "monthly"
    if 70 <= med <= 110 and std/90 <0.2: return "quarterly"
    if 320 <= med <= 420 and std/365 <0.2: return "annual"
    return "irregular"

def week_of_year(ts):
    ts = pd.Timestamp(ts)
    return int(((ts.month-1) * 30.5 + float(ts.day))/7)

def replicate_last_year_transactions(s_hist):
    week_of_year_transaction_map = {}
    for w in s_hist.index:
        week_of_year_transaction_map[week_of_year(w)] = s_hist[w]
    projection_list = []
    for w in proj_week_starts:
        woy = week_of_year(w)
        corresponding_last_year_transaction = week_of_year_transaction_map.get(woy)
        projection_list.append(corresponding_last_year_transaction)
    proj_series = pd.Series(projection_list, index=proj_week_starts)
    return proj_series

