import pandas as pd

def week_windows(date_start):

    # If you want to force a start date, set it here; otherwise we'll try to infer from prior forecast.
    PROJ_WEEK1_START = pd.to_datetime(date_start)

    N_ACTUAL_WEEKS = 4
    N_PROJ_WEEKS   = 13

    LOOKBACK_WEEKS_LINE_TS   = 52
    LOOKBACK_MONTHS_CADENCE  = 12

    TOP_N_INFLOW_LINES  = 40
    TOP_N_OUTFLOW_LINES = 80

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

    hist_week_starts = pd.date_range(
        start=PROJ_WEEK1_START - pd.Timedelta(weeks=LOOKBACK_WEEKS_LINE_TS),
        end=actual_week_starts[-1],
        freq="W-MON",
    )

    cadence_start = (PROJ_WEEK1_START - pd.DateOffset(months=LOOKBACK_MONTHS_CADENCE)).normalize()
    cadence_end   = (PROJ_WEEK1_START - pd.Timedelta(days=1)).normalize()
    proj_end_date = proj_week_starts[-1] + pd.Timedelta(days=7)

    return (PROJ_WEEK1_START, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, actual_week_starts, proj_week_starts, 
            all_week_starts, hist_week_starts, cadence_start, cadence_end, proj_end_date)