import pandas as pd
import numpy as np
from openai import OpenAI
from pydantic import BaseModel
import json
import statistics
from src.general_preprocessing import monday_week_start

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


def project_cadenced_events(dates, amounts, proj_start, proj_end, cadence_start, cadence_end):
    """
    Schedule future events based on cadence kind; return list of (date, amount_signed)
    Amount uses median of past event amounts (signed).
    """
    dates = pd.to_datetime(dates).dropna().sort_values()
    amounts = pd.Series(amounts).astype(float)

    if len(dates) == 0:
        return []

    kind = classify_cadence(dates, cadence_start, cadence_end)
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

def is_weekly_flow(series_hist, threshold=0.60):
    """
    Decide whether a line behaves like a weekly-flow series.
    If it has non-zero activity in >= threshold of weeks, treat as weekly-flow.
    """
    nz_rate = (series_hist != 0).mean() if len(series_hist) else 0.0
    return nz_rate >= threshold

def build_weekly_series(transactions_df, week_index):
    """
    transactions_df has columns: date, amount
    returns weekly sum series indexed by week_index (Mon starts)
    """
    wk = monday_week_start(transactions_df["date"])
    s = transactions_df.groupby(wk)["amount"].sum()
    return s.reindex(week_index, fill_value=0.0)

def classify_cadence(date_series: pd.Series, cadence_start, cadence_end) -> str:

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

def replicate_last_year_transactions(s_hist, proj_week_starts):
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

class PersistentTxn(BaseModel):
    persistent: bool
    proyected_value: float

# This function ended up being useless in the face of  the new adjust_for_truning_events function.
def detect_persitent_txn_changes(key, s_hist):
    client = OpenAI()

    prompt = f"""I'll give you a list of transactions through time (one per week). All of them correspond to a certain label, a certain account.
    You need to determine if there has been an abrupt change in the value of the transactions and if it is one that might be persistent in the long term.
    You need to use the label and the transactions to judge this. For example, if there is a sudden drop or increase in a list of transactions called
    'Payroll', then it makes sense to assume that there were some layoffs or hires. This would indicate that the change is going to persist through time.
    Same for sudden significant changes in transactions called 'Income', 'CC payments'. In general, whenever you see a really sharp change in transactions.
    But if you see some random fluctuations on an acount called 'Chase 974', then we can assume it's not something persistent. You need to return two values:
    a boolean called persistent, which determines if there was a persitent change or not. And a float called proyected_value, which, in case the boolean is True,
    will be the value that needs to be proyected for future weeks. If the boolean is False, then proyected _value should be 0.
    
    Here's the label for the account: {key}

    And here's the transaction history:
    {s_hist}"""

    response = client.responses.parse(
        model="gpt-5.4",
        input=prompt,
        text_format=PersistentTxn,
        #reasoning={"effort": "high"}
    )

    return json.loads(response.output_parsed.model_dump_json())

class PersistentTxn(BaseModel):
    account_type: str

def detect_type_of_account(key):
    client = OpenAI()

    prompt = f"""I'll give you an account name I got from the transaction detail document exported by Quickbooks from a certain company.
    You need to determine if the account represents an Inflow (Positive transactions, money coming in), Outflow (Negative transactions, money going out)
    or Mix (Money could be flowing in or out of the company). I'll provide some guiding examples. Anything called 'Income' should eb an Inflow and 
    anything called 'Payroll' should be an Outflow. You should only output Inflow or Outflow if you're over 90% certainty of this assesment. Everything else
    should called Mix. You need to out put this in JSON format with one field: account_type.
    The value of that field will be a string that can be either 'Inflow', 'Outflow' or 'Mix'.

    Remember, do not output anythin different than Mix if you're not over 90% certain.
    
    Here's the account name:
    {key}"""

    response = client.responses.parse(
        model="gpt-5.4",
        input=prompt,
        text_format=PersistentTxn,
        #reasoning={"effort": "high"}
    )

    json_object = json.loads(response.output_parsed.model_dump_json())
    print(key)
    print(json_object)

    return json_object['account_type']

def adjust_for_truning_events(sample):

    sensibility = 6
        
    m_list = []
    std_list = []
    # Window needs to be at least len 4 to catch monthly transactions (1 week per data point)
    window_len = 4
    turning_event_start = 0
    turning_event = []
    for i in range(len(sample)-window_len):
        window = sample[i:i+window_len]
        m = statistics.mean(window)
        std = statistics.stdev(window)
        m_list.append(m)
        std_list.append(std)
        if i> 0:
            prev_m = m_list[-2]
            prev_std = std_list[-2]
        else:
            last_stable_mean = m
            last_stable_std = std

        # Only record one turning event
        if abs(m) > abs(last_stable_mean) + sensibility*abs(last_stable_std):
            if turning_event_start==0:
                turning_event_start = i
                last_stable_mean = prev_m
                last_stable_std = prev_std
            turning_event.append(sample[i])
        else:
            last_stable_mean = m
            last_stable_std = std
    if len(turning_event) > 4:
        print("TURNING EVENT DETECTED")
        cropped_sample = sample[turning_event_start+2:]
        return cropped_sample
    else:
        return sample

def project_cash(bank_actual_pivot, bank_tx, cadence_start, cadence_end, cc_accounts, proj_week_starts, PROJ_WEEK1_START, proj_end_date, hist_week_starts, idx_names):

    # =========================
    # PROJECT BANK CASH LINES (non-CC-payment lines + CC payments separately)
    # =========================
    hist_bank_tx = bank_tx[(bank_tx["date"] >= cadence_start) & (bank_tx["date"] <= cadence_end)].copy()

    if cc_accounts is not None:
        # Separate CC payments (bank -> CC account)
        hist_ccpay_bank = hist_bank_tx[hist_bank_tx["split_account"].isin(cc_accounts)].copy()
        hist_noncc_bank = hist_bank_tx[~hist_bank_tx["split_account"].isin(cc_accounts)].copy()
    else:
        hist_noncc_bank = hist_bank_tx.copy()
        hist_ccpay_bank = []

    # Build projection matrix for all bank lines
    proj_bank = pd.DataFrame(0.0, index=bank_actual_pivot.index, columns=proj_week_starts)

    for key, df_line in hist_noncc_bank.groupby(idx_names):
        df_line = df_line.sort_values("date")
        s_hist = build_weekly_series(df_line[["date","amount"]], hist_week_starts)
        # TODO: Take this line out and have AI determine if this an account that should be inflow (all positive), outflow (All engative) or leave as is.
        if "Income:Membership Fee Income" in key:
            s_hist = abs(s_hist)
        account_type = detect_type_of_account(key)

        if account_type == 'Inflow':
            s_hist = abs(s_hist)
        elif account_type == 'Outflow':
            s_hist = -abs(s_hist)

        s_hist = adjust_for_truning_events(s_hist)

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

        # Detect abrupt persistent changes (e.g. Payroll increases or drops due to hires or layoffs)
        #persistency = detect_persitent_txn_changes(key, s_hist)
        persistency = {"persistent":[]}
        
        if persistency['persistent']:
 
            for idx in proj_series.index:
                if proj_series[idx] != 0.0:
                    proj_series[idx] = persistency['proyected_value']

        if key in proj_bank.index:
            proj_bank.loc[key, proj_week_starts] = proj_series.values
        else:
            proj_bank.loc[key] = 0.0
            proj_bank.loc[key, proj_week_starts] = proj_series.values

    return hist_ccpay_bank, proj_bank


def build_projections_table(all_week_starts, inflows_by_cat, outflows_by_cat, beg_bal_series, end_bal_series, total_inflows, total_outflows, inflows_present, outflows_present, week1_cash_balance=0.0):
    # Template-style table
    rows = []
    rows.append(("Week Number", "", ""))
    rows.append(("", "", ""))
    cash_balance_indexes = []
    rows.append(("Beginning Bank Balance", "", ""))
    cash_balance_indexes.append(len(rows)+1)
    rows.append(("Cash Inflows", "", ""))
    inflow_section_indexes = []
    for inflow_cat in inflows_by_cat:
        rows.append(("", inflow_cat, ""))
        inflow_section_indexes.append(len(rows)+1)
        for acct in sorted(inflows_by_cat[inflow_cat]):
            rows.append(("", "", acct))

        rows.append(("", "", ""))

    rows.append(("Total Cash Inflows", "", ""))
    inflow_section_indexes.append(len(rows)+1)
    rows.append(("Cash Outflows", "", ""))
    outflow_section_indexes = []
    for outflow_cat in outflows_by_cat:
        rows.append(("", outflow_cat, ""))
        outflow_section_indexes.append(len(rows)+1)
        for acct in sorted(outflows_by_cat[outflow_cat]):
            rows.append(("", "", acct))

        rows.append(("", "", ""))

    rows.append(("Total Cash Outflows", "", ""))
    outflow_section_indexes.append(len(rows)+1)
    rows.append(("", "", ""))
    rows.append(("Ending Bank Balance", "", ""))
    cash_balance_indexes.append(len(rows)+1)

    proj_sheet = pd.DataFrame(rows, columns=["Section","Notes","Line Item"])
    for w in all_week_starts:
        proj_sheet[w.strftime("%Y-%m-%d")] = np.nan

    def put_row_value(section, acct, values):
        mask = (proj_sheet["Section"].eq(section)) & (proj_sheet["Line Item"].eq(acct))
        idx = proj_sheet.index[mask]
        if len(idx):
            i = idx[0]
            for w in all_week_starts:
                if isinstance(values[w], int) or isinstance(values[w], float):
                    proj_sheet.loc[i, w.strftime("%Y-%m-%d")] = float(values[w])
                elif isinstance(values[w], str):
                    proj_sheet[w.strftime("%Y-%m-%d")] = proj_sheet[w.strftime("%Y-%m-%d")].astype("object")
                    proj_sheet.loc[i, w.strftime("%Y-%m-%d")] = values[w]

    week_numbers = [""]*4 + [f"Week {n+1}" for n in range(13)]
    week_number_series = pd.Series(week_numbers, index=all_week_starts)
    put_row_value("Week Number","", week_number_series)
    put_row_value("Beginning Bank Balance","", beg_bal_series)
    put_row_value("Ending Bank Balance","", end_bal_series)
    put_row_value("Total Cash Inflows","", total_inflows)
    put_row_value("Total Cash Outflows","", total_outflows)

    for index, row in inflows_present.iterrows():
        if len(index) == 3:
            acct = index[0]
        else:
            acct = index
        put_row_value("", acct, row)
    for index, row in outflows_present.iterrows():
        if len(index) == 3:
            acct = index[0]
        else:
            acct = index
        put_row_value("", acct, row)

    proj_sheet.iloc[2,7] = week1_cash_balance
    
    return proj_sheet, inflow_section_indexes, outflow_section_indexes, cash_balance_indexes