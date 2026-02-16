import pandas as pd
import numpy as np
import re

def collapse_other(df, keep_index, other_name):
    idx_names = list(df.index.names)
    keep = df.loc[keep_index].copy() if len(keep_index) else df.iloc[0:0].copy()
    other = df.drop(index=keep_index, errors="ignore").copy()
    if len(other):
        other_row = other.sum(axis=0)
        other_idx = pd.MultiIndex.from_tuples([(other_name,)], names=idx_names)
        other_df = pd.DataFrame([other_row.values], index=other_idx, columns=df.columns)
        keep = pd.concat([keep, other_df], axis=0)
    return keep

def build_inflows_outflows(combined, actual_starts, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES):
    # Inflows/Outflows
    horizon_sum = combined.sum(axis=1)
    inflows = combined.loc[horizon_sum > 0].copy()
    outflows = combined.loc[horizon_sum < 0].copy()

    trailing = combined[actual_starts]
    top_inflows = (inflows.assign(m=lambda d: trailing.loc[d.index].clip(lower=0).sum(axis=1))
                .sort_values("m", ascending=False).head(TOP_N_INFLOW_LINES).index)
    top_outflows = (outflows.assign(m=lambda d: (-trailing.loc[d.index].clip(upper=0)).sum(axis=1))
                    .sort_values("m", ascending=False).head(TOP_N_OUTFLOW_LINES).index)

    inflows = collapse_other(inflows, top_inflows, "Other Inflows")
    outflows = collapse_other(outflows, top_outflows, "Other Outflows")

    inflows_present = inflows.round(2)
    outflows_present = outflows.abs().round(2)

    total_inflows = inflows_present.sum(axis=0).round(2)
    total_outflows = outflows_present.sum(axis=0).round(2)

    return inflows_present, outflows_present, total_inflows, total_outflows


def write_output_excel(all_starts, beginning_cash_balance, total_inflows, total_outflows, inflows_present, outflows_present, acct_info, cash_tx, OUTPUT_XLSX):
    
    # Balances
    beg_bal = pd.Series(index=all_starts, dtype=float)
    end_bal = pd.Series(index=all_starts, dtype=float)
    running = float(round(beginning_cash_balance, 2))
    for w in all_starts:
        beg_bal[w] = running
        running = round(running + float(total_inflows[w]) - float(total_outflows[w]), 2)
        end_bal[w] = running

    summary = pd.DataFrame({
        "Period Start": all_starts,
        "Period End": [w + pd.Timedelta(days=6) for w in all_starts],
        "Beginning Bank Balance": [beg_bal[w] for w in all_starts],
        "Total Cash Inflows": [float(total_inflows[w]) for w in all_starts],
        "Total Cash Outflows": [float(total_outflows[w]) for w in all_starts],
        "Ending Bank Balance": [end_bal[w] for w in all_starts],
    })

    # Projections table: NO Account col, NO duplicates (line_item index is unique now)
    rows = []
    rows.append(("Beginning Bank Balance", ""))
    rows.append(("Cash Inflows", ""))
    for line in inflows_present.index:
        rows.append(("", line))
    rows.append(("Total Cash Inflows", ""))
    rows.append(("Cash Outflows", ""))
    for line in outflows_present.index:
        rows.append(("", line))
    rows.append(("Total Cash Outflows", ""))
    rows.append(("Ending Bank Balance", ""))

    proj_table = pd.DataFrame(rows, columns=["Section","Line Item"])
    for w in all_starts:
        proj_table[w.strftime("%Y-%m-%d")] = np.nan

    def put(section, series):
        m = (proj_table["Section"] == section) & (proj_table["Line Item"] == "")
        if not m.any(): return
        i = proj_table.index[m][0]
        for w in all_starts:
            proj_table.loc[i, w.strftime("%Y-%m-%d")] = float(series[w])

    put("Beginning Bank Balance", beg_bal)
    put("Total Cash Inflows", total_inflows)
    put("Total Cash Outflows", total_outflows)
    put("Ending Bank Balance", end_bal)

    for line, row in inflows_present.iterrows():
        m = (proj_table["Section"] == "") & (proj_table["Line Item"] == line)
        if not m.any(): continue
        i = proj_table.index[m][0]
        for w in all_starts:
            proj_table.loc[i, w.strftime("%Y-%m-%d")] = float(row[w])

    for line, row in outflows_present.iterrows():
        m = (proj_table["Section"] == "") & (proj_table["Line Item"] == line)
        if not m.any(): continue
        i = proj_table.index[m][0]
        for w in all_starts:
            proj_table.loc[i, w.strftime("%Y-%m-%d")] = float(row[w])

    date_cols = [c for c in proj_table.columns if re.match(r"^\d{4}-\d{2}-\d{2}$", str(c))]
    proj_table[date_cols] = proj_table[date_cols].apply(pd.to_numeric, errors="coerce").round(2)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        proj_table.to_excel(writer, sheet_name="Projections (Table)", index=False)
        inflows_present.reset_index().to_excel(writer, sheet_name="Cash Inflows (Detail)", index=False)
        outflows_present.reset_index().to_excel(writer, sheet_name="Cash Outflows (Detail)", index=False)
        acct_info.to_excel(writer, sheet_name="Detected Bank Accounts", index=False)
        cash_tx.sort_values(["account_name","date"]).to_excel(writer, sheet_name="Cash Tx (Bank Only)", index=False)
