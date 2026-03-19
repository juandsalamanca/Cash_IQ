import pandas as pd

# =========================
# COMBINE ACTUALS + PROJECTIONS FOR BANK CASH
# =========================

def get_combined_bank(proj_bank, bank_actual_pivot, actual_week_starts, proj_week_starts, all_week_starts):
    combined_bank = pd.concat(
        [
            bank_actual_pivot[actual_week_starts],
            proj_bank[proj_week_starts]
        ],
        axis=1
    ).fillna(0.0)

    combined_full = combined_bank.reindex(columns=all_week_starts, fill_value=0.0)
    return combined_full



# ============================================================
# WRITE OUTPUT
# ============================================================

def write_output_excel(VENDOR_SUMMARY_PATH, OUTPUT_XLSX, summary, proj_table, inflows_present, outflows_present, gl, ar, ar_assumptions_df):
    
    vendor_summary = pd.read_excel(VENDOR_SUMMARY_PATH)
    vendor_summary.columns = [str(c).strip() for c in vendor_summary.columns]

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        proj_table.to_excel(writer, sheet_name="Projections (Table)", index=False)

        inflows_present.reset_index().to_excel(writer, sheet_name="Cash Inflows (Detail)", index=False)
        outflows_present.reset_index().to_excel(writer, sheet_name="Cash Outflows (Detail)", index=False)

        gl.sort_values(["account_name_raw","date"]).to_excel(writer, sheet_name="GL Cleaned", index=False)

        if ar is not None and ar_assumptions_df is not None:
            ar.to_excel(writer, sheet_name="AR Aging (Raw)", index=False)
            ar_assumptions_df.to_excel(writer, sheet_name="AR Collections (Assumptions)", index=False)
        vendor_summary.to_excel(writer, sheet_name="Expenses by Vendor (Raw)", index=False)
