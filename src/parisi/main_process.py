from src.parisi.preprocessing import load_and_clean_gl, week_windows
from src.parisi.cash import create_cash_pivot, detect_bank_accounts, project_cash
from src.parisi.postprocessing import build_inflows_outflows, write_output_excel
from src.styling import style_projections
from src.classify_transactions import get_classifications
from src.general_postprocessing import calculate_category_totals
from src.retroactive_comparison import compare_reports
import streamlit as st

@st.cache_data
def get_parisi_cash_iq(COA_PATH="", GL_PATH="", date_strt="", OUTPUT_XLSX="parisi_output.xlsx", previous_cashiq_path="", initial_cash_balance=0.0):

    (anchor, actual_starts, proj_starts, all_starts, hist_starts, cadence_start, cadence_end, proj_end_date,
            TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS) = week_windows(date_strt)
    
    tx = load_and_clean_gl(GL_PATH)

    acct_info, bank_accounts, cash_tx = detect_bank_accounts(tx, BANK_CODE_MIN, BANK_CODE_MAX, BANK_NAME_KEYWORDS)

    beginning_cash_balance, pivot_actual = create_cash_pivot(anchor, cash_tx, all_starts, bank_accounts, tx)

    combined = project_cash(cash_tx, pivot_actual, anchor, all_starts, cadence_start, cadence_end, proj_end_date, hist_starts, actual_starts, proj_starts)

    inflows_present, outflows_present, total_inflows, total_outflows = build_inflows_outflows(combined, actual_starts, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES)

    inflows_by_cat, outflows_by_cat = get_classifications("parisi", inflows_present, outflows_present)

    write_output_excel(all_starts, inflows_by_cat, outflows_by_cat, beginning_cash_balance, 
                            total_inflows, total_outflows, inflows_present, outflows_present, 
                            acct_info, cash_tx, OUTPUT_XLSX, initial_cash_balance)

    TEMP_OUTPUT_XLSX = calculate_category_totals(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)

    if previous_cashiq_path is not None:
        compare_reports(previous_cashiq_path, TEMP_OUTPUT_XLSX, OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)

    style_projections(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)

    calculate_category_totals(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)

    with open(OUTPUT_XLSX, "rb") as f:
        return f.read()
