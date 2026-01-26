from src.trinity.styling import style_projections
from src.trinity.preprocessing import week_windows, load_and_clean_coa, load_and_clean_gl
from src.trinity.cash import begin_cash, buil_actual_weekly_cash, project_cash
from src.trinity.credit_card import begin_cc, get_cc_debt_history, project_cc_debt, project_cc_payments, allocate_payments
from src.trinity.postprocessing import get_combined_bank, build_inflows_outflows, get_cash_balance, get_cc_output_sheets, write_outout_excel
import streamlit as st

@st.cache_data
def get_trinity_cash_iq(COA_PATH, GL_PATH, date_strt, OUTPUT_XLSX):

    # First initialize the DFs and vars we need
    week_windows(date_strt)
    coa = load_and_clean_coa(COA_PATH)
    gl = load_and_clean_gl(GL_PATH, coa)

    # Start processing the cash data
    bank_tx, beginning_cash_balance, asof_date = begin_cash(gl, coa)
    cc_spend_txn = begin_cc(gl)
    bank_actual_pivot = buil_actual_weekly_cash(bank_tx)
    hist_ccpay_bank, proj_bank = project_cash(bank_actual_pivot)

    # Start processing the CC data
    cc_spend_cat_pivot, cc_spend_hist_start = get_cc_debt_history(cc_spend_txn, asof_date)
    cc_spend_proj_cat, cc_spend_cat_pivot_top = project_cc_debt(cc_spend_cat_pivot, cc_spend_hist_start)
    payment_event_dates = project_cc_payments(hist_ccpay_bank, asof_date)
    cc_payment_schedule, cc_payment_alloc = allocate_payments(cc_spend_proj_cat, cc_spend_cat_pivot_top, payment_event_dates)

    # Now combine the information to get the excel output
    combined_full = get_combined_bank(proj_bank)
    inflows_present, outflows_present, total_inflows, total_outflows = build_inflows_outflows(combined_full)
    beg_bal_series, end_bal_series = get_cash_balance(total_inflows, total_outflows, beginning_cash_balance)
    cc_spend_proj_display, cc_spend_actual_display, cc_payment_alloc_present = get_cc_output_sheets(inflows_present, outflows_present, total_inflows, total_outflows, 
                                                                                                    cc_spend_cat_pivot_top, cc_spend_proj_cat, cc_payment_alloc)
    write_outout_excel(inflows_present, outflows_present, total_inflows, total_outflows, cc_spend_proj_display, 
                       cc_spend_actual_display, cc_payment_alloc_present, cc_spend_txn, cc_payment_schedule, 
                       beg_bal_series, end_bal_series, OUTPUT_XLSX)
    style_projections(OUTPUT_XLSX)

    with open(OUTPUT_XLSX, "rb") as f:
        return f.read()