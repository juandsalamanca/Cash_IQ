from src.trinity.styling import style_projections
from src.trinity.preprocessing import week_windows, load_and_clean_coa, load_and_clean_gl
from src.trinity.cash import begin_cash, buil_actual_weekly_cash, project_cash
from src.trinity.credit_card import begin_cc, get_cc_debt_history, project_cc_debt, project_cc_payments, allocate_payments
from src.trinity.postprocessing import get_combined_bank, build_inflows_outflows, get_cash_balance, get_cc_output_sheets, write_output_excel, calculate_category_totals
from src.trinity.classify_transactions import get_calssifications
import streamlit as st



@st.cache_data
def get_trinity_cash_iq(COA_PATH, GL_PATH, date_strt, OUTPUT_XLSX):

    # TODO: Need to pass all the global vars properly as params through the functions
    # First initialize the DFs and vars we need
    (PROJ_WEEK1_START, CC_MIX_ROLLING_WEEKS, CC_SPEND_TS_WEEKS, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, 
     TOP_N_CC_CATS, actual_week_starts, proj_week_starts, all_week_starts, hist_week_starts, cadence_start,
       cadence_end, proj_end_date) = week_windows(date_strt)
    coa, bank_accounts, cc_accounts = load_and_clean_coa(COA_PATH)
    gl = load_and_clean_gl(GL_PATH, coa)

    # Start processing the cash data
    bank_tx, beginning_cash_balance, asof_date = begin_cash(gl, coa, PROJ_WEEK1_START, bank_accounts, cc_accounts)
    cc_spend_txn = begin_cc(gl, bank_accounts, cc_accounts)
    bank_actual_pivot, idx_names = buil_actual_weekly_cash(bank_tx, all_week_starts)
    hist_ccpay_bank, proj_bank = project_cash(bank_actual_pivot, bank_tx, cadence_start, cadence_end, cc_accounts, proj_week_starts, 
                                              PROJ_WEEK1_START, proj_end_date, hist_week_starts, idx_names)

    # Start processing the CC data
    cc_spend_cat_pivot, cc_spend_hist_start = get_cc_debt_history(cc_spend_txn, asof_date, PROJ_WEEK1_START, CC_SPEND_TS_WEEKS)
    cc_spend_proj_cat, cc_spend_cat_pivot_top = project_cc_debt(cc_spend_cat_pivot, cc_spend_hist_start, TOP_N_CC_CATS, proj_week_starts, actual_week_starts)
    payment_event_dates, ccpay_kind, dom_mode = project_cc_payments(hist_ccpay_bank, asof_date, PROJ_WEEK1_START, proj_end_date, cadence_start, cadence_end)
    cc_payment_schedule, cc_payment_alloc = allocate_payments(cc_spend_proj_cat, cc_spend_cat_pivot_top, payment_event_dates, CC_MIX_ROLLING_WEEKS, 
                                                              proj_week_starts, idx_names, ccpay_kind, dom_mode)

    # Now combine the information to get the excel output
    combined_full = get_combined_bank(proj_bank, bank_actual_pivot, actual_week_starts, proj_week_starts, all_week_starts, cc_payment_alloc)
    inflows_present, outflows_present, total_inflows, total_outflows = build_inflows_outflows(combined_full, actual_week_starts, all_week_starts, 
                                                                                              TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, idx_names)
    beg_bal_series, end_bal_series = get_cash_balance(total_inflows, total_outflows, beginning_cash_balance, all_week_starts)
    cc_spend_proj_display, cc_spend_actual_display, cc_payment_alloc_present = get_cc_output_sheets(cc_spend_cat_pivot_top, cc_spend_proj_cat, 
                                                                                                    cc_payment_alloc, all_week_starts, proj_week_starts)
    inflows_by_cat, outflows_by_cat = get_calssifications(inflows_present, outflows_present)
    inflow_section_indexes, outflow_section_indexes, cash_balance_indexes = write_output_excel(all_week_starts, inflows_by_cat, outflows_by_cat, inflows_present, outflows_present, total_inflows, 
                       total_outflows, cc_spend_proj_display, cc_spend_actual_display, cc_payment_alloc_present,
                       cc_spend_txn, cc_payment_schedule, beg_bal_series, end_bal_series, PROJ_WEEK1_START, OUTPUT_XLSX)
    
    calculate_category_totals(OUTPUT_XLSX, inflow_section_indexes, outflow_section_indexes, cash_balance_indexes)
    
    style_projections(OUTPUT_XLSX, inflow_section_indexes, outflow_section_indexes, cash_balance_indexes)

    with open(OUTPUT_XLSX, "rb") as f:
        return f.read()