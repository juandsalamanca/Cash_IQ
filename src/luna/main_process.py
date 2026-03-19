from src.luna.preprocessing import week_windows
from src.luna.cash import begin_cash, buil_actual_weekly_cash
from src.luna.ar_aging import get_assumptions
from src.luna.postprocessing import get_combined_bank
from src.projections import project_cash
from src.general_preprocessing import load_and_clean_coa, load_and_clean_gl
from src.general_postprocessing import build_inflows_outflows, get_cash_balance, write_output_excel
from src.classify_transactions import get_classifications



def get_luna_cash_iq(COA_PATH, GL_PATH, date_strt, OUTPUT_XLSX, initial_cash_balance=0.0, AR_AGING_PATH=None, VENDOR_SUMMARY_PATH=None, AR_BUCKET_ASSUMPTIONS=None):

    # TODO: 
    # First initialize the DFs and vars we need
    (PROJ_WEEK1_START, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, actual_week_starts, proj_week_starts, 
            all_week_starts, hist_week_starts, cadence_start, cadence_end, proj_end_date) = week_windows(date_strt)
    
    coa, bank_accounts, cc_accounts = load_and_clean_coa(COA_PATH)
    gl = load_and_clean_gl(GL_PATH, coa)

    # Start processing the cash data
    bank_tx, beginning_cash_balance = begin_cash(gl, coa, PROJ_WEEK1_START, bank_accounts)
    bank_actual_pivot, idx_names = buil_actual_weekly_cash(bank_tx, all_week_starts)
    _, proj_bank = project_cash(bank_actual_pivot, bank_tx, cadence_start, cadence_end, proj_week_starts, 
                                              PROJ_WEEK1_START, proj_end_date, hist_week_starts, idx_names, cc_accounts)
    
    combined = get_combined_bank(proj_bank, bank_actual_pivot, actual_week_starts, proj_week_starts, all_week_starts)

    if AR_AGING_PATH is not None:
        ar, ar_assumptions_df = get_assumptions(AR_AGING_PATH, AR_BUCKET_ASSUMPTIONS, PROJ_WEEK1_START, proj_week_starts, combined)
    else:
        ar = None
        ar_assumptions_df = None

    inflows_present, outflows_present, total_inflows, total_outflows = build_inflows_outflows(combined, actual_week_starts, all_week_starts, 
                                                                                              TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, idx_names)
    
    beg_bal_series, end_bal_series = get_cash_balance(total_inflows, total_outflows, beginning_cash_balance, all_week_starts)

    inflows_by_cat, outflows_by_cat = get_classifications("luna", inflows_present, outflows_present)

    write_output_excel(all_week_starts, inflows_by_cat, outflows_by_cat, inflows_present, outflows_present, total_inflows, 
                       total_outflows, beg_bal_series=beg_bal_series, end_bal_series=end_bal_series, PROJ_WEEK1_START=PROJ_WEEK1_START, 
                       OUTPUT_XLSX=OUTPUT_XLSX, week1_cash_balance=initial_cash_balance, VENDOR_SUMMARY_PATH=VENDOR_SUMMARY_PATH, 
                       ar=ar, ar_assumptions_df=ar_assumptions_df)

    return inflows_by_cat, outflows_by_cat