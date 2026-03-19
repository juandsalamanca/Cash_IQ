from src.general_main_process import get_cash_iq

def test_luna_main():
    COA_PATH = "tests/luna/Luna Locums LLC_Account List.xlsx"
    GL_PATH = "tests/luna/Luna Locums LLC_Transaction Detail by Account.xlsx"
    AR_AGING_PATH = "tests/luna/Luna Locums LLC_A_R Aging Summary Report.xlsx"
    VENDOR_SUMMARY_PATH = "tests/luna/Luna Locums LLC_Balance Sheet.xlsx"
    OUTPUT_XLSX = "tests/luna/output.xlsx"
    previous_cashiq_path = "tests/luna/Luna Locum Weekly Projections CashIQ 02_16_2026.xlsx"
    initial_cash_balance = 11000.0
    cash_floor=30000.0
    date_strt = "2026-02-26"


    excel_bytes = get_cash_iq("Luna", COA_PATH=COA_PATH, GL_PATH=GL_PATH, 
                                date_strt=date_strt, OUTPUT_XLSX=OUTPUT_XLSX, 
                                previous_cashiq_file=previous_cashiq_path, initial_cash_balance=initial_cash_balance, 
                                AR_AGING_PATH=AR_AGING_PATH, VENDOR_SUMMARY_PATH=VENDOR_SUMMARY_PATH, cash_floor=cash_floor)

    # Check that the output is a valid Excel file (starts with PK, which is the signature for ZIP files, and Excel files are ZIP archives)
    assert excel_bytes[:2] == b'PK'