from src.general_main_process import get_cash_iq

def test_continuum_main():
    # Sample file paths (these should point to test files in your test environment)
    COA_PATH = "tests/continuum/Continuum Wellness Holdings_Account List.xlsx"
    GL_PATH = "tests/continuum/Continuum+Wellness+Holdings_Transaction+Detail+by++Account.xlsx"
    date_strt = "2026-03-12"
    OUTPUT_XLSX = "tests/continuum/output.xlsx"
    AP_AGING_PATH = "tests/continuum/Continuum Wellness Holdings_A_P Aging Detail Report.xlsx"
    previous_cashiq_path = None
    initial_cash_balance = 13000.0
    cash_floor=20000.0

    # Run the main process
    excel_bytes = get_cash_iq("Continuum",COA_PATH=COA_PATH, GL_PATH=GL_PATH, 
                                date_strt=date_strt, OUTPUT_XLSX=OUTPUT_XLSX, 
                                previous_cashiq_file=previous_cashiq_path, initial_cash_balance=initial_cash_balance, 
                                AR_AGING_PATH=None, VENDOR_SUMMARY_PATH=None, cash_floor=cash_floor, AP_AGING=AP_AGING_PATH)

    # Check that the output is a valid Excel file (starts with PK, which is the signature for ZIP files, and Excel files are ZIP archives)
    assert excel_bytes[:2] == b'PK'