from src.general_main_process import get_cash_iq

def test_strivewell_main():
    # Sample file paths (these should point to test files in your test environment)
    COA_PATH = "tests/strivewell/NJ Sweat LLC, a StriveWell company_Account List.xlsx"
    GL_PATH = "tests/strivewell/NJ Sweat LLC, a StriveWell company_Transaction Detail by Account.xlsx"
    previous_cashiq_path = "tests/strivewell/2.23 NJ Sweat Weekly Projections CashIQ 02_23_2026.xlsx"
    date_strt = "2026-03-02"
    OUTPUT_XLSX = "tests/strivewell/output.xlsx"
    initial_cash_balance = 12000.0

    # Run the main process
    excel_bytes = get_cash_iq("Strivewell", COA_PATH=COA_PATH, GL_PATH=GL_PATH, date_strt=date_strt, OUTPUT_XLSX=OUTPUT_XLSX, previous_cashiq_path=previous_cashiq_path, initial_cash_balance=initial_cash_balance)

    # Check that the output is a valid Excel file (starts with PK, which is the signature for ZIP files, and Excel files are ZIP archives)
    assert excel_bytes[:2] == b'PK'