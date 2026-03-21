from src.general_main_process import get_cash_iq
from src.ai_summary import get_summary

def test_trinity_main():
    # Sample file paths (these should point to test files in your test environment)
    COA_PATH = "tests/trinity/Grace Global Logistics Inc_Account List CashIQ (4).xlsx"
    GL_PATH = "tests/trinity/Grace Global Logistics Inc_Transaction Detail by Account CashIQ (4).xlsx"
    date_strt = "2026-02-16"
    OUTPUT_XLSX = "tests/trinity/output.xlsx"
    previous_cashiq_path = "tests/trinity/2.9 Trinity Logistics Weekly Projections CashIQ 02_9_2026.xlsx"
    initial_cash_balance = 13000.0
    cash_floor=40000.0
    client = "Trinity"

    # Run the main process
    excel_bytes = get_cash_iq(client, COA_PATH=COA_PATH, GL_PATH=GL_PATH, 
                                date_strt=date_strt, OUTPUT_XLSX=OUTPUT_XLSX, 
                                previous_cashiq_file=previous_cashiq_path, initial_cash_balance=initial_cash_balance, 
                                AR_AGING_PATH=None, VENDOR_SUMMARY_PATH=None, cash_floor=cash_floor)
    
    summary_bytes = get_summary(client, date_strt, OUTPUT_XLSX)

    # Check that the outputs are valid Excel and Word files (starts with PK, which is the signature for ZIP files
    # Word and Excel files are ZIP archives)
    # This is just to check of the functions can run without raising erros and produce valid files.
    assert excel_bytes[:2] == b'PK'
    assert summary_bytes[:2] == b'PK'