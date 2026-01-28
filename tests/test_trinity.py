import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from src.trinity.main_process import get_trinity_cash_iq

def test_trinity_main():
    # Sample file paths (these should point to test files in your test environment)
    COA_PATH = "tests/Grace Global Logistics Inc_Account List.xlsx"
    GL_PATH = "tests/Grace Global Logistics Inc_Transaction Detail by Account.xlsx"
    date_strt = "2026-01-12"
    OUTPUT_XLSX = "tests/output.xlsx"

    # Run the main process
    excel_bytes = get_trinity_cash_iq(COA_PATH=COA_PATH, GL_PATH=GL_PATH, date_strt=date_strt, OUTPUT_XLSX=OUTPUT_XLSX)

    # Check that the output is not empty
    assert excel_bytes is not None
    assert len(excel_bytes) > 0

    # Optionally, you can add more checks here to validate the contents of the output Excel file
    output_projections_df = pd.read_excel(OUTPUT_XLSX, sheet_name='Projections (Table)')
    ground_truth_projections_df = pd.read_excel("tests/Grace_Global_13_Week_Cashflow_v5_2026-01-12.xlsx", sheet_name='Projections (Table)')

    assert_frame_equal(output_projections_df, ground_truth_projections_df)