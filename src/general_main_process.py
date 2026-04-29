import streamlit as st
import traceback
import os
import warnings
from src.retroactive_comparison import compare_reports
from src.styling import style_projections
from src.general_postprocessing import calculate_category_totals
from src.balance_integrations import integrate_balance
from src.trinity.main_process import get_trinity_cash_iq
from src.parisi.main_process import get_parisi_cash_iq
from src.strivewell.main_process import get_strivewell_cash_iq
from src.luna.main_process import get_luna_cash_iq
from src.continuum.main_process import get_continuum_cash_iq


@st.cache_data(show_spinner=False)
def get_cash_iq(client, COA_PATH, GL_PATH, date_strt, OUTPUT_XLSX, previous_cashiq_file, 
                initial_cash_balance=0.0, AR_AGING_PATH=None, VENDOR_SUMMARY_PATH=None, 
                AR_BUCKET_ASSUMPTIONS=None, cash_floor=0.0, AP_AGING=None, BALANCE=None):
    
    
    client_map = {
        "Trinity": get_trinity_cash_iq,
        "Parisi": get_parisi_cash_iq,
        "Luna": get_luna_cash_iq,
        "Strivewell": get_strivewell_cash_iq,
        "Continuum": get_continuum_cash_iq,
        "SupafitGrow": get_trinity_cash_iq,
        "Gamechanger": get_trinity_cash_iq
        }
    
    main_function = client_map.get(client)

    progress_text = "Processing raw data and generating projections..."
    my_bar = st.progress(0, text=progress_text)
        
    inflows_by_cat, outflows_by_cat, cc_accounts = main_function(COA_PATH, GL_PATH, date_strt, OUTPUT_XLSX, 
                initial_cash_balance=initial_cash_balance, AR_AGING_PATH=AR_AGING_PATH, VENDOR_SUMMARY_PATH=VENDOR_SUMMARY_PATH, 
                AR_BUCKET_ASSUMPTIONS=AR_BUCKET_ASSUMPTIONS, AP_AGING=AP_AGING, BALANCE=BALANCE)
    
    if BALANCE is not None:
        try:
            integrate_balance(BALANCE, cc_accounts, OUTPUT_XLSX)
        except Exception as e:
            traceback.print_exc()
            st.warning(f"Error intergating the balance sheet: {str(e)}")
            warnings.warn(f"Error intergating the balance sheet: {str(e)}")

    
    if previous_cashiq_file is not None:
        try:
            my_bar.progress(40, text="Learning from previous CashIQ report...")
            TEMP_OUTPUT_XLSX = calculate_category_totals(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)
            if isinstance(previous_cashiq_file, str):
                previous_cashiq_path = previous_cashiq_file
            else:
                previous_cashiq_path = "temp_previous_cashiq.xlsx"
                with open(previous_cashiq_path, "wb") as tmp:
                    tmp.write(previous_cashiq_file.getvalue())
            compare_reports(previous_cashiq_path, TEMP_OUTPUT_XLSX, OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)
            os.remove(previous_cashiq_path)

        except Exception as e:
            traceback.print_exc()
            st.warning(f"Error comparing with previous Cash IQ report: {str(e)}")
            warnings.warn(f"Error comparing with previous Cash IQ report: {str(e)}")

    try:
        my_bar.progress(70, text="Styling projections in output Excel file...")
        style_projections(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat, cash_floor)
    except Exception as e:
        traceback.print_exc()
        st.warning(f"Error styling projections in output Excel file: {str(e)}")
        warnings.warn(f"Error styling projections in output Excel file: {str(e)}")

    try:
        my_bar.progress(90, text="Calculating category totals with excel formulas...")
        calculate_category_totals(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)
    except Exception as e:
        traceback.print_exc()
        st.warning(f"Error calculating category totals with excel formulas after the styling: {str(e)}")
        warnings.warn(f"Error calculating category totals with excel formulas after the styling: {str(e)}")

    my_bar.progress(100, text="Done")
    with open(OUTPUT_XLSX, "rb") as f:
        return f.read()
    
    