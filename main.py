import streamlit as st
from src.trinity.main_process import get_trinity_cash_iq
from src.parisi.main_process import get_parisi_cash_iq


st.header("Cash IQ")

client = st.selectbox("Select the client", ["Trinity", "Parisi", "Luna", "Strivewell", "SupafitGrow", "Gamechanger"])

if client == "Luna":
    st.warning("Luna's Cash IQ is currently under development. Please check back later.")
    st.stop()

client_map = {
    "Trinity": get_trinity_cash_iq,
    "Parisi": get_parisi_cash_iq,
    "Luna": "luna",
    "Strivewell": get_trinity_cash_iq,
    "SupafitGrow": get_trinity_cash_iq,
    "Gamechanger": get_trinity_cash_iq
    }

coa_file = None
previous_cashiq_file = None

initial_cash_balance = st.number_input("Enter initial cash balance", min_value=0.0)

if client in ["Trinity", "Strivewell", "SupafitGrow", "Gamechanger"]:

    coa_file = st.file_uploader(
        "Upload COA file", type=["xlsx", "xls"]
    )
    
gl_file = st.file_uploader(
    "Upload GL file", type=["xlsx", "xls"]
)

previous_cashiq_file = st.file_uploader(
    "Upload previous Cash IQ file", type=["xlsx", "xls"]
)

date_strt = str(st.date_input("Select projection start date")).replace("/", "-")
projection_function = client_map[client]

if client == "Parisi":
    condition = gl_file and date_strt
elif client == "Trinity":
    condition = coa_file and gl_file and date_strt

process = st.button("Process")

if process:

    if not(condition):
        st.error("Please upload all required files and select a date.")

    else:

        excel_bytes = projection_function(COA_PATH=coa_file, GL_PATH=gl_file, date_strt=date_strt, OUTPUT_XLSX="output.xlsx", previous_cashiq_path=previous_cashiq_file, initial_cash_balance=initial_cash_balance)
        if client == "Trinity":
             f_name = f"Grace_Global_13_Week_Cashflow_{date_strt}.xlsx"
        elif client == "Parisi":
            f_name = f"Parisi_Speed_School_{date_strt}.xlsx"
        else:
            f_name = f"{client}_Cash_IQ_{date_strt}.xlsx"
            
        st.download_button(
            label="Download Excel",
            data=excel_bytes,
            file_name=f_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            icon=":material/download:",
        )