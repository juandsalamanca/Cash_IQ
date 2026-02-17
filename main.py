import streamlit as st
from src.trinity.main_process import get_trinity_cash_iq
from src.parisi.main_process import get_parisi_cash_iq


st.header("Cash IQ")

client = st.selectbox("Select the client", ["Trinity", "Parisi", "Luna"])

client_map = {
    "Trinity": get_trinity_cash_iq,
    "Parisi": get_parisi_cash_iq,
    "Luna": "luna"
    }

coa_file = None
previous_cashiq_file = None

if client == "Trinity":

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

        excel_bytes = projection_function(COA_PATH=coa_file, GL_PATH=gl_file, date_strt=date_strt, OUTPUT_XLSX="output.xlsx", previous_cashiq_path=previous_cashiq_file)
        if client == "Trinity":
             f_name = f"Grace_Global_13_Week_Cashflow_{date_strt}.xlsx"
        elif client == "Parisi":
            f_name = f"Parisi_Speed_School_{date_strt}.xlsx"
            
        st.download_button(
            label="Download Excel",
            data=excel_bytes,
            file_name=f_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            icon=":material/download:",
        )