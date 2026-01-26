import streamlit as st
from src.trinity.main_process import get_trinity_cash_iq


st.header("Cash IQ")

client = st.selectbox("Select the client", ["Trinity", "Luna"])

client_map = {
    "Trinity": get_trinity_cash_iq,
    "Luna": "luna"
    }

coa_file = st.file_uploader(
    "Upload COA file", type=["xlsx", "xls"]
)

gl_file = st.file_uploader(
    "Upload GL file", type=["xlsx", "xls"]
)

date_strt = st.date_input("Select projection start date")

projection_function = client_map[client]

excel_bytes = projection_function(COA_PATH=coa_file, GL_PATH=gl_file, date_strt=date_strt, OUTPUT_XLSX="output.xlsx")


st.download_button(
    label="Download Excel",
    data=excel_bytes,
    file_name=f"Grace_Global_13_Week_Cashflow_{date_strt}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    icon=":material/download:",
)