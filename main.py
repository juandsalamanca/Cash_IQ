import streamlit as st
from src.trinity.main_process import get_trinity_cash_iq
from src.parisi.main_process import get_parisi_cash_iq
from src.strivewell.main_process import get_strivewell_cash_iq
from src.luna.main_process import get_luna_cash_iq
from src.ai_summary import get_summary

if "excel_bytes" not in st.session_state:
    st.session_state.excel_bytes = None
if "summary_bytes" not in st.session_state:
    st.session_state.summary_bytes = None
st.header("Cash IQ")

client = st.selectbox("Select the client", ["Trinity", "Parisi", "Luna", "Strivewell", "SupafitGrow", "Gamechanger"])

client_map = {
    "Trinity": get_trinity_cash_iq,
    "Parisi": get_parisi_cash_iq,
    "Luna": get_luna_cash_iq,
    "Strivewell": get_strivewell_cash_iq,
    "SupafitGrow": get_trinity_cash_iq,
    "Gamechanger": get_trinity_cash_iq
    }

coa_file = None
previous_cashiq_file = None
ar_file = None
vendor_file = None

initial_cash_balance = st.number_input("Enter initial cash balance", min_value=0.0)

if client in ["Trinity", "Strivewell", "SupafitGrow", "Gamechanger"]:

    coa_file = st.file_uploader(
        "Upload COA file", type=["xlsx", "xls"]
    )

if client == "Luna":

    coa_file = st.file_uploader(
        "Upload COA file", type=["xlsx", "xls"]
    )
    
    ar_file = st.file_uploader(
        "Upload AR Aging file", type=["xlsx", "xls"]
    )

    vendor_file = st.file_uploader(
        "Upload Balance sheet file (optional)", type=["xlsx", "xls"]
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
elif client in ["Trinity", "Strivewell"]:
    condition = coa_file and gl_file and date_strt
elif client == "Luna":
    condition = ar_file and date_strt and gl_file and coa_file

process = st.button("Process")

output_file_name = "output.xlsx"
if process:

    if not(condition):
        st.error("Please upload all required files and select a date.")

    else:
        try:
            st.session_state.excel_bytes = projection_function(COA_PATH=coa_file, GL_PATH=gl_file, date_strt=date_strt, OUTPUT_XLSX=output_file_name, previous_cashiq_path=previous_cashiq_file, initial_cash_balance=initial_cash_balance, AR_AGING_PATH=ar_file, VENDOR_SUMMARY_PATH=vendor_file)
        except ValueError as e:
            st.error(str(e))
        
            
if st.session_state.excel_bytes is not None:

    if client == "Trinity":
            f_name = f"Grace_Global_13_Week_Cashflow_{date_strt}.xlsx"
    elif client == "Parisi":
        f_name = f"Parisi_Speed_School_{date_strt}.xlsx"
    else:
        f_name = f"{client}_Cash_IQ_{date_strt}.xlsx"

    st.download_button(
        label="Download Excel",
        data=st.session_state.excel_bytes,
        file_name=f_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        icon=":material/download:",
    )
    summary_button = st.button("Get summary")
    if summary_button:
        st.session_state.summary_bytes = get_summary(date_strt, output_file_name)

    if st.session_state.summary_bytes is not None:

        st.download_button(
            label="Download Summary",
            data=st.session_state.summary_bytes,
            file_name="summary.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            icon=":material/download:",
            )