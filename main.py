import requests
import streamlit as st
from src.general_main_process import get_cash_iq
from src.ai_summary import get_summary
import os
from dotenv import load_dotenv
load_dotenv()

if os.getenv("GSHEET_URL") is None:
    os.environ["GSHEET_URL"] = st.secrets["GSHEET_URL"]

@st.cache_data(show_spinner=False)
def update_gsheet(client, data):

    url = os.getenv("GSHEET_URL")
    payload = {
    client.lower(): data
    }

    requests.post(url, json=payload)

@st.cache_data(show_spinner=False)
def retrieve_client_data(client):
    url = os.getenv("GSHEET_URL")
    data = requests.get(url)
    try:
        client_data = data.json().get(client.lower(), None)
        return float(client_data)
    except Exception as e:
        st.warning(f"Error retrieving the cash for client: {client}")
        return None

if "excel_bytes" not in st.session_state:
    st.session_state.excel_bytes = None
if "summary_bytes" not in st.session_state:
    st.session_state.summary_bytes = None

coa_file = None
previous_cashiq_file = None
ar_file = None
vendor_file = None

st.header("Cash IQ", text_alignment="center")

col1, col2 = st.columns(2, gap="xlarge")

with col2:

    client = st.selectbox("Select the client", ["Trinity", "Parisi", "Luna", "Strivewell", "Continuum", "SupafitGrow", "Gamechanger"])

    initial_cash_balance = st.number_input("Enter initial cash balance", min_value=0.0)

    date_strt = str(st.date_input("Select projection start date")).replace("/", "-")

with col1:

    if client in ["Trinity", "Strivewell", "Continuum", "SupafitGrow", "Gamechanger"]:

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

with col2:
    with st.spinner("Retrieving saved cash floor...", show_time=True):
        saved_cash_floor = retrieve_client_data(client)    
    cash_floor = st.number_input("Enter cash floor", value=saved_cash_floor, min_value=0.0)


if client == "Parisi":
    condition = gl_file and date_strt
elif client in ["Trinity", "Strivewell", "Continuum", "SupafitGrow", "Gamechanger"]:
    condition = coa_file and gl_file and date_strt
elif client == "Luna":
    condition = ar_file and date_strt and gl_file and coa_file

process = st.button("Process")

output_file_name = "output.xlsx"
if process:

    if saved_cash_floor != float(cash_floor):
        with st.spinner("Updating cash floor in Google Sheet...", show_time=True):
            update_gsheet(client, cash_floor)

    if not(condition):
        st.error("Please upload all required files and select a date.")

    else:
        try:
            st.session_state.excel_bytes = get_cash_iq(client=client, COA_PATH=coa_file, GL_PATH=gl_file, 
                                                       date_strt=date_strt, OUTPUT_XLSX=output_file_name, 
                                                       previous_cashiq_file=previous_cashiq_file, initial_cash_balance=initial_cash_balance, 
                                                       AR_AGING_PATH=ar_file, VENDOR_SUMMARY_PATH=vendor_file, cash_floor=cash_floor)
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

    official_client_name_mapping = {
        "Trinity": "Trinity Logistics", 
        "Parisi": "Parisi", 
        "Luna": "Luna Locums", 
        "Strivewell": "Strivewell", 
        "Continuum": "Continuum", 
        "SupafitGrow": "SupafitGrow", 
        "Gamechanger": "Gamechanger"
    }

    client_name = official_client_name_mapping.get(client, client)

    client_name = st.text_input("Client Name for summary", value=client_name)
    summary_button = st.button("Get summary")
    if summary_button:
        with st.spinner("Getting AI summary...", show_time=True):
            st.session_state.summary_bytes = get_summary(client_name, date_strt, output_file_name)

    if st.session_state.summary_bytes is not None:

        st.download_button(
            label="Download Summary",
            data=st.session_state.summary_bytes,
            file_name="summary.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            icon=":material/download:",
            )
        

