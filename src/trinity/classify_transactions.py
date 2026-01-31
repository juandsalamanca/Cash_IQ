from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel
import json
import os
import streamlit as st

load_dotenv()

if os.getenv("OPENAI_API_KEY") is None:
    os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]


class InflowsFormat(BaseModel):
    collected: list[str]
    line_credit: list[str]
    other: list[str]
    

def classify_inflows(inflow_list):
    client = OpenAI()

    prompt = f"""Classify each of the cash inflows from the list provided bellow into one of the provided categories.
    The categories will have a brief description of what they mean so you can make the best classification possible.
    Do NOT change any of the names of the inflows from the provided list. The names of the categorized inflows must match
    letter for letter the names in the provided list.

    Inflows list:
    {inflow_list}

    Categories:
    collected: Any AR Customer or Account that is a Income account.
    line_credit: Any inflow that is related to a Liability account
    other: Any account labeled as Other Income.
    

    Provide the classification in the format: Inflow - Category
    """

    response = client.responses.parse(
    model="gpt-4.1",
    temperature=0,
    input=prompt,
    text_format=InflowsFormat
    )

    json_output = json.loads(response.output_parsed.model_dump_json())
    key_mapping = {'line_credit': 'Line of Credit Advances', 'other': 'Other Income', 'collected': 'AR Collected'}

    new_dict = {key_mapping[k]: v for k, v in json_output.items()}
    return new_dict


class OutflowsFormat(BaseModel):
    expenses_accounts_payable: list[str]
    credit_cards_loans: list[str]
    owner_expenses: list[str]

def classify_outflows(outflow_list):
    client = OpenAI()

    prompt = f"""Classify each of the cash outflows from the list provided bellow into one of the provided categories.
    The categories will have a brief description of what they mean so you can make the best classification possible.
    Do NOT change any of the names of the inflows from the provided list. The names of the categorized inflows must match
    letter for letter the names in the provided list.

    Outflows list:
    {outflow_list}

    Categories:
    expenses_accounts_payable: Any outflow that is from AP vendors, Expense, or an Other expense account
    credit_cards_loans: Any account that is Credit card or Liability.
    owner_expenses: Any account that is an equity account.

    Provide the classification in the format: Outflow - Category
    """

    response = client.responses.parse(
    model="gpt-4.1",
    temperature=0,
    input=prompt,
    text_format=OutflowsFormat
    )

    json_output = json.loads(response.output_parsed.model_dump_json())
    key_mapping = {'expenses_accounts_payable': 'Expenses Accounts Payable', 'credit_cards_loans': 'Credit Cards and Loans', 'owner_expenses': "Owner's Expense"}

    new_dict = {key_mapping[k]: v for k, v in json_output.items()}
    return new_dict

def get_calssifications(inflows_present, outflows_present):
    inflows_list = inflows_present.index.get_level_values('split_account').to_list()
    inflows_by_cat = classify_inflows(inflows_list)

    outflows_list = outflows_present.index.get_level_values('split_account').to_list()
    outflows_by_cat = classify_outflows(outflows_list)

    return inflows_by_cat, outflows_by_cat