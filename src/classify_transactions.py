from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel
import json
import os
import streamlit as st
import pandas as pd

load_dotenv()

if os.getenv("OPENAI_API_KEY") is None:
    os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]


class TrinityInflowsFormat(BaseModel):
    collected: list[str]
    line_credit: list[str]
    other: list[str]

class TrinityOutflowsFormat(BaseModel):
    expenses_accounts_payable: list[str]
    credit_cards_loans: list[str]
    owner_expenses: list[str]

class ParisiInflowsFormat(BaseModel):
    income: list[str]
    line_credit: list[str]
    other: list[str]


def classify_transactions(transaction_list, transaction_type, categories, key_mapping, output_format):
    client = OpenAI()

    prompt = f"""Classify each of the cash {transaction_type} from the list provided bellow into one of the provided categories.
    The categories will have a brief description of what they mean so you can make the best classification possible.
    Do NOT change any of the names of the {transaction_type} from the provided list. The names of the categorized {transaction_type} must match
    letter for letter the names in the provided list.

    {transaction_type} list:
    {transaction_list}

    Categories:
    {categories}

    Provide the classification in the format: Category - List of {transaction_type}
    """

    response = client.responses.parse(
    model="gpt-4.1",
    temperature=0,
    input=prompt,
    text_format=output_format
    )

    json_output = json.loads(response.output_parsed.model_dump_json())

    new_dict = {key_mapping[k]: v for k, v in json_output.items()}
    return new_dict

def get_classifications(client, inflows_present, outflows_present):

    with open("client_data.json", "r") as f:
        data = json.load(f)

    client_data = data[client]
    inflow_categories = client_data["inflow_categories"]
    outflow_categories = client_data["outflow_categories"]
    key_mapping_inflows = client_data["key_mapping_inflows"]
    key_mapping_outflows = client_data["key_mapping_outflows"]

    if client == "trinity":
        inflows_format = TrinityInflowsFormat
        outflows_format = TrinityOutflowsFormat
    elif client == "parisi":
        inflows_format = ParisiInflowsFormat
        outflows_format = TrinityOutflowsFormat
    else:
        raise ValueError(f"Unsupported client: {client}")

    if isinstance(inflows_present.index, pd.core.indexes.multi.MultiIndex):
        inflows_list = inflows_present.index.get_level_values('split_account').to_list()
    else:
        inflows_list = inflows_present.index.to_list()
    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)

    if isinstance(outflows_present.index, pd.core.indexes.multi.MultiIndex):
        outflows_list = outflows_present.index.get_level_values('split_account').to_list()
    else:
        outflows_list = outflows_present.index.to_list()
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)

    return inflows_by_cat, outflows_by_cat

if __name__ == "__main__":
    pass
    '''
    inflow_categories = """collected: Any AR Customer or Account that is a Income account.
    line_credit: Any inflow that is related to a Liability account
    other: Any account labeled as Other Income."""

    outflow_categories = """expenses_accounts_payable: Any outflow that is from AP vendors, Expense, or an Other expense account
    credit_cards_loans: Any account that is Credit card or Liability.
    owner_expenses: Any account that is an equity account."""

    key_mapping_inflows = {'line_credit': 'Line of Credit Advances', 'other': 'Other Income', 'collected': 'AR Collected'}
    key_mapping_outflows = {'expenses_accounts_payable': 'Expenses Accounts Payable', 'credit_cards_loans': 'Credit Cards and Loans', 'owner_expenses': "Owner's Expense"}

    parisi_inflow_categories = """income: Any AR Customer or Account that is a Income account.
    line_credit: Any inflow that is related to a Liability account
    other: Any account labeled as Other Income."""

    parisi_key_mapping_inflows = {'line_credit': 'Line of Credit Advances', 'other': 'Other Income', 'income': 'Income'}

    import json

    data = {"trinity":
            {"inflow_categories": inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows},
            "parisi": 
            {"inflow_categories": parisi_inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": parisi_key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows}}
    
    with open('client_data.json', 'w') as f:
        json.dump(data, f)

    '''