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


class MisplacedItems(BaseModel):
    misplaced: list[str]

def get_misplaced_outflow_inflow(transaction_list, txn_type):
    client = OpenAI()

    if txn_type == "inflows":
        oposite = "outflows"
    else:
        oposite = "inflows"

    prompt = f"""I will provide for you a list of transaction labels corresponding to  {txn_type}. You need to check if there is anything in that list
    that could correspond to the {oposite} list. If there are items were indeed placed in the wrong list, return them separated by commas. 
    Only return items if you have over 90% confidence of them being misplaced. I'll give you a couple of examples:
    Anything named 'Accounts Payable' should be an outflow.
    Anything named 'AR Collected' should be an inflow.
    
    Remember, do NOT return anything that you are not highly confident of.
    Do not return anything but the misplaced items. If none were misplaced (not confident of any of them), return empty string.
    
    Here's the list of transactions:
    {transaction_list}"""

    response = client.responses.parse(
    model="gpt-5.4",
    temperature=0,
    input=prompt,
    text_format=MisplacedItems
    )

    json_output = json.loads(response.output_parsed.model_dump_json())

    print(json_output)

    return json_output['misplaced']


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

    """

    response = client.responses.parse(
    model="gpt-5.4",
    temperature=0,
    input=prompt,
    text_format=output_format
    )

    json_output = json.loads(response.output_parsed.model_dump_json())

    new_dict = {key_mapping[k]: v for k, v in json_output.items()}
    return new_dict

def merge_transactions_with_same_label(label, original_location, inflows_present, outflows_present):

    """ 
    We find the misplaced row, save it, drop it from the original DF and then sum its values with the 
    corresponding one in the destination DF.
    """

    if original_location == "inflows":
        origin = inflows_present
        destination = outflows_present
    else:
        origin = outflows_present
        destination = inflows_present

    for idx in origin.index:
        if label in idx:
            print(idx)
            break
    row = origin.loc[idx]
    origin = origin.drop(index=idx)
    destination.loc[idx] += row
    return origin, destination

    
def get_classifications(client, inflows_present, outflows_present):

    with open("client_data.json", "r") as f:
        data = json.load(f)

    client_data = data[client]
    inflow_categories = client_data["inflow_categories"]
    outflow_categories = client_data["outflow_categories"]
    key_mapping_inflows = client_data["key_mapping_inflows"]
    key_mapping_outflows = client_data["key_mapping_outflows"]

    if client in ["trinity", "strivewell", "luna", "continuum"]:
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

    if isinstance(outflows_present.index, pd.core.indexes.multi.MultiIndex):
        outflows_list = outflows_present.index.get_level_values('split_account').to_list()
    else:
        outflows_list = outflows_present.index.to_list()


    misplaced_inflows = get_misplaced_outflow_inflow(inflows_list, 'inflows')
    misplaced_outflows = get_misplaced_outflow_inflow(outflows_list, 'outflows')

    if misplaced_inflows:
        inflows_list = [item for item in inflows_list if item not in misplaced_inflows]
        # We check if any of the misplaced inflows are already in the outflows
        repeated = [item for item in misplaced_inflows if item in outflows_list]
        if repeated:
            # We take out the repeated items from the misplaced list
            misplaced_inflows = [item for item in misplaced_inflows if item not in repeated]
            # Now drop the repeated items from the inflows and sum them with the appropriate row in the outflows
            for label in repeated:
                inflows_present, outflows_present = merge_transactions_with_same_label(label, 'inflows', inflows_present, outflows_present)
        outflows_list.extend(misplaced_inflows)

    if misplaced_outflows:
        outflows_list = [item for item in outflows_list if item not in misplaced_outflows]
         # We check if any of the misplaced outflows are already in the inflows
        repeated = [item for item in misplaced_outflows if item in inflows_list]
        if repeated:
            # We take out the repeated items from the misplaced list
            misplaced_outflows = [item for item in misplaced_outflows if item not in repeated]
            # Now drop the repeated items from the outflows and sum them with the appropriate row in the inflows
            for label in repeated:
                outflows_present, inflows_present = merge_transactions_with_same_label(label, 'outflows', inflows_present, outflows_present)

        inflows_list.extend(misplaced_outflows)

    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)

    return inflows_by_cat, outflows_by_cat, inflows_present, outflows_present


if __name__ == "__main__":
    #pass
    
    inflow_categories = """collected: Any AR Customer or Account that is a Income account. Any proper names from cusotmers should also be under this category.
    line_credit: Any inflow that is related to a Liability account
    other: Any account labeled as Other Income."""

    outflow_categories = """expenses_accounts_payable: Any outflow that is from AP vendors (account payable), Expense, or an Other expense account
    credit_cards_loans: Any account that is Credit card or Loan.
    owner_expenses: Any account that is an equity account."""

    key_mapping_inflows = {'line_credit': 'Line of Credit Advances and Loan', 'other': 'Other Income', 'collected': 'AR Collected'}
    key_mapping_outflows = {'expenses_accounts_payable': 'Expenses & Accounts Payable', 'credit_cards_loans': 'Credit Cards And Loans', 'owner_expenses': "Owner's Expense"}

    parisi_inflow_categories = """income: Any AR Customer or Account that is a Income account.
    line_credit: Any inflow that is related to a Liability account
    other: Any account labeled as Other Income."""

    parisi_key_mapping_inflows = {'line_credit': 'Line of Credit Advances', 'other': 'Other Income', 'income': 'Income'}

    import json

    data = {"trinity":
            {"inflow_categories": inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows},
            "strivewell":
            {"inflow_categories": inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows},
            "luna":
            {"inflow_categories": inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows},
            "continuum":
            {"inflow_categories": inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows},
            "parisi": 
            {"inflow_categories": parisi_inflow_categories, "outflow_categories": outflow_categories, "key_mapping_inflows": parisi_key_mapping_inflows, "key_mapping_outflows": key_mapping_outflows}}
    
    with open('client_data.json', 'w') as f:
        json.dump(data, f)

    