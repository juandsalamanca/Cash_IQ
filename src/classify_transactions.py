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


class MisplacedItem(BaseModel):
    name: str
    account_type: str
    detail: str

class MisplacedItems(BaseModel):
    misplaced: list[MisplacedItem]

def get_misplaced_outflow_inflow(transaction_list, txn_type):
    client = OpenAI()

    if txn_type == "inflows":
        oposite = "outflows"
    else:
        oposite = "inflows"

    prompt = f"""I will provide for you a list of transaction labels corresponding to  {txn_type}. It will be a list of tuples of three items.
    The first item in each tuple will be the name of the account, the second the account type and the third the account detail.
    You need to check if there is anything in that list that could correspond to the {oposite} list. If there are items that were indeed placed 
    in the wrong list, return them in a list inside a JSON with key 'misplaced'. Only return items if you have over 90% confidence of them being misplaced. 
    I'll give you a couple of examples:
    Anything named 'Accounts Payable' should be an outflow.
    Anything named 'AR Collected' should be an inflow.
    
    Remember, do NOT return anything that you are not highly confident of.
    Do not return anything but the misplaced tuples. If none were misplaced (not confident of any of them), return empty list.
    
    Here's the list of transactions:
    {transaction_list}"""

    response = client.responses.parse(
    model="gpt-5.4",
    temperature=0,
    input=prompt,
    text_format=MisplacedItems
    )

    json_output = json.loads(response.output_parsed.model_dump_json())

    misplaced_list = [(item['name'], item['account_type'], item['detail']) for item in json_output['misplaced']]

    return misplaced_list


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
        if label == idx:
            print("Matching index:", idx)
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

    inflows_list = inflows_present.index.to_list()
    outflows_list = outflows_present.index.to_list()



    post_correction = False

    # Check if there are duplicate labels between inflows and outflows
    repeated = [item for item in inflows_list if item in outflows_list]
    # If so, we add " (Deposit)" to the label so we don't get duplicate indexes in the same projections table
    # This will avoid getting an index malfunction (empty rows) when putting the rows in the build_projections_table funciton
    if repeated:
        repeated_set = set(repeated)
        new_inflows_index = []

        for idx in inflows_present.index.to_list():
            if idx in repeated_set:
                idx_list = list(idx)
                idx_list[0] += " (Deposit)"
                new_inflows_index.append(tuple(idx_list))
            else:
                new_inflows_index.append(idx)

        if isinstance(inflows_present.index, pd.MultiIndex):
            inflows_present.index = pd.MultiIndex.from_tuples(new_inflows_index, names=inflows_present.index.names)
        else:
            inflows_present.index = pd.Index(new_inflows_index, name=inflows_present.index.name)

        inflows_list = new_inflows_index


    if post_correction:
        inflows_list, outflows_list, inflows_present, outflows_present = correct_misplaced_flows(inflows_list, 
                                                                                                 outflows_list, 
                                                                                                 inflows_present, 
                                                                                                 outflows_present)

    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)

    return inflows_by_cat, outflows_by_cat, inflows_present, outflows_present

# Deprecated function, any misplacing should be taken care of in the projections logic with the detect_type_of_account function
def correct_misplaced_flows(inflows_list, outflows_list, inflows_present, outflows_present):
    misplaced_inflows = get_misplaced_outflow_inflow(inflows_list, 'inflows')
    misplaced_outflows = get_misplaced_outflow_inflow(outflows_list, 'outflows')

    print("Misplaced Inflows:")
    print(misplaced_inflows)
    print("Misplaced Outflows:")
    print(misplaced_outflows)

    if misplaced_inflows:
        inflows_list = [item for item in inflows_list if item not in misplaced_inflows]
        # We check if any of the misplaced inflows are already in the outflows
        repeated = [item for item in misplaced_inflows if item in outflows_list]
        if repeated:
            print("Repeated:", repeated)
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
            print("Repeated:", repeated)
            # We take out the repeated items from the misplaced list
            misplaced_outflows = [item for item in misplaced_outflows if item not in repeated]
            # Now drop the repeated items from the outflows and sum them with the appropriate row in the inflows
            for label in repeated:
                outflows_present, inflows_present = merge_transactions_with_same_label(label, 'outflows', inflows_present, outflows_present)

        inflows_list.extend(misplaced_outflows)

    if not isinstance(inflows_list[0], str):
        inflows_list = [item[0] for item in inflows_list]
    if not isinstance(outflows_list[0], str):
        outflows_list = [item[0] for item in outflows_list]

    print("New Inflows:")
    print(inflows_list)
    print("New Outflows:")
    print(outflows_list)
    return inflows_list, outflows_list, inflows_present, outflows_present

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

    
if __name__ == "__main__":
    inflows = [('Other Inflows', 'Unmapped', ''), ('Benjamin Quirk', 'Unmapped', ''), ('Channel selling fees:Stripe fees', 'Cost of Goods Sold', 'Other Costs of Services - COS'), ('Christine Fujiyama', 'Unmapped', ''), ('Deferred Revenue', 'Other Current Liabilities', 'Deferred Revenue'), ('Income:Initiation/Onboarding Fee', 'Income', 'Service/Fee Income'), ('Income:Membership Fee Income', 'Income', 'Service/Fee Income'), ('Income:Other - Cancellations/Late Fees', 'Income', 'Discounts/Refunds Given'), ('Justin Dean', 'Unmapped', ''), ('Kevin Schwartz:31000 Contributions - Kevin Schwartz', 'Unmapped', ''), ('Matthew Stadtmauer', 'Unmapped', ''), ('Ruth Stadtmauer', 'Unmapped', '')]
    response = get_misplaced_outflow_inflow(inflows, 'inflows')
    print(response)