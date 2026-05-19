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


class Account(BaseModel):
    name: str
    account_type: str
    detail: str

class MisplacedItems(BaseModel):
    misplaced: list[Account]

def get_misplaced_outflow_inflow(transaction_list, txn_type):
    client = OpenAI()

    if txn_type == "inflows":
        oposite = "outflows"
    else:
        oposite = "inflows"

    prompt = f"""I will provide for you a list of transaction labels corresponding to  {txn_type}. It will be a list of tuples of three items each.
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
    collected: list[Account]
    line_credit: list[Account]
    other: list[Account]

class TrinityOutflowsFormat(BaseModel):
    expenses_accounts_payable: list[Account]
    credit_cards_loans: list[Account]
    owner_expenses: list[Account]

class ParisiInflowsFormat(BaseModel):
    income: list[Account]
    line_credit: list[Account]
    other: list[Account]


def classify_transactions(transaction_list, transaction_type, categories, key_mapping, output_format):
    client = OpenAI()

    def get_prompt(transaction_list):
        prompt = f"""I will provide for you a list of transaction labels corresponding to  {transaction_type}. 
        It will be a list of tuples of three items each.
        The first item in each tuple will be the name of the account, the second the account type and the third the account detail.
        You need to classify each of those tuples into one of the provided categories. ALL tuples must be put into one category.
        Do NOT leave any tuple outside of the categorized output. If you're not sure about which category a tuple belongs to, make your best guess.
        The categories will have a brief description of what they mean so you can make the best classification possible.
        Do NOT change any of the names of the {transaction_type} from the provided list. The names of each of the items in the categorized 
        tuples ({transaction_type}) must match letter for letter the names in the provided list.

        {transaction_type} (tuple) list:
        {transaction_list}

        Categories:
        {categories}

        """
        return prompt
    
    prompt = get_prompt(transaction_list)

    response = client.responses.parse(
    model="gpt-5.4",
    temperature=0,
    input=prompt,
    text_format=output_format
    )

    json_output = json.loads(response.output_parsed.model_dump_json())
    print("Pre processed:")
    print(json_output)
    print("-"*100)
    new_json_output = {}
    for key in json_output:
        new_json_output[key] = [(item['name'], item['account_type'], item['detail']) for item in json_output[key]]
    print("Processed:")
    print(new_json_output)
    print("-"*100)

    missing = ["Start the loop"]
    counter = 0

    # We'll keep checking if any accounts were missed. If so, we categorize them and add them to the final result.
    while missing != []:
        
        # Verify no account is missing in the categorized output:
        general_account_list  = []
        for key in new_json_output:
            general_account_list.extend(new_json_output[key])
        print("General account list:")
        print(general_account_list)
        print("-"*100)
        missing = [item for item in transaction_list if item not in general_account_list]
        print("Missing:")
        print(missing)
        print("-"*100)
        if missing:

            prompt = get_prompt(missing)

            missing_response = client.responses.parse(
            model="gpt-5.4",
            temperature=0,
            input=prompt,
            text_format=output_format
            )

            missing_json_output = json.loads(missing_response.output_parsed.model_dump_json())
            new_missing_json_output = {}
            for key in missing_json_output:
                new_missing_json_output[key] = [(item['name'], item['account_type'], item['detail']) for item in missing_json_output[key]]
            print("New missing json outoput:")
            print(new_missing_json_output)
            print("-"*100)
            for key in new_missing_json_output:
                for item in new_missing_json_output[key]:
                    new_json_output[key].append(item)

        print("New json output:")
        print(new_json_output)
        print("-"*100)
        if counter >3:
            raise ValueError("The categorized transactions are missing accounts after 3 retries")
        print("Counter:", counter)
        counter += 1

    new_dict = {key_mapping[k]: [item[0] for item in v] for k, v in new_json_output.items()}
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
    print("Inflow list:")
    print(inflows_list)
    print("Outflow list:")
    print(outflows_list)
    print(outflows_present.index)
    print("-"*100)
    post_correction = False

    # Check if there are duplicate labels between inflows and outflows
    repeated = [item for item in inflows_list if item in outflows_list]
    print("Repeated:")
    print(repeated)
    print("-"*100)
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

    print("Inflow list:")
    print(inflows_list)
    print("Outflow list:")
    print(outflows_list)
    print("Outflows present:")
    print(outflows_present.index)
    print("-"*100)

    if post_correction:
        inflows_list, outflows_list, inflows_present, outflows_present = correct_misplaced_flows(inflows_list, 
                                                                                                 outflows_list, 
                                                                                                 inflows_present, 
                                                                                                 outflows_present)

    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)
    print(inflows_by_cat)
    print(outflows_by_cat)
    print("-"*100)

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

    inflows = [('N/P On Deck Capital (Deposit)', 'Long Term Liabilities', 'Notes Payable'), ("Owner's Investment", 'Equity', "Owner's Equity"), ('Sales', 'Income', 'Sales of Product Income'), ('Vantage LOC (Deposit)', 'Long Term Liabilities', 'Notes Payable')]
    outflows = [('Other Outflows', 'Unmapped', ''), ('AMEX LOC', 'Other Current Liabilities', 'Line of Credit'), ('Accounts Payable (A/P)', 'Accounts payable (A/P)', 'Accounts Payable (A/P)'), ('Advertising & Marketing', 'Expenses', 'Advertising/Promotional'), ('American Express', 'Unmapped', ''), ('American Express Gold Card (2003) - 2', 'Credit Card', 'Credit Card'), ('Auto:Auto Insurance', 'Expenses', 'Insurance'), ('Bank Charges & Fees:Bank Fees', 'Expenses', 'Bank Charges'), ('CREDIT CARD (5290) - 1', 'Credit Card', 'Credit Card'), ('Charitable Contributions', 'Expenses', 'Charitable Contributions'), ('Chase 4571', 'Credit Card', 'Credit Card'), ('Chase 9754', 'Credit Card', 'Credit Card'), ('Continuing Education', 'Expenses', 'Other Business Expenses'), ('Direct Deposit Payable', 'Other Current Liabilities', 'Direct Deposit Payable'), ('Dues & subscription', 'Expenses', 'Dues & subscriptions'), ('Job Supplies', 'Cost of Goods Sold', 'Supplies & Materials - COGS'), ('Legal & Professional Services:Consulting', 'Expenses', 'Legal & Professional Fees'), ('N/P Ascentium Capital', 'Long Term Liabilities', 'Notes Payable'), ('N/P Ascentium Capital 2', 'Long Term Liabilities', 'Notes Payable'), ('N/P On Deck Capital', 'Long Term Liabilities', 'Notes Payable'), ('Office Supplies & Software', 'Expenses', 'Office/General Administrative Expenses'), ("Owner's Pay & Personal Expenses", 'Equity', "Owner's Equity"), ('Payroll Expenses:Employee Health Contributions', 'Expenses', 'Insurance'), ('Payroll Liabilities:Federal Taxes (941/944)', 'Other Current Liabilities', 'Payroll Tax Payable'), ('QuickBooks Tax Holding Account', 'Other Current Assets', 'Other Current Assets'), ('Rent & Lease', 'Expenses', 'Rent or Lease of Buildings'), ('Repairs & Maintenance', 'Expenses', 'Repair & Maintenance'), ('Taxes Paid', 'Expenses', 'Taxes Paid'), ('Utilities:Electricity', 'Expenses', 'Utilities'), ('Utilities:Internet', 'Expenses', 'Utilities'), ('Vantage LOC', 'Long Term Liabilities', 'Notes Payable'), ('citi business cc', 'Credit Card', 'Credit Card'), ('American Express Gold Card (2003) - 2', 'Credit Card Payment', ''), ('CREDIT CARD (5290) - 1', 'Credit Card Payment', ''), ('Chase 9600', 'Credit Card Payment', ''), ('citi business cc', 'Credit Card Payment', ''), ('Chase 4571', 'Credit Card Payment', ''), ('My Best Buy® Visa® Card (7115) - 3', 'Credit Card Payment', ''), ('Chase 9754', 'Credit Card Payment', '')]
    inflows_by_cat = classify_transactions(inflows, "inflows", inflow_categories, key_mapping_inflows, TrinityInflowsFormat)
    print(inflows_by_cat)
    print("-"*100)
    outflows_by_cat = classify_transactions(outflows, "outflows", outflow_categories, key_mapping_outflows, TrinityOutflowsFormat)
    print(outflows_by_cat)

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
    
    #with open('client_data.json', 'w') as f:
    #    json.dump(data, f)