from src.classify_transactions import classify_transactions, TrinityInflowsFormat, TrinityOutflowsFormat, ParisiInflowsFormat
import json

#--------------------------------------------------
#        Test Inflows/Outflows Classification
#--------------------------------------------------

outflows = """Direct Deposit Payable
Direct Deposit Payable
N/P On Deck Capital
AMEX LOC
Accounts Payable (A/P)
Equipment
Owner's Pay & Personal Expenses
QuickBooks Tax Holding Account
Employee Health Contributions
Chase 9754
Taxes & Licenses
citi business cc
Legal & Professional Services:Consulting
Job Supplies
Utilities:Internet
Office Supplies & Software
Bank Charges & Fees:Bank Fees
Charitable Contributions
Utilities
Auto:Auto Insurance
My Best Buy® Visa® Card (7115) - 3
CREDIT CARD (5290) - 1
Dues & subscription
"""

inflows = """Sales
Keyston
Action Gypsum
Riverside
Norwegian
Vantage LOC Draw
Trinity Eagle Loan Proceeds
Other Inflows
Owner's Investment"""

def test_classifications():

    client = "trinity"
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
    
    inflows_list = inflows.split("\n")
    outflows_list = outflows.split("\n")

    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)

    print(inflows_by_cat)
    print(outflows_by_cat)

    assert isinstance(inflows_by_cat, dict)
    assert isinstance(outflows_by_cat, dict)