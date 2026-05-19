from src.classify_transactions import classify_transactions, TrinityInflowsFormat, TrinityOutflowsFormat, ParisiInflowsFormat
import json

#--------------------------------------------------
#        Test Inflows/Outflows Classification
#--------------------------------------------------



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
    
    inflows_list = [('N/P On Deck Capital (Deposit)', 'Long Term Liabilities', 'Notes Payable'), ("Owner's Investment", 'Equity', "Owner's Equity"), ('Sales', 'Income', 'Sales of Product Income'), ('Vantage LOC (Deposit)', 'Long Term Liabilities', 'Notes Payable')]
    outflows_list = [('Other Outflows', 'Unmapped', ''), ('AMEX LOC', 'Other Current Liabilities', 'Line of Credit'), ('Accounts Payable (A/P)', 'Accounts payable (A/P)', 'Accounts Payable (A/P)'), ('Advertising & Marketing', 'Expenses', 'Advertising/Promotional'), ('American Express', 'Unmapped', ''), ('American Express Gold Card (2003) - 2', 'Credit Card', 'Credit Card'), ('Auto:Auto Insurance', 'Expenses', 'Insurance'), ('Bank Charges & Fees:Bank Fees', 'Expenses', 'Bank Charges'), ('CREDIT CARD (5290) - 1', 'Credit Card', 'Credit Card'), ('Charitable Contributions', 'Expenses', 'Charitable Contributions'), ('Chase 4571', 'Credit Card', 'Credit Card'), ('Chase 9754', 'Credit Card', 'Credit Card'), ('Continuing Education', 'Expenses', 'Other Business Expenses'), ('Direct Deposit Payable', 'Other Current Liabilities', 'Direct Deposit Payable'), ('Dues & subscription', 'Expenses', 'Dues & subscriptions'), ('Job Supplies', 'Cost of Goods Sold', 'Supplies & Materials - COGS'), ('Legal & Professional Services:Consulting', 'Expenses', 'Legal & Professional Fees'), ('N/P Ascentium Capital', 'Long Term Liabilities', 'Notes Payable'), ('N/P Ascentium Capital 2', 'Long Term Liabilities', 'Notes Payable'), ('N/P On Deck Capital', 'Long Term Liabilities', 'Notes Payable'), ('Office Supplies & Software', 'Expenses', 'Office/General Administrative Expenses'), ("Owner's Pay & Personal Expenses", 'Equity', "Owner's Equity"), ('Payroll Expenses:Employee Health Contributions', 'Expenses', 'Insurance'), ('Payroll Liabilities:Federal Taxes (941/944)', 'Other Current Liabilities', 'Payroll Tax Payable'), ('QuickBooks Tax Holding Account', 'Other Current Assets', 'Other Current Assets'), ('Rent & Lease', 'Expenses', 'Rent or Lease of Buildings'), ('Repairs & Maintenance', 'Expenses', 'Repair & Maintenance'), ('Taxes Paid', 'Expenses', 'Taxes Paid'), ('Utilities:Electricity', 'Expenses', 'Utilities'), ('Utilities:Internet', 'Expenses', 'Utilities'), ('Vantage LOC', 'Long Term Liabilities', 'Notes Payable'), ('citi business cc', 'Credit Card', 'Credit Card'), ('American Express Gold Card (2003) - 2', 'Credit Card Payment', ''), ('CREDIT CARD (5290) - 1', 'Credit Card Payment', ''), ('Chase 9600', 'Credit Card Payment', ''), ('citi business cc', 'Credit Card Payment', ''), ('Chase 4571', 'Credit Card Payment', ''), ('My Best Buy® Visa® Card (7115) - 3', 'Credit Card Payment', ''), ('Chase 9754', 'Credit Card Payment', '')]
    
    inflows_by_cat = classify_transactions(inflows_list, "inflows", inflow_categories, key_mapping_inflows, inflows_format)
    outflows_by_cat = classify_transactions(outflows_list, "outflows", outflow_categories, key_mapping_outflows, outflows_format)

    print(inflows_by_cat)
    print(outflows_by_cat)

    assert isinstance(inflows_by_cat, dict)
    assert isinstance(outflows_by_cat, dict)