from src.trinity.classify_transactions import classify_inflows, classify_outflows


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

def tesT_outflows_classification():
    outflows_list = outflows.split("\n")
    result = classify_outflows(outflows_list)
    print(result)
    assert 'Expenses Accounts Payable' in result
    assert 'Credit Cards and Loans' in result
    assert "Owner's Expense" in result

def test_inflows_classification():
    inflows = """Line of Credit Advance
    Client Payment - Invoice #1234
    Miscellaneous Other Income
    Line of Credit Advance
    Customer Payment - Invoice #5678
    """
    inflows_list = inflows.split("\n")
    result = classify_inflows(inflows_list)
    print(result)
    assert 'Line of Credit Advances' in result
    assert 'Other Income' in result
    assert 'AR Collected' in result