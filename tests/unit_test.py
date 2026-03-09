from src.classify_transactions import get_classifications


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
    assert True