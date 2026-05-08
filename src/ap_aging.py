import pandas as pd
from datetime import datetime
from dateutil import parser


# ---------------------------------------------------
#  Use the AP AGING report to project known debt
# ---------------------------------------------------

# If there is any scheduled payment we integrate it in the projections

def get_debt_for_projections(date_strt, AP_AGING):

    date_obj = parser.parse(date_strt)
    ap_df = pd.read_excel(AP_AGING, skiprows=2).reset_index()
    
    ap_df["Due date"] = pd.to_datetime(ap_df["Due date"])
    current_debt = {"due_date": [], "amount": [], "lender_accnt": []}
    for i, due_date in enumerate(ap_df["Due date"]):

        if pd.Timedelta(days=90) + date_obj > due_date and due_date > date_obj:
            current_debt["amount"].append(ap_df.loc[i,"Amount"])
            current_debt["due_date"].append(due_date)
            current_debt["lender_accnt"].append(ap_df.loc[i,"Vendor display name"])

    return current_debt

def sum_debt_to_projections(outflows_present, current_debt, cc_spend_txn):

    cols = outflows_present.columns.to_list()
    
    for i, due_date in enumerate(current_debt["due_date"]):
        for j in range(len(cols)-1):
            col = cols[j]
            next_col = cols[j+1]
            # locate the correct column (date) to put the payment in
            if due_date > col and due_date < next_col:
                amount = current_debt["amount"][i]
                accnt = current_debt["lender_accnt"][i]

                # First look if the split account is even in the cc_spend df so we can mapp it to the correct cc
                if accnt in cc_spend_txn["split_account"]:
                    idx = cc_spend_txn["split_account"].index(accnt)
                    accnt_name = cc_spend_txn.loc["account_name", idx]
                    # If the account is already in the outflows just sum the value
                    if accnt_name in outflows_present["split_account"]:
                        outf_idx = outflows_present["split_account"].index(accnt_name)
                        outflows_present.loc[outf_idx, col] += amount
                    # If not, then create a new row full of zeros except for the scheduled payment
                    else:
                        outflows_present.loc[(accnt_name, 'Credit Card Payment', ''), :] = 0
                        outflows_present.loc[-1, col] += amount

                else:
                    outflows_present.loc[(accnt, 'Credit Card Payment', ''), :] = 0
                    outflows_present.loc[(accnt, 'Credit Card Payment', ''), col] += amount

                    

                # TODO: Look in the cc DFs to map each lender account to the correct credit card
    return outflows_present


def integrate_current_debt(date_strt, AP_AGING, outflows_present, cc_spend_txn):

    current_debt = get_debt_for_projections(date_strt, AP_AGING)
    outflows_present = sum_debt_to_projections(outflows_present, current_debt, cc_spend_txn)
    return outflows_present


if __name__ == "__main__":

    date_str = "2026-03-30"
    date_obj = parser.parse(date_str)
    ap_df = pd.read_excel("tests/continuum/Continuum Wellness Holdings_A_P Aging Detail Report.xlsx",
                          skiprows=2).reset_index()
    
    ap_df["Due date"] = pd.to_datetime(ap_df["Due date"])
    current_debt = {"due_date": [], "amount": []}
    for i, due_date in enumerate(ap_df["Due date"]):

        if pd.Timedelta(days=90) + date_obj > due_date and due_date > date_obj:
            current_debt["amount"].append(ap_df.loc[i,"Amount"])
            current_debt["due_date"].append(due_date)

    print(current_debt)
    
