import pandas as pd

def get_balance_from_cc_accounts(balance_sheet, cc_accounts):

    balance_df = pd.read_excel(balance_sheet, skiprows=4)

    null_balances = {}
    for accnt in cc_accounts:
        if accnt in balance_df["Distribution account"]:
            idx = balance_df["Distribution account"].index(accnt)
            balance = balance_df.loc[idx, "Total"]
            if (isinstance(balance, int) or isinstance(balance,float)) and int(balance) == 0:
                null_balances[accnt] = balance

    return null_balances

def erase_null_cc_projections(null_balances, OUTPUT_XLSX):

    output_df = pd.read_excel(OUTPUT_XLSX)

    for accnt in output_df["Line Item"]:
        if accnt in null_balances:
            idx = output_df["Line Item"].index(accnt)
            output_df.drop(index=idx)

    output_df.to_excel(OUTPUT_XLSX)
