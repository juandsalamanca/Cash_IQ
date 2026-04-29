import pandas as pd

def get_balance_from_cc_accounts(balance_sheet, cc_accounts):

    balance_df = pd.read_excel(balance_sheet, skiprows=4)
    cols  = balance_df.columns
    accnt_col = cols[0]
    amount_col = cols[1]
    print(accnt_col)
    print(amount_col)

    print("-"*100)
    null_balances = {}
    for accnt in cc_accounts:
        print(accnt)
        if accnt in balance_df[accnt_col].to_list():
            idx = balance_df.index[balance_df[accnt_col] == accnt]
            balance = balance_df.loc[idx, amount_col].item()
            print(balance)
            print(type(balance))
            print(int(balance))
            if (isinstance(balance, int) or isinstance(balance, float)) and int(balance) == 0:
                null_balances[accnt] = balance
    print(null_balances)
    return null_balances

def erase_null_cc_projections(null_balances, OUTPUT_XLSX):

    output_df = pd.read_excel(OUTPUT_XLSX, sheet_name="Projections (Table)")
    print(output_df.tail(10))
    for accnt in output_df["Line Item"].to_list():
        print(accnt)
        if accnt in null_balances:
            print("Found it")
            idx = output_df.index[output_df["Line Item"] == accnt]
            print(idx)
            output_df = output_df.drop(index=idx)

    print(output_df.tail(10))

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        output_df.to_excel(writer, sheet_name="Projections (Table)", index=False)


def integrate_balance(balance_sheet, cc_accounts, OUTPUT_XLSX):

    null_balances = get_balance_from_cc_accounts(balance_sheet, cc_accounts)
    erase_null_cc_projections(null_balances, OUTPUT_XLSX)