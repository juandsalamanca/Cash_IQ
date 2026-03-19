import pandas as pd
from datetime import date, datetime
from openpyxl import load_workbook
from openpyxl.styles.numbers import is_date_format
from src.general_postprocessing import get_category_indexes

def unify_category_columns(df, cat_col_idx):
    for i in range(len(df)):
        if pd.isna(df.iloc[i, cat_col_idx]):

            for j in range(cat_col_idx):
                if not pd.isna(df.iloc[i, j]):
                    df.iloc[i, cat_col_idx] = df.iloc[i, j]
                    break
    return df


def get_mismatches(simplified_previous_cashiq_df, simplified_new_cashiq):

    mismatches = []

    for i in range(3, len(simplified_new_cashiq)):

        # Get teh category we are standing in
        category = simplified_new_cashiq.iloc[i, 0]
        if pd.isna(category):
            continue
        else:
            # Check if same category is present and save the index
            for x in range(3, len(simplified_previous_cashiq_df)):
                if simplified_previous_cashiq_df.iloc[x, 0] == category:
                    break
            else:
                continue
        # we start tje loop with 1 to avoid the category column
        for j in range(1, len(simplified_new_cashiq.columns)-1):
            # For the same cateogries we compare values with one shoft to the right on the old one ot match the dates
            #print("Indexes: ", i, j, " and ", x, j+1)
            new_value = simplified_new_cashiq.iloc[i, j]
            #print("New value: ", new_value)
            old_value = simplified_previous_cashiq_df.iloc[x, j+1]
            #print("Old value: ", old_value)
            if new_value != old_value:

                if (pd.isna(new_value) or new_value == '' or new_value == '\xa0') and (pd.isna(old_value) or old_value == '' or old_value == '\xa0'):
                    continue

                mismatch = {}
                mismatch["new_value"] = new_value
                mismatch["old_value"] = old_value
                mismatch["category"] = category
                mismatch["week"] = simplified_new_cashiq.iloc[0, j]
                mismatches.append(mismatch)

    return mismatches


def learn_from_previous_cashiq(previous_cashiq_path, OUTPUT_XLSX, inflows_by_cat, outflows_by_cat):

    old_wb = load_workbook(previous_cashiq_path)

    ws = old_wb.active
    allowed_colors = ['FFF2977E', '00000000', 'FF53C9B8', 'FFA3A5D0', 'FFBFBFBF']
    adjustments = []
    group = None
    group_indexes = {}
    for i in range(1, ws.max_row+1):
        for j in range(0, ws.max_column):
            # Group needs to eb saved beforehand because they are in diffferent rows than the new accounts in yellow
            if j == 1 and ws[i][j].value is not None:
                group = ws[i][j].value
                group_indexes[group] = i
            fill_color = ws[i][j].fill.start_color.rgb
            if fill_color not in allowed_colors:
                category = ws[i][2].value
                adjustments.append({"row": i, "column": j, "category": category, "group": group, "value": ws[i][j].value})

    new_accounts = {}
    new_wb = load_workbook(OUTPUT_XLSX)
    new_ws = new_wb["Projections (Table)"]
    for adjustment in adjustments:
        category = adjustment["category"]
        group = adjustment["group"]
        row = adjustment["row"]
        col = adjustment["column"]
        val = adjustment["value"]
        if val == category:
            if group == "Line of Credit Advances":
                group = "Line of Credit Advances and Loan"
            new_accounts[category] = {"row": row, "group": group}
            print(f"New account detected: {category} in group {group}")
        for i in range(1, ws.max_row+1):
            if new_ws[i][2].value == category:
                # If the new account is already present in the new cashiq we delete it from the new account so we don't add it again at the end of the group
                if category in new_accounts:
                    del new_accounts[category]
                # Substract 1 from the column number to account for the one week shift into the future for the new CashIQ
                new_ws[i][col-1].value = val

    # Because the old and new cashiq can have different row number due to different accounts we need to keep track of the index of the groups
    # We've kept track of the groups to which each new account belongs, now we insert them one row above the row of the next group
    # The way they are added at the end of the correct group

    print("-"*100)
    print("New accounts:")
    print(new_accounts)
    new_cash_iq_group_indexes = get_category_indexes(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat, sheet_name="Projections (Table)")[0]
    group_list = list(new_cash_iq_group_indexes.keys())

    added_rows = 0
    try:
        for account in new_accounts:
            row = new_accounts[account]["row"]
            group = new_accounts[account]["group"]
            next_group = group_list[group_list.index(group)+1]
            next_group_indexes = new_cash_iq_group_indexes[next_group]
            new_ws.insert_rows(next_group_indexes[0]+added_rows)
            for j in range(len(new_ws[next_group_indexes[0]+added_rows])):

                # If the value is the new account name we leave column number 'j' the same, no timeshift here
                if j == 2:
                    new_ws[next_group_indexes[0]+added_rows-1][j].value = ws[row][j].value
                elif j < 7 and j > 2:
                    new_ws[next_group_indexes[0]+added_rows-1][j].value = 0
                elif j > 7:
                    new_ws[next_group_indexes[0]+added_rows-1][j-1].value = ws[row][j].value

            # In the last week of the new report we set it to zero since there cannot be a value there in the old report
            new_ws[next_group_indexes[0]+added_rows-1][j].value = 0
            added_rows += 1
    except Exception as e:
        error_message = f"""Error while adding new accounts from previous Cash IQ: {str(e)}.
        Please check the format of the previous Cash IQ and ensure it matches the expected structure:
        Inflows: 'Line of Credit Advances and Loan', 'Other Income', 'AR Collected'
        Outflows: 'Expenses & Accounts Payable', 'Credit Cards And Loans', 'Owner's Expense'
        """
        raise ValueError(error_message)

    new_wb.save(OUTPUT_XLSX)
    old_wb.close()
    new_wb.close()

def skip_rows_until_week_dates(excel_path, sheet_name=0):

    wb = load_workbook(excel_path)
    ws = wb[sheet_name] if isinstance(sheet_name, str) else wb.worksheets[sheet_name]

    def _is_date_cell(cell):
        value = cell.value
        if value is None:
            return False
        if isinstance(value, (datetime, date)):
            return True
        if isinstance(value, (int, float)) and is_date_format(cell.number_format):
            return True
        return False

    first_week_row = None
    for row_idx, row_cells in enumerate(ws.iter_rows(), start=1):
        # Crop the row to the point where week dates should start (4th column onward).
        cropped_cells = list(row_cells[3:])
        if not cropped_cells:
            continue

        non_empty_cells = [cell for cell in cropped_cells if cell.value is not None]
        if not non_empty_cells:
            continue

        if all(_is_date_cell(cell) for cell in non_empty_cells):
            first_week_row = row_idx
            break

    if first_week_row is not None and first_week_row > 1:
        ws.delete_rows(1, first_week_row - 1)

    wb.save(excel_path)
    wb.close()


def preprocess_df_for_comparison(excel_path, sheet_name=0):

    wb = load_workbook(excel_path)
    ws = wb[sheet_name] if isinstance(sheet_name, str) else wb.worksheets[sheet_name]

    # Drop all columns where every cell value is empty to keep structure compact.
    for col_idx in range(ws.max_column, 0, -1):
        is_empty_column = True
        for row_idx in range(1, ws.max_row + 1):
            if ws.cell(row=row_idx, column=col_idx).value is not None:
                is_empty_column = False
                break
        if is_empty_column:
            ws.delete_cols(col_idx, 1)

    wb.save(excel_path)
    wb.close()

    #skip_rows_until_week_dates(excel_path, sheet_name=sheet_name)


def compare_reports(previous_cashiq_path, TEMP_OUTPUT_XLSX, OUTPUT_XLSX, inflows_by_cat, outflows_by_cat):

    # Detect old or new format for both reports without rewriting source workbooks.
    preprocess_df_for_comparison(previous_cashiq_path)

    previous_cashiq_df = pd.read_excel(previous_cashiq_path)
    new_cashiq = pd.read_excel(TEMP_OUTPUT_XLSX, sheet_name="Projections (Table)")

    simplified_previous_cashiq_df = unify_category_columns(previous_cashiq_df, 2).drop(columns=previous_cashiq_df.columns[:2].to_list())
    simplified_new_cashiq = unify_category_columns(new_cashiq, 2).drop(columns=new_cashiq.columns[:2].to_list())
    mismatches = get_mismatches(simplified_previous_cashiq_df, simplified_new_cashiq)
    mismatch_df = pd.json_normalize(mismatches)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="new") as writer:
        mismatch_df.to_excel(writer, sheet_name="Mismatches", index=False)

    learn_from_previous_cashiq(previous_cashiq_path, OUTPUT_XLSX, inflows_by_cat, outflows_by_cat)