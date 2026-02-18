import pandas as pd
from openpyxl import load_workbook

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
            new_value = simplified_new_cashiq.iloc[i, j]
            old_value = simplified_previous_cashiq_df.iloc[x, j+1]
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


def learn_from_previopus_cashiq(previous_cashiq_path, OUTPUT_XLSX):

    old_wb = load_workbook(previous_cashiq_path)

    sheet_name = "Projections"
    ws = old_wb[sheet_name]
    allowed_colors = ['FFF2977E', '00000000', 'FF53C9B8', 'FFFFFF00', 'FFA3A5D0', 'FFBFBFBF']
    adjustments = []
    for i in range(1, ws.max_row+1):
        for j in range(0, ws.max_column):
            fill_color = ws[i][j].fill.start_color.rgb
            if fill_color not in allowed_colors:
                print(f"Found adjustment with color {fill_color}")
                category = ws[i][3].value
                adjustments.append({"row": i, "column": j, "category": category, "value": ws[i][j].value})
                print(adjustments)

    new_wb = load_workbook(OUTPUT_XLSX)
    new_ws = new_wb["Projections (Table)"]
    for adjustment in adjustments:
        category = adjustment["category"]
        col = adjustment["column"]
        val = adjustment["value"]
        for i in range(1, new_ws.max_row+1):
            if new_ws[i][2].value == category:
                new_ws[i][col-2].value = val
        
    new_wb.save(OUTPUT_XLSX)


def compare_reports(previous_cashiq_path, TEMP_OUTPUT_XLSX, OUTPUT_XLSX):

    previous_cashiq_df = pd.read_excel(previous_cashiq_path)[4:].reset_index(drop=True)
    new_cashiq = pd.read_excel(TEMP_OUTPUT_XLSX, sheet_name="Projections (Table)")

    simplified_previous_cashiq_df = unify_category_columns(previous_cashiq_df, 3).drop(columns=["Unnamed: 0", "13 Week Cash Flow Forcast", "Unnamed: 2"])
    simplified_new_cashiq = unify_category_columns(new_cashiq, 2).drop(columns=["Unnamed: 0", "Unnamed: 1"])
    mismatches = get_mismatches(simplified_previous_cashiq_df, simplified_new_cashiq)
    mismatch_df = pd.json_normalize(mismatches)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="new") as writer:
        mismatch_df.to_excel(writer, sheet_name="Mismatches", index=False)

    learn_from_previopus_cashiq(previous_cashiq_path, OUTPUT_XLSX)