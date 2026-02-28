from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import pandas as pd


def calculate_category_totals(OUTPUT_XLSX, inflows_by_cat, outflows_by_cat):

    _, cash_balance_indexes, inflow_section_indexes, outflow_section_indexes = get_category_indexes(OUTPUT_XLSX, 
                                                                                                              inflows_by_cat, 
                                                                                                              outflows_by_cat, 
                                                                                                              sheet_name="Projections (Table)")
    wb = load_workbook(OUTPUT_XLSX, data_only=True)
    wb_2 = load_workbook(OUTPUT_XLSX, data_only=True)
    ws = wb["Projections (Table)"]
    ws_2 = wb_2["Projections (Table)"]
    # -----------------------------------------
    # Get the category totals
    # -----------------------------------------

    for section_indexes in [inflow_section_indexes, outflow_section_indexes]:

        if section_indexes is inflow_section_indexes:
            operation = 'SUM'
        else:
            operation = '-SUM'

        for i in range(len(section_indexes)):
            idx = section_indexes[i]
            if idx == section_indexes[-1]:
                break

            next_idx = section_indexes[i+1]

            row = ws[idx]
            for col in range(3, len(row)):
                col_letter = get_column_letter(col+1)
                if next_idx-2 >= idx+1:
                    row[col].value = f'={operation}({col_letter}{idx+1}:{col_letter}{next_idx-2})'
                    number_value = 0
                    for n in range(idx+1, next_idx-1):
                        summand = ws[n][col].value
                        if summand is not None and pd.isna(summand) == False:
                            if operation == 'SUM':
                                number_value += summand
                            else:
                                number_value -= summand
                    ws_2[idx][col].value = number_value
                else:
                    row[col].value = 0.0
                    ws_2[idx][col].value = 0.0


    # -----------------------------------------
    # Calculate total inflows and outflows
    # -----------------------------------------

    total_inflows_row_idx = inflow_section_indexes[-1]
    total_outflows_row_idx = outflow_section_indexes[-1]
    for col in range(3, ws.max_column):
        #print(col)
        col_letter = get_column_letter(col+1)
        # Total Inflows
        inflows_sum_String =  f'='
        number_value = 0
        for i in range(len(inflow_section_indexes)-1):
            inflows_sum_String += f'{col_letter}{inflow_section_indexes[i]}+'
            sumand = ws_2[inflow_section_indexes[i]][col].value
            if sumand is not None and pd.isna(sumand) == False:
                number_value += sumand
        inflows_sum_String = inflows_sum_String.rstrip('+')
        ws_2[total_inflows_row_idx][col].value = number_value
        row = ws[total_inflows_row_idx]
        row[col].value = inflows_sum_String

        # Total Outflows
        outflows_sum_String =  f'='
        number_value = 0
        for i in range(len(outflow_section_indexes)-1):
            outflows_sum_String += f'{col_letter}{outflow_section_indexes[i]}+'
            summand = ws_2[outflow_section_indexes[i]][col].value
            if summand is not None and pd.isna(summand) == False:
                number_value += summand
        outflows_sum_String = outflows_sum_String.rstrip('+')
        ws_2[total_outflows_row_idx][col].value = number_value
        row = ws[total_outflows_row_idx]
        row[col].value = outflows_sum_String


    # ----------------------------------------------
    # Calculate beginning and ending cash balances
    # ----------------------------------------------

    beg_cash_row_idx = cash_balance_indexes[0]
    end_cash_row_idx = cash_balance_indexes[1]
    beg_row = ws[beg_cash_row_idx]
    end_row = ws[end_cash_row_idx]
    beg_row_2 = ws_2[beg_cash_row_idx]
    end_row_2 = ws_2[end_cash_row_idx]

    # Logic is different for the values of the past than teh ones of the present:

    # We need to have the hard coded numbers done first, that way we don't run into strings when doing the calculations


    # Hard coded numbers

    # Cash balances for the past
    for col in range(6, 2, -1):
        col_letter = get_column_letter(col+1)
        next_col_letter = get_column_letter(col+2)

        # End balance is just the beg balanace from teh next column
        end_row_2[col].value = ws_2[beg_cash_row_idx][col+1].value

        # Beg balaance is end balaance - inflows - outflows (already negative)
        beg_row_2[col].value = ws_2[end_cash_row_idx][col].value - ws_2[total_outflows_row_idx][col].value - ws_2[total_inflows_row_idx][col].value

    # Cash balances for the future
    for col in range(7, ws.max_column):
        col_letter = get_column_letter(col+1)
        prev_col_letter = get_column_letter(col)

        # Beg balaance is end balaance from previous column except for the one of the present
        if col == 7:
            pass
        else:
            beg_row_2[col].value = ws_2[end_cash_row_idx][col-1].value

        # End balance is beg balance + inflows + outflows (already negative)
        end_row_2[col].value = ws_2[beg_cash_row_idx][col].value + ws_2[total_inflows_row_idx][col].value + ws_2[total_outflows_row_idx][col].value


    # Excel formulas

    # Cash balances for the past
    for col in range(3, 7):
        col_letter = get_column_letter(col+1)
        next_col_letter = get_column_letter(col+2)

        # End balance is just the beg balanace from teh next column
        end_row[col].value = f'={next_col_letter}{beg_cash_row_idx}'

        # Beg balaance is end balaance - inflows - outflows (already negative)
        beg_row[col].value = f'={col_letter}{end_cash_row_idx}-{col_letter}{total_outflows_row_idx}-{col_letter}{total_inflows_row_idx}'

    # Cash balances for the future
    for col in range(7, ws.max_column):
        col_letter = get_column_letter(col+1)
        prev_col_letter = get_column_letter(col)

        # Beg balaance is end balaance from previous column except for the one of the present
        if col == 7:
            pass
        else:
            beg_row[col].value = f'={prev_col_letter}{end_cash_row_idx}'

        # End balance is beg balance + inflows + outflows (already negative)
        end_row[col].value = f'={col_letter}{beg_cash_row_idx}+{col_letter}{total_inflows_row_idx}+{col_letter}{total_outflows_row_idx}'


    wb.save(OUTPUT_XLSX)
    TEMP_OUTPUT_XLSX = str(OUTPUT_XLSX)[:-5] + "_temp.xlsx"
    wb_2.save(TEMP_OUTPUT_XLSX)
    return TEMP_OUTPUT_XLSX


def get_category_indexes(excel_path, inflows_by_cat, outflows_by_cat, sheet_name="", final_format=False):

    wb = load_workbook(excel_path)

    if sheet_name:
        ws = wb[sheet_name]
    else:
        ws = wb.active
    cat_indexes = {}

    if final_format:
        cat_range = range(1,3)
    else:
        cat_range = range(0,2)

    for i in range(1, ws.max_row+1):
        for j in cat_range:
            if ws[i][j].value is not None:
                cat_indexes[ws[i][j].value] = [i,j]

    cash_balance_indexes = []
    inflow_section_indexes = []
    outflow_section_indexes = []
    for key in cat_indexes:
        if key in ["Beginning Bank Balance", "Ending Bank Balance"]:
            cash_balance_indexes.append(cat_indexes[key][0])
        if key == "Total Cash Inflows" or key in inflows_by_cat:
            inflow_section_indexes.append(cat_indexes[key][0])
        if key == "Total Cash Outflows" or key in outflows_by_cat:
            outflow_section_indexes.append(cat_indexes[key][0])

    return cat_indexes, cash_balance_indexes, inflow_section_indexes, outflow_section_indexes