from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import pandas as pd


def calculate_category_totals(OUTPUT_XLSX, inflow_section_indexes, outflow_section_indexes, cash_balance_indexes):

    header_rows = 1
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

            row = ws[idx+header_rows]
            for col in range(3, len(row)):
                col_letter = get_column_letter(col+1)
                if next_idx-2 >= idx+1:
                    row[col].value = f'={operation}({col_letter}{idx+1+header_rows}:{col_letter}{next_idx-2+header_rows})'
                    number_value = 0
                    for n in range(idx+header_rows, next_idx-1+header_rows):
                        summand = ws[n+header_rows][col].value
                        if summand is not None and pd.isna(summand) == False:
                            if operation == 'SUM':
                                number_value += summand
                            else:
                                number_value -= summand
                    ws_2[idx+header_rows][col].value = number_value
                else:
                    row[col].value = 0.0
                    ws_2[idx+header_rows][col].value = 0.0


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
            inflows_sum_String += f'{col_letter}{inflow_section_indexes[i]+header_rows}+'
            sumand = ws_2[inflow_section_indexes[i]+header_rows][col].value
            if sumand is not None and pd.isna(sumand) == False:
                number_value += sumand
        inflows_sum_String = inflows_sum_String.rstrip('+')
        ws_2[total_inflows_row_idx+header_rows][col].value = number_value
        row = ws[total_inflows_row_idx+header_rows]
        row[col].value = inflows_sum_String

        # Total Outflows
        outflows_sum_String =  f'='
        number_value = 0
        for i in range(len(outflow_section_indexes)-1):
            outflows_sum_String += f'{col_letter}{outflow_section_indexes[i]+header_rows}+'
            summand = ws_2[outflow_section_indexes[i]+header_rows][col].value
            if summand is not None and pd.isna(summand) == False:
                number_value += summand
        outflows_sum_String = outflows_sum_String.rstrip('+')
        ws_2[total_outflows_row_idx+header_rows][col].value = number_value
        row = ws[total_outflows_row_idx+header_rows]
        row[col].value = outflows_sum_String


    # ----------------------------------------------
    # Calculate beginning and ending cash balances
    # ----------------------------------------------

    beg_cash_row_idx = cash_balance_indexes[0]
    end_cash_row_idx = cash_balance_indexes[1]
    beg_row = ws[beg_cash_row_idx+header_rows]
    end_row = ws[end_cash_row_idx+header_rows]
    beg_rwo_2 = ws_2[beg_cash_row_idx+header_rows]
    end_row_2 = ws_2[end_cash_row_idx+header_rows]

    # Logic is different for the values of the past than teh ones of the present:

    # Cash balances for the past
    for col in range(3, 7):
        col_letter = get_column_letter(col+1)
        next_col_letter = get_column_letter(col+2)

        # End balance is just the beg balanace from teh next column
        end_row[col].value = f'={next_col_letter}{beg_cash_row_idx+header_rows}'
        #end_row_2[col].value = ws[beg_cash_row_idx+header_rows][col+1].value

        # Beg balaance is end balaance - inflows - outflows (already negative)
        beg_row[col].value = f'={col_letter}{end_cash_row_idx+header_rows}-{col_letter}{total_outflows_row_idx+header_rows}-{col_letter}{total_inflows_row_idx+header_rows}'
        #beg_rwo_2[col].value = ws[end_cash_row_idx+header_rows][col].value - ws[total_outflows_row_idx+header_rows][col].value - ws[total_inflows_row_idx+header_rows][col].value

    for col in range(7, ws.max_column):
        col_letter = get_column_letter(col+1)
        prev_col_letter = get_column_letter(col)

        # Beg balaance is end balaance from previous column except for the one of the present
        if col == 7:
            pass
        else:
            beg_row[col].value = f'={prev_col_letter}{end_cash_row_idx+header_rows}'
            #beg_rwo_2[col].value = ws[end_cash_row_idx+header_rows][col-1]

        # End balance is beg balance + inflows + outflows (already negative)
        end_row[col].value = f'={col_letter}{beg_cash_row_idx+header_rows}+{col_letter}{total_inflows_row_idx+header_rows}+{col_letter}{total_outflows_row_idx+header_rows}'
        #end_row_2[col].value = ws[beg_cash_row_idx+header_rows][col].value + ws[total_inflows_row_idx+header_rows][col].value + ws[total_outflows_row_idx+header_rows][col].value

    wb.save(OUTPUT_XLSX)
    TEMP_OUTPUT_XLSX = str(OUTPUT_XLSX)[:-5] + "_temp.xlsx"
    wb_2.save(TEMP_OUTPUT_XLSX)
    return TEMP_OUTPUT_XLSX