import pandas as pd
import numpy as np
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from src.projections import build_projections_table

def get_combined_bank(proj_bank, bank_actual_pivot, actual_week_starts, proj_week_starts, all_week_starts, cc_payment_alloc):
    # Add CC payment allocation rows to bank cash projections
    if len(cc_payment_alloc):
        # ensure columns match
        cc_payment_alloc = cc_payment_alloc.reindex(columns=proj_week_starts, fill_value=0.0)
        # append to proj_bank (cash impacts)
        for idx in cc_payment_alloc.index:
            if idx not in proj_bank.index:
                proj_bank.loc[idx] = 0.0
            proj_bank.loc[idx, proj_week_starts] += cc_payment_alloc.loc[idx, proj_week_starts].values

    # =========================
    # COMBINE ACTUALS + PROJECTIONS FOR BANK CASH
    # =========================
    combined_bank = pd.concat(
        [
            bank_actual_pivot[actual_week_starts],
            proj_bank[proj_week_starts]
        ],
        axis=1
    ).fillna(0.0)

    combined_full = combined_bank.reindex(columns=all_week_starts, fill_value=0.0)
    return combined_full

def collapse_other(df, keep_index, other_name, index_names):
    keep = df.loc[keep_index].copy() if len(keep_index) else df.iloc[0:0].copy()
    other = df.drop(index=keep_index, errors="ignore").copy()
    if len(other):
        other_row = other.sum(axis=0)
        other_idx = pd.MultiIndex.from_tuples([(other_name, "Other", "")], names=index_names)
        other_df = pd.DataFrame([other_row.values], index=other_idx, columns=df.columns)
        keep = pd.concat([keep, other_df], axis=0)
    return keep

def build_inflows_outflows(combined_full, actual_week_starts, all_week_starts, TOP_N_INFLOW_LINES, TOP_N_OUTFLOW_LINES, idx_names):
    # =========================
    # BUILD INFLOWS/OUTFLOWS PRESENTATION (NO FLAT)
    # =========================
    trailing_actual = combined_full[actual_week_starts].copy()
    inflow_mask  = trailing_actual.sum(axis=1) > 0
    outflow_mask = trailing_actual.sum(axis=1) < 0

    # rank lines by trailing magnitude
    top_inflows = (
        combined_full.loc[inflow_mask]
        .assign(trailing_inflow=lambda df: trailing_actual.loc[df.index].clip(lower=0).sum(axis=1))
        .sort_values("trailing_inflow", ascending=False)
        .head(TOP_N_INFLOW_LINES)
        .index
    )

    top_outflows = (
        combined_full.loc[outflow_mask]
        .assign(trailing_outflow=lambda df: (-trailing_actual.loc[df.index].clip(upper=0)).sum(axis=1))
        .sort_values("trailing_outflow", ascending=False)
        .head(TOP_N_OUTFLOW_LINES)
        .index
    )

    inflows_tbl = combined_full.loc[inflow_mask, all_week_starts].copy()
    outflows_tbl = combined_full.loc[outflow_mask, all_week_starts].copy()

    inflows_tbl  = collapse_other(inflows_tbl,  top_inflows,  "Other Inflows",  idx_names)
    outflows_tbl = collapse_other(outflows_tbl, top_outflows, "Other Outflows", idx_names)

    # Presentation: inflows positive; outflows positive
    inflows_present  = inflows_tbl.copy()
    outflows_present = outflows_tbl.copy().abs()

    total_inflows  = inflows_present.sum(axis=0)
    total_outflows = outflows_present.sum(axis=0)

    return inflows_present, outflows_present, total_inflows, total_outflows

def get_cash_balance(total_inflows, total_outflows, beginning_cash_balance, all_week_starts):
    # =========================
    # BEGIN/END CASH BALANCE
    # =========================
    beg_bal_series = pd.Series(index=all_week_starts, dtype=float)
    end_bal_series = pd.Series(index=all_week_starts, dtype=float)

    running_begin = beginning_cash_balance
    for w in all_week_starts:
        beg_bal_series[w] = running_begin
        running_end = running_begin + float(total_inflows[w]) - float(total_outflows[w])
        end_bal_series[w] = running_end
        running_begin = running_end

    return beg_bal_series, end_bal_series

    

def get_cc_output_sheets(cc_spend_cat_pivot_top, cc_spend_proj_cat, cc_payment_alloc, all_week_starts, proj_week_starts):
    # =========================
    # CC OUTPUT SHEETS:
    #   - CC Spend Transactions (all CC spend rows)
    #   - CC Spend Weekly (by category)
    #   - CC Spend Projected (by category)
    #   - CC Payment Schedule
    #   - CC Payment Allocation (cash impact, by category)
    # =========================
    # Weekly CC spend actual (by category) for display (last actual + projected horizon)
    cc_spend_actual_display = cc_spend_cat_pivot_top.reindex(columns=all_week_starts, fill_value=0.0)
    for w in cc_spend_actual_display.columns:
        if w not in cc_spend_cat_pivot_top.columns:
            cc_spend_actual_display[w] = 0.0
    cc_spend_actual_display = cc_spend_actual_display[all_week_starts]

    cc_spend_proj_display = cc_spend_proj_cat.reindex(columns=proj_week_starts, fill_value=0.0)

    cc_payment_alloc_present = cc_payment_alloc.abs() if len(cc_payment_alloc) else pd.DataFrame(columns=proj_week_starts)

    return cc_spend_proj_display, cc_spend_actual_display, cc_payment_alloc_present



def write_output_excel(all_week_starts, inflows_by_cat, outflows_by_cat, inflows_present, outflows_present, total_inflows, total_outflows, cc_spend_proj_display, cc_spend_actual_display, cc_payment_alloc_present, cc_spend_txn, cc_payment_schedule, beg_bal_series, end_bal_series, PROJ_WEEK1_START, OUTPUT_XLSX, week1_cash_balance=0.0):
    # =========================
    # WRITE OUTPUT EXCEL
    # =========================
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        # Summary
        summary = pd.DataFrame(
            {
                "Week Start": all_week_starts,
                "Beginning Bank Balance": [beg_bal_series[w] for w in all_week_starts],
                "Total Cash Inflows": [total_inflows[w] for w in all_week_starts],
                "Total Cash Outflows": [total_outflows[w] for w in all_week_starts],
                "Ending Bank Balance": [end_bal_series[w] for w in all_week_starts],
            }
        )
        summary.to_excel(writer, sheet_name="Summary", index=False)

        # Cash details
        inflows_present.reset_index().to_excel(writer, sheet_name="Cash Inflows (Detail)", index=False)
        outflows_present.reset_index().to_excel(writer, sheet_name="Cash Outflows (Detail)", index=False)

        # Credit card sheets
        cc_spend_txn.sort_values(["account_name","date"]).to_excel(writer, sheet_name="CC Spend - Transactions", index=False)
        cc_spend_actual_display.reset_index().to_excel(writer, sheet_name="CC Spend - Weekly (Hist)", index=False)
        cc_spend_proj_display.reset_index().to_excel(writer, sheet_name="CC Spend - Weekly (Proj)", index=False)
        cc_payment_schedule.to_excel(writer, sheet_name="CC Payments - Schedule", index=False)
        cc_payment_alloc_present.reset_index().to_excel(writer, sheet_name="Cash - CC Pay Allocation", index=False)

        proj_sheet, inflow_section_indexes, outflow_section_indexes, cash_balance_indexes = build_projections_table(all_week_starts, inflows_by_cat, outflows_by_cat, beg_bal_series, end_bal_series, total_inflows, total_outflows, inflows_present, outflows_present, week1_cash_balance)

        proj_sheet.to_excel(writer, sheet_name="Projections (Table)", index=False)

        print(f"Saved: {OUTPUT_XLSX}")
        print(f"Projection Week 1 starts: {PROJ_WEEK1_START.date()} (Monday)")

    return inflow_section_indexes, outflow_section_indexes, cash_balance_indexes


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