from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill

def style_projections(OUTPUT_XLSX):

    wb = load_workbook(OUTPUT_XLSX)

    # Define styles
    sheet_name = "Projections (Table)"

    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Sheet '{sheet_name}' not found")

    ws = wb[sheet_name]
    # Styles
    font_style = Font(name="Aptos Narrow", size=12)
    header_fill = PatternFill(
        start_color="A3A5D0",
        end_color="A3A5D0",
        fill_type="solid"
    )

    accounting_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'

    # Apply styles
    for row in ws.iter_rows():
        for cell in row:
            # Font for everything
            cell.font = font_style

            # Accounting format only for numbers
            if isinstance(cell.value, (int, float)):
                cell.number_format = accounting_format

    # Header fill
    for cell in ws[1]:
        cell.fill = header_fill

    wb.save(OUTPUT_XLSX)