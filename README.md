# Cash IQ

A **13-week cash flow forecasting tool** built with Streamlit. Analysts upload QuickBooks-exported Excel files, select a client, and receive a professionally formatted Excel workbook with actual historical cash flows, statistical 13-week projections, credit card schedules, and an optional AI-generated Word document summary.

Live app: https://cashiq.streamlit.app/

---

## Features

- **Historical actuals** — Aggregates weekly cash flows from a General Ledger export, grouped by inflow/outflow category
- **Statistical projections** — Two projection modes per line item:
  - *Cadence-based*: detects weekly / biweekly / semimonthly / monthly / quarterly / annual patterns and schedules future events using median historical amounts
  - *Weekly-flow series*: week-of-month seasonality averages combined with a linear trend from the trailing 12 weeks (slope clamped to ±15%/week)
- **AI transaction classification** — GPT-4.1 classifies every line item into client-specific inflow and outflow categories using structured outputs
- **Credit card processing** — Reconstructs CC debt history, projects future spend, detects payment cadence, and generates a full CC payment allocation schedule (Trinity, Strivewell, Continuum)
- **AR Aging projections** — Distributes collectible AR amounts into future weeks using bucket-based collection probabilities and triangular weighting (Luna)
- **Retroactive comparison** — Reads a prior Cash IQ report, detects manually adjusted cells (by fill color), and carries those overrides one week forward into the new report
- **AI narrative summary** — Optional GPT-5 Word document with a paragraph summary and 4–5 named insight cards (observations + recommendations), generated from the output CSV
- **Cash floor monitoring** — Conditional formatting turns the ending balance row red when it falls below a configurable cash floor (stored in Google Sheets)

---

## Project Structure

```
Cash_IQ/
├── main.py                        # Streamlit UI entry point
├── client_data.json               # Per-client category definitions and key mappings
├── requirements.txt
└── src/
    ├── general_main_process.py    # Central pipeline dispatcher
    ├── general_preprocessing.py   # Shared COA + GL loaders
    ├── general_postprocessing.py  # Inflow/outflow tables, Excel writer
    ├── projections.py             # Statistical projection engine
    ├── classify_transactions.py   # GPT-4.1 AI categorisation
    ├── retroactive_comparison.py  # Carry forward manual overrides
    ├── styling.py                 # openpyxl workbook formatting
    ├── ai_summary.py              # GPT-5 Word document summary
    ├── trinity/                   # Trinity (Grace Global) client module
    ├── parisi/                    # Parisi Speed School client module
    ├── luna/                      # Luna client module (includes AR Aging)
    ├── strivewell/                # Strivewell client module
    └── continuum/                 # Continuum client module
```

Each client module follows the same internal structure:

```
<client>/
├── preprocessing.py   # Week-window calculation, date math
├── cash.py            # Bank transaction extraction, actuals pivot
├── postprocessing.py  # Combine actuals + projections into final tables
└── credit_card.py     # (Trinity only; reused by Strivewell and Continuum)
```

---

## Supported Clients

| Client | COA Required | AR Aging | Credit Cards | Notes |
|---|:---:|:---:|:---:|---|
| **Trinity** | ✅ | ❌ | ✅ | Branded as *Grace Global 13-Week Cashflow* |
| **Strivewell** | ✅ | ❌ | ✅ | Same pipeline as Trinity |
| **Continuum** | ✅ | ❌ | ✅ | Like Trinity; omits per-CC transaction history sheet |
| **Luna** | ✅ | ✅ optional | ❌ | Adds AR Aging collection projections |
| **Parisi** | ❌ | ❌ | ❌ | Self-contained; bank accounts detected by GL code range |
| **SupafitGrow / Gamechanger** | ✅ | ❌ | ✅ | Routed through Trinity pipeline |

---

## Setup

### Prerequisites

- Python 3.12+
- A virtual environment (the repo ships with `vali/`)

### Install dependencies

```bash
python -m venv vali
source vali/bin/activate        # Windows: vali\Scripts\activate
pip install -r requirements.txt
```

### Environment variables

Create a `.env` file in the `Cash_IQ/` directory (or configure Streamlit secrets for deployment):

```env
OPENAI_API_KEY=sk-...
GSHEET_URL=https://docs.google.com/spreadsheets/...
```

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | Required for AI classification and the AI summary feature |
| `GSHEET_URL` | Google Sheets endpoint used to persist per-client cash floor values |

---

## Running the App

```bash
source vali/bin/activate
cd Cash_IQ
streamlit run main.py
```

Open http://localhost:8501 in your browser.

---

## Usage

1. Select a **client** from the sidebar dropdown
2. Upload the **Chart of Accounts** Excel file exported from QuickBooks
3. Upload the **General Ledger** Excel file exported from QuickBooks
4. *(Luna only)* Optionally upload an **AR Aging** report
5. *(Optional)* Upload a **prior Cash IQ report** to carry forward manual adjustments
6. Select the **projection start date**
7. Click **Generate** — download the formatted `.xlsx` workbook and, optionally, the AI Word summary

### Output files

| File | Contents |
|---|---|
| `{Client}_Cash_IQ_YYYY-MM-DD.xlsx` | Summary, Projections Table, Inflow/Outflow Detail, CC sheets, AR sheets |
| `{Client}_Cash_IQ_Summary_YYYY-MM-DD.docx` | AI-generated narrative summary *(optional)* |

---

## Configuration

`client_data.json` defines per-client AI classification behaviour:

```json
{
  "trinity": {
    "inflow_categories": "...",
    "outflow_categories": "...",
    "key_mapping_inflows": { "ar_collected": "AR Collected", ... },
    "key_mapping_outflows": { "expenses": "Expenses & Accounts Payable", ... }
  }
}
```

---

## Running Tests

```bash
cd Cash_IQ
pytest
```

Test suites live in `tests/` with per-client subfolders (`tests/trinity/`, `tests/luna/`, etc.).

---

## Key Dependencies

| Package | Purpose |
|---|---|
| `streamlit` | Web UI |
| `pandas` | Data processing |
| `openpyxl` | Excel read/write and formatting |
| `openai` | GPT-4.1 classification and GPT-5 summary |
| `python-docx` | Word document generation |
| `Pillow` | Cover page image compositing |
| `requests` | Google Sheets HTTP integration |
| `python-dotenv` | Local `.env` secret loading |
| `pytest` | Unit testing |
