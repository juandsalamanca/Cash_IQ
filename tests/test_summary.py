from src.ai_summary import get_summary
import time

def test_summary():

    # Client has to be continuum, luna or strivewell so that in the deployment runtime those tests run beofre this one.
    # That way the output file gets saved and we can use it here, given that tests run in alphabetical order.
    date_strt = "2026-03-02"
    OUTPUT_XLSX = "tests/strivewell/output.xlsx"
    client = "Strivewell"
    summary_bytes = get_summary(client, date_strt, OUTPUT_XLSX)

    time.sleep(5)
    assert summary_bytes[:2] == b'PK'
