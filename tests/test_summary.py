from src.ai_summary import get_summary


def test_summary():

    # Client has to be continuum, luna or strivewell so that in the deployment runtime those tests run beofre this one
    # That way the output file gets saved and we can use it here
    date_strt = "2026-03-02"
    OUTPUT_XLSX = "tests/strivewell/output.xlsx"
    client = "Strivewell"
    summary_bytes = get_summary(client, date_strt, OUTPUT_XLSX)
    assert summary_bytes[:2] == b'PK'
