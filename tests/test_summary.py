from src.ai_summary import get_summary


def test_summary():
    date_strt = "2026-02-16"
    OUTPUT_XLSX = "tests/trinity/output.xlsx"
    client = "Trinity"
    summary_bytes = get_summary(client, date_strt, OUTPUT_XLSX)
    assert summary_bytes[:2] == b'PK'
