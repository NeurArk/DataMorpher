from pathlib import Path

from datamorpher.reporter import build_report


def test_report_contains_summary(tmp_path: Path) -> None:
    types = {"a": "integer"}
    report = build_report(
        Path("in.csv"),
        tmp_path / "out.csv",
        1,
        1,
        {"duplicates": 0, "imputed": {}},
        types,
        0.1,
    )
    assert "DataMorpher Report" in report
    assert "in.csv" in report
    assert "out.csv" in report
