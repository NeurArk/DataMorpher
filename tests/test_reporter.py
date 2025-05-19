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


def test_report_includes_transformations(tmp_path: Path) -> None:
    types = {"a": "integer"}
    clean_info = {
        "duplicates": 0,
        "imputed": {"a": "mean"},
        "invalid": {},
        "transformations": {"a": ["1 -> 2"]},
    }
    report = build_report(
        Path("in.csv"),
        tmp_path / "out.csv",
        1,
        1,
        clean_info,
        types,
        0.1,
    )
    assert "Transformations appliquées" in report
    assert "1 -> 2" in report
