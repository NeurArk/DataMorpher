from pathlib import Path

import pandas as pd

from datamorpher.cleaner import clean_data
from datamorpher.converter import convert
from datamorpher.reporter import build_report


def test_messy_sample_cleaning(tmp_path: Path) -> None:
    df = pd.read_csv("sample_data/messy_data.csv")
    cleaned, info = clean_data(df)

    assert cleaned["price"].dtype.kind in {"i", "f"}
    dates = cleaned["launch_date"].dropna()
    assert dates.str.match(r"\d{4}-\d{2}-\d{2}").all()
    assert cleaned["active"].dtype == bool
    assert cleaned["product_name"].dtype == object

    types = convert.detect_types(cleaned)
    report = build_report(
        Path("messy_data.csv"),
        tmp_path / "out.csv",
        len(df),
        len(cleaned),
        info,
        types,
        0.0,
    )
    assert "messy_data.csv" in report
    assert "Duplicates removed" in report

