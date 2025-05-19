import pandas as pd

from datamorpher.cleaner import clean_data


def test_cleaning_removes_duplicates_and_imputes() -> None:
    df = pd.DataFrame({"a": [1, 1, None], "b": ["x", "x", None]})
    cleaned, info = clean_data(df)
    assert info["duplicates"] == 1
    assert info["imputed"] == {"a": "mean", "b": "mode"}
    assert len(cleaned) == 2
    assert not cleaned.isna().any().any()
