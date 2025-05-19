import pandas as pd

from datamorpher.cleaner import clean_data


def test_cleaning_removes_duplicates_and_imputes() -> None:
    df = pd.DataFrame({"a": [1, 1, None], "b": ["x", "x", None]})
    cleaned, info = clean_data(df)
    assert info["duplicates"] == 1
    assert info["imputed"] == {"a": "mean", "b": "mode"}
    assert len(cleaned) == 2
    assert not cleaned.isna().any().any()


def test_numeric_and_date_validation() -> None:
    df = pd.DataFrame(
        {
            "num": ["1", "twenty-eight", "8000foo0"],
            "date": ["2020-01-01", "invalid_date", "2021-02-03"],
        }
    )
    cleaned, info = clean_data(df)

    assert info["invalid"] == {"num": 1, "date": 1}
    assert cleaned.loc[1, "num"] == 28
    assert cleaned["date"].isna().sum() == 1
