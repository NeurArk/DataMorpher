import pandas as pd

from datamorpher.cleaner import clean_data


def test_cleaning_removes_duplicates_and_imputes() -> None:
    df = pd.DataFrame({"a": [1, 1, None], "b": ["x", "x", None]})
    cleaned, info = clean_data(df)
    assert info["duplicates"] == 1
    assert info["imputed"] == {"a": "median", "b": "mode"}
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

    assert info["invalid"] == {"date": 1}
    assert cleaned.loc[1, "num"] == 28
    assert cleaned.loc[2, "num"] == 8000
    assert cleaned["date"].isna().sum() == 1


def test_date_format_variants() -> None:
    df = pd.DataFrame(
        {
            "date": ["2020-01-02", "03/04/2021", "05/06/2022", "2020/07/08"],
        }
    )
    cleaned, _ = clean_data(df)
    expected = ["2020-01-02", "2021-04-03", "2022-06-05", "2020-07-08"]
    assert list(cleaned["date"]) == expected
