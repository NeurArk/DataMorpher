"""Data cleaning utilities."""

from __future__ import annotations

from typing import Dict

import pandas as pd
from pandas.api.types import infer_dtype


def clean_data(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, object]]:
    """Clean a DataFrame and report actions taken."""
    info: Dict[str, object] = {"duplicates": 0, "imputed": {}, "invalid": {}}

    before = len(df)
    df = df.drop_duplicates()
    info["duplicates"] = before - len(df)

    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()

    for col in df.columns:
        dtype = infer_dtype(df[col], skipna=True)
        series = df[col]
        if dtype in {"string", "mixed"}:
            validated, invalid = _validate_numeric(series)
            if validated is not None:
                df[col] = validated
                if invalid:
                    info["invalid"][col] = invalid
                dtype = "floating"
            else:
                validated, invalid = _validate_dates(series)
                if validated is not None:
                    df[col] = validated
                    if invalid:
                        info["invalid"][col] = invalid
                    dtype = "date"

        if df[col].isna().any():
            if dtype in {"integer", "floating"}:
                df[col] = df[col].fillna(df[col].mean())
                info["imputed"][col] = "mean"
            elif dtype in {"string", "categorical", "boolean"}:
                mode = df[col].mode(dropna=True)
                if not mode.empty:
                    df[col] = df[col].fillna(mode.iloc[0])
                    info["imputed"][col] = "mode"
    return df, info


_NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}


def _words_to_num(text: str | None) -> float | None:
    """Convert simple number words to a float."""
    if not isinstance(text, str):
        return None
    text = text.lower().replace("-", " ")
    parts = text.split()
    if not parts:
        return None
    value = 0
    for part in parts:
        if part not in _NUMBER_WORDS:
            return None
        value += _NUMBER_WORDS[part]
    return float(value)


def _validate_numeric(series: pd.Series) -> tuple[pd.Series | None, int]:
    """Return numeric series if convertible and count invalid entries."""
    converted = pd.to_numeric(series, errors="coerce")
    if converted.isna().any():
        as_words = series.where(converted.isna()).apply(_words_to_num)
        converted.update(as_words)
    if converted.notna().sum() == 0:
        return None, 0
    invalid = int((converted.isna() & series.notna()).sum())
    return converted, invalid


def _validate_dates(series: pd.Series) -> tuple[pd.Series | None, int]:
    """Return series of ISO formatted dates if convertible."""
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.notna().sum() == 0:
        return None, 0
    invalid = int(parsed.isna().sum())
    return parsed.dt.strftime("%Y-%m-%d"), invalid
