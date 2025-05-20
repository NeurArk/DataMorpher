"""Data cleaning utilities."""

from __future__ import annotations

from typing import Dict, Sequence

import pandas as pd
from pandas.api.types import infer_dtype


def clean_data(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, object]]:
    """Clean a DataFrame and report actions taken."""
    info: Dict[str, object] = {
        "duplicates": 0,
        "imputed": {},
        "invalid": {},
        "transformations": {},
    }

    before = len(df)
    df = df.drop_duplicates()
    info["duplicates"] = before - len(df)

    transformations: Dict[str, list[str]] = info["transformations"]

    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()

    for col in df.columns:
        dtype = infer_dtype(df[col], skipna=True)
        series = df[col]
        if dtype in {"string", "mixed"}:
            bools = _normalize_booleans(series)
            if bools.notna().sum() / max(series.notna().sum(), 1) >= 0.5:
                df[col] = bools
                dtype = "boolean"
                series = df[col]
        if dtype in {"string", "mixed"}:
            validated, invalid = _validate_numeric(
                series, col, transformations
            )
            if validated is not None:
                ratio = validated.notna().sum() / max(series.notna().sum(), 1)
                if ratio >= 0.5:
                    df[col] = validated
                    if invalid:
                        info["invalid"][col] = invalid
                    dtype = "floating"
            if dtype in {"string", "mixed"}:
                validated, invalid = _validate_dates(
                    series, column=col, transformations=transformations
                )
                if validated is not None:
                    ratio = validated.notna().sum() / max(
                        series.notna().sum(), 1
                    )
                    if ratio >= 0.5:
                        df[col] = validated
                        if invalid:
                            info["invalid"][col] = invalid
                        dtype = "date"

        if df[col].isna().any():
            if dtype in {"integer", "floating"}:
                value = df[col].median()
                df[col] = df[col].fillna(value)
                info["imputed"][col] = "median"
                transformations.setdefault(col, []).append(
                    f"NaN -> {value:.2f} (median)"
                )
            elif dtype in {"string", "categorical"}:
                mode = df[col].mode(dropna=True)
                if not mode.empty:
                    value = mode.iloc[0]
                    df[col] = df[col].fillna(value)
                    info["imputed"][col] = "mode"
                    transformations.setdefault(col, []).append(
                        f"NaN -> {value} (mode)"
                    )
            elif dtype == "boolean":
                mode = df[col].mode(dropna=True)
                if not mode.empty:
                    value = bool(mode.iloc[0])
                    df[col] = df[col].fillna(value)
                    info["imputed"][col] = "mode"
                    transformations.setdefault(col, []).append(
                        f"NaN -> {value} (mode)"
                    )
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


def _normalize_booleans(series: pd.Series) -> pd.Series:
    """Normalize various boolean representations."""
    true_values = {"true", "yes", "1", "t", "y"}
    false_values = {"false", "no", "0", "f", "n", "inactive"}

    lowercase = series.astype(str).str.lower()
    result = pd.Series(index=series.index, dtype="object")
    result[lowercase.isin(true_values)] = True
    result[lowercase.isin(false_values)] = False
    return result


def _validate_numeric(
    series: pd.Series,
    column: str | None = None,
    transformations: Dict[str, list[str]] | None = None,
) -> tuple[pd.Series | None, int]:
    """Return numeric series if convertible and count invalid entries."""
    converted = pd.to_numeric(series, errors="coerce")

    if converted.isna().any():
        as_words = series.where(converted.isna()).apply(_words_to_num)
        if transformations is not None and column is not None:
            for idx, val in as_words.dropna().items():
                transformations.setdefault(column, []).append(
                    f"{series[idx]} -> {val}"
                )
        converted.update(as_words)

    if converted.isna().any():
        mask = converted.isna()
        extracted = (
            series.where(mask)
            .where(~series.str.contains(r"[/-]", na=False))
            .str.extract(r"(\d+\.\d+|\d+)")[0]
        )
        extracted_numeric = pd.to_numeric(extracted, errors="coerce")
        if transformations is not None and column is not None:
            for idx, val in extracted_numeric.dropna().items():
                transformations.setdefault(column, []).append(
                    f"{series[idx]} -> {val}"
                )
        converted.update(extracted_numeric)

    if converted.notna().sum() == 0:
        return None, 0

    ratio = converted.notna().sum() / max(series.notna().sum(), 1)
    if ratio < 0.5:
        return None, 0

    invalid = int((converted.isna() & series.notna()).sum())
    return converted, invalid


def _validate_dates(
    series: pd.Series,
    formats: Sequence[str] | None = None,
    *,
    column: str | None = None,
    transformations: Dict[str, list[str]] | None = None,
) -> tuple[pd.Series | None, int]:
    """Return series of ISO formatted dates if convertible."""
    if formats is None:
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%m-%d-%Y",
            "%B %d %Y",
            "%d %B %Y",
        ]

    parsed = pd.Series(pd.NaT, index=series.index)
    for fmt in formats:
        parsed_try = pd.to_datetime(series, errors="coerce", format=fmt)
        parsed = parsed.fillna(parsed_try)

    if parsed.notna().sum() == 0:
        return None, 0

    ratio = parsed.notna().sum() / max(series.notna().sum(), 1)
    if ratio < 0.5:
        return None, 0

    if transformations is not None and column is not None:
        for idx in series.index:
            if pd.isna(parsed[idx]) or pd.isna(series[idx]):
                continue
            formatted = parsed[idx].strftime("%Y-%m-%d")
            if str(series[idx]) != formatted:
                transformations.setdefault(column, []).append(
                    f"{series[idx]} -> {formatted}"
                )

    invalid = int(parsed.isna().sum())
    return parsed.dt.strftime("%Y-%m-%d"), invalid
