"""File reading, writing and type detection logic."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
from pandas.api.types import infer_dtype


class convert:
    """Namespace for conversion helpers."""

    @staticmethod
    def read(path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(path)
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path)
        if suffix == ".json":
            try:
                return pd.read_json(path, lines=True)
            except ValueError:
                return pd.read_json(path)
        raise ValueError(f"Unsupported input format: {path.suffix}")

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            df.to_csv(path, index=False)
        elif suffix in {".xlsx", ".xls"}:
            df.to_excel(path, index=False)
        elif suffix == ".json":
            df.to_json(path, orient="records", lines=True)
        else:
            raise ValueError(f"Unsupported output format: {path.suffix}")

    @staticmethod
    def detect_types(df: pd.DataFrame) -> Dict[str, str]:
        types: Dict[str, str] = {}
        for col in df.columns:
            dtype = infer_dtype(df[col], skipna=True)
            if dtype == "string" and _looks_like_date(df[col].astype(str)):
                dtype = "date"
            types[col] = dtype
        return types


def _looks_like_date(series: pd.Series) -> bool:
    sample = series.dropna().astype(str).head(10)
    return sample.str.match(r"\d{4}-\d{2}-\d{2}").all()
