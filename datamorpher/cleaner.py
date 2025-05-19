"""Data cleaning utilities."""

from __future__ import annotations

from typing import Dict

import pandas as pd
from pandas.api.types import infer_dtype


def clean_data(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, object]]:
    """Remove duplicates and impute missing values."""
    info: Dict[str, object] = {"duplicates": 0, "imputed": {}}

    before = len(df)
    df = df.drop_duplicates()
    info["duplicates"] = before - len(df)

    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    for col in df.columns:
        dtype = infer_dtype(df[col], skipna=True)
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
