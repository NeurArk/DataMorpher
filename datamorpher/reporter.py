"""Markdown report generation."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from tabulate import tabulate


def build_report(
    input_path: Path,
    output_path: Path,
    rows_in: int,
    rows_out: int,
    clean_info: Dict[str, object],
    types: Dict[str, str],
    duration: float,
) -> str:
    """Return a markdown report string."""
    table = tabulate(
        [[col, typ] for col, typ in types.items()],
        headers=["Column", "Detected Type"],
        tablefmt="github",
    )
    imputed = (
        "\n".join(f"- {c}: {m}" for c, m in clean_info["imputed"].items())
        or "None"
    )
    report = f"""# DataMorpher Report

## Summary
- Input: {input_path.name} ({rows_in} rows)
- Output: {output_path.name} ({rows_out} rows)
- Duplicates removed: {clean_info["duplicates"]}
- Values imputed:\n{imputed}
- Duration: {duration:.2f}s

## Column Types
{table}
"""
    return report
