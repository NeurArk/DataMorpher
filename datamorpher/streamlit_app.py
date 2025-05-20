"""Streamlit frontend for DataMorpher."""

from __future__ import annotations

import io
import sys
import time
from pathlib import Path

# Add parent directory to sys.path when running directly
if __name__ == "__main__":
    import os
    # Get the absolute path of the parent directory
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # Add to sys.path if not already there
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

import pandas as pd
import streamlit as st

# Import from datamorpher package
from datamorpher.cleaner import clean_data
from datamorpher.converter import convert
from datamorpher.reporter import build_report

st.set_page_config(page_title="DataMorpher")
st.title("DataMorpher")

uploaded = st.file_uploader(
    "Upload file",
    type=["csv", "xlsx", "xls", "json"],
    accept_multiple_files=False,
)
fmt = st.radio("Output format", ["csv", "xlsx", "json"])
clean = st.checkbox("Clean data")
want_report = st.checkbox("Generate report")
process = st.button("Convert")

if process and uploaded:
    start = time.perf_counter()
    suffix = Path(uploaded.name).suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(uploaded)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(uploaded)
    elif suffix == ".json":
        try:
            df = pd.read_json(uploaded, lines=True)
        except ValueError:
            uploaded.seek(0)
            df = pd.read_json(uploaded)
    else:
        st.error("Unsupported format")
        st.stop()

    rows_in = len(df)
    if clean:
        df, info = clean_data(df)
    else:
        info = {"duplicates": 0, "imputed": {}}

    buf = io.BytesIO()
    out_path = Path(f"output.{fmt}")
    convert.write(df, out_path)
    with open(out_path, "rb") as f:
        buf.write(f.read())
    buf.seek(0)
    duration = time.perf_counter() - start
    rows_out = len(df)

    st.download_button(
        "Download file",
        buf,
        file_name=out_path.name,
        mime="application/octet-stream",
    )

    if want_report:
        report_text = build_report(
            Path(uploaded.name),
            out_path,
            rows_in,
            rows_out,
            info,
            convert.detect_types(df),
            duration,
        )
        st.download_button(
            "Download report",
            report_text,
            file_name="report.md",
            mime="text/markdown",
        )
