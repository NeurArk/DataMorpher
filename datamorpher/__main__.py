"""Command-line interface for DataMorpher."""

import time
from pathlib import Path

import typer

from .cleaner import clean_data
from .converter import convert
from .reporter import build_report

app = typer.Typer(add_completion=False)


@app.command()
def main(
    input: Path = typer.Option(
        ..., exists=True, readable=True, help="Input file"
    ),
    output: Path = typer.Option(..., help="Output file"),
    clean: bool = typer.Option(False, help="Enable data cleaning"),
    force: bool = typer.Option(False, help="Overwrite existing output"),
    report: Path | None = typer.Option(
        None, help="Path to save markdown report"
    ),
):
    """Convert data files with optional cleaning and reporting."""
    if output.exists() and not force:
        typer.echo(f"Error: {output} exists. Use --force to overwrite.")
        raise typer.Exit(code=1)

    start = time.perf_counter()
    df = convert.read(input)
    rows_in = len(df)

    if clean:
        df, clean_info = clean_data(df)
    else:
        clean_info = {"duplicates": 0, "imputed": {}}

    convert.write(df, output)
    duration = time.perf_counter() - start
    rows_out = len(df)

    if report:
        text = build_report(
            input,
            output,
            rows_in,
            rows_out,
            clean_info,
            convert.detect_types(df),
            duration,
        )
        report.write_text(text)
        typer.echo(f"Report written to {report}")

    typer.echo(f"Wrote {rows_out} rows to {output}")


if __name__ == "__main__":  # pragma: no cover
    app()
