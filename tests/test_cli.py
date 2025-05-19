from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from datamorpher.__main__ import app


def test_cli_conversion(tmp_path: Path) -> None:
    runner = CliRunner()
    df = pd.DataFrame({"a": [1, 2]})
    src = tmp_path / "in.csv"
    out = tmp_path / "out.json"
    df.to_csv(src, index=False)
    result = runner.invoke(app, ["--input", str(src), "--output", str(out)])
    assert result.exit_code == 0
    assert out.exists()
