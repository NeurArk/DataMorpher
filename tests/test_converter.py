from pathlib import Path

import pandas as pd

from datamorpher.converter import convert


def test_csv_to_excel(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    src = tmp_path / "input.csv"
    out = tmp_path / "out.xlsx"
    df.to_csv(src, index=False)

    df_read = convert.read(src)
    assert len(df_read) == 2
    convert.write(df_read, out)
    assert out.exists()

    df_x = pd.read_excel(out)
    assert df_x.equals(df)
