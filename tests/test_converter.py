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


def test_json_reading_variants(tmp_path: Path) -> None:
    ndjson = tmp_path / "data_lines.json"
    with ndjson.open("w", encoding="utf-8") as f:
        f.write('{"a":1}\n{"a":2}\n')

    array_json = tmp_path / "data_array.json"
    array_json.write_text('[{"a":1},{"a":2}]', encoding="utf-8")

    df_lines = convert.read(ndjson)
    df_array = convert.read(array_json)

    assert len(df_lines) == 2
    assert len(df_array) == 2


def test_json_to_csv_flatten(tmp_path: Path) -> None:
    src = tmp_path / "nested.json"
    src.write_text('{"a": 1, "b": {"c": 2}}\n{"a": 3, "b": {"c": 4}}')
    df = convert.read(src)
    out = tmp_path / "out.csv"
    convert.write(df, out)
    df_out = pd.read_csv(out)
    assert "b.c" in df_out.columns
    assert df_out.loc[0, "b.c"] == 2
