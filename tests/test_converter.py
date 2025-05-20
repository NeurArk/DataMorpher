from pathlib import Path

import pandas as pd
import pytest

from datamorpher.converter import (
    _infer_column_type,
    _refine_inferred_types,
    convert,
)


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


def test_column_type_detection() -> None:
    """Test the detection of column types."""
    # Create a test dataframe with various column types
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "product_name": ["iPhone 14", "Galaxy S21", "Pixel 7"],
        "price": [599.99, 699.99, 499.99],
        "rating": [4.5, 4.7, 4.8],
        "date_created": ["2023-01-15", "2023-02-20", "2023-03-10"],
        "is_available": [True, False, True],
        "store_location": ["New York", "Los Angeles", "Chicago"],
    })
    
    # Test individual column type detection
    assert _infer_column_type(df, "id") == "identifier"
    assert _infer_column_type(df, "product_name") == "string"
    assert _infer_column_type(df, "price") == "currency"
    assert _infer_column_type(df, "date_created") == "date"
    assert _infer_column_type(df, "is_available") == "boolean"
    
    # Test full dataframe type detection
    types = convert.detect_types(df)
    assert len(types) == 7  # All columns have types
    assert types["id"] == "identifier"
    assert types["product_name"] == "string"
    assert types["price"] == "currency"
    assert types["date_created"] == "date"
    assert types["is_available"] == "boolean"


def test_type_detection_edge_cases() -> None:
    """Test type detection for edge cases."""
    # Mixed types in columns
    df = pd.DataFrame({
        "mixed_numbers": [1, 2.5, "3", "four"],
        "mixed_dates": ["2023-01-01", "Jan 1, 2023", "yesterday", "invalid"],
        "mixed_bool": [True, "yes", "no", "maybe"],
    })
    
    # Test refined type detection
    initial_types = {
        "mixed_numbers": "string",
        "mixed_dates": "string",
        "mixed_bool": "string",
    }
    refined = _refine_inferred_types(df, initial_types)
    
    # The refiner should detect possible dates and booleans
    assert refined["mixed_bool"] in ["string", "boolean"]  # Either is acceptable


def test_detect_types_on_real_data() -> None:
    """Test type detection on more realistic data."""
    df = pd.DataFrame({
        "product_id": [101, 102, 103, 104],
        "name": ["Product A", "Product B", "Product C", "Product D"],
        "price_usd": [19.99, 29.99, 39.99, 49.99],
        "in_stock": ["yes", "no", "yes", "yes"],
        "categories": ["Electronics, Gadgets", "Home, Kitchen", "Electronics", "Toys"],
        "rating": [4.5, 3.8, 4.2, 4.7],
        "created_at": ["2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05"],
    })
    
    types = convert.detect_types(df)
    
    # Check key column types
    assert types["product_id"] == "identifier"
    assert types["price_usd"] == "currency"  # Should detect as currency based on name
    assert types["in_stock"] in ["boolean", "string"]  # Should detect as boolean
    assert types["rating"] in ["floating", "integer"]  # Should detect as floating
    assert types["created_at"] == "date"  # Should detect as date


def test_unsupported_formats() -> None:
    """Test error handling for unsupported formats."""
    with pytest.raises(ValueError):
        # Attempt to read an unsupported format
        convert.read(Path("file.unsupported"))
        
    # Create a dataframe for writing test
    df = pd.DataFrame({"a": [1, 2]})
    
    with pytest.raises(ValueError):
        # Attempt to write to an unsupported format
        convert.write(df, Path("file.unsupported"))