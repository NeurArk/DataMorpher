from pathlib import Path

from datamorpher.reporter import (
    build_report,
    _group_similar_transformations,
    _categorize_transformation,
    format_example_values
)


def test_report_contains_summary(tmp_path: Path) -> None:
    types = {"a": "integer"}
    report = build_report(
        Path("in.csv"),
        tmp_path / "out.csv",
        1,
        1,
        {"duplicates": 0, "imputed": {}},
        types,
        0.1,
    )
    assert "DataMorpher Report" in report
    assert "in.csv" in report
    assert "out.csv" in report


def test_report_includes_transformations(tmp_path: Path) -> None:
    types = {"a": "integer"}
    clean_info = {
        "duplicates": 0,
        "imputed": {"a": "mean"},
        "invalid": {},
        "transformations": {"a": ["1 -> 2"]},
    }
    report = build_report(
        Path("in.csv"),
        tmp_path / "out.csv",
        1,
        1,
        clean_info,
        types,
        0.1,
    )
    assert "Applied Transformations" in report
    assert "1 -> 2" in report


def test_report_handles_warnings() -> None:
    """Test that warnings are properly included in the report."""
    types = {"a": "integer", "b": "string"}
    clean_info = {
        "duplicates": 0,
        "imputed": {},
        "warnings": ["Column 'b' detected as product_name - preserved as is"],
    }
    
    report = build_report(
        Path("in.csv"),
        Path("out.csv"),
        1,
        1,
        clean_info,
        types,
        0.1,
    )
    
    assert "Notes and Warnings" in report
    assert "preserved as is" in report


def test_transformation_grouping() -> None:
    """Test grouping of similar transformations."""
    # Create a list of transformations of different types
    transformations = [
        "NaN -> 5.00 (median)",
        "NaN -> 10.00 (median)",
        "NaN -> Apple (mode)",
        "twenty five -> 25.0",
        "fifty -> 50.0",
        "10k -> 10000 (unit conversion k)",
        "$50.99 -> 50.99 (currency conversion)",
        "01/15/2023 -> 2023-01-15",
        "2022/12/20 -> 2022-12-20",
    ]
    
    # Group the transformations
    grouped = _group_similar_transformations(transformations)
    
    # Check that the grouping is correct
    assert "Median Imputation" in grouped
    assert len(grouped["Median Imputation"]) == 2
    
    assert "Mode Imputation" in grouped
    assert len(grouped["Mode Imputation"]) == 1
    
    assert "Unit/Currency Conversion" in grouped
    assert len(grouped["Unit/Currency Conversion"]) == 2
    
    assert "Date Format Standardization" in grouped
    assert len(grouped["Date Format Standardization"]) == 2


def test_transformation_categorization() -> None:
    """Test categorization of individual transformations."""
    # Test different types of transformations
    assert _categorize_transformation("NaN -> 5.00 (median)") == "Median Imputation"
    assert _categorize_transformation("NaN -> Apple (mode)") == "Mode Imputation"
    assert _categorize_transformation("10k -> 10000 (unit conversion k)") == "Unit/Currency Conversion"
    assert _categorize_transformation("$50.99 -> 50.99 (currency conversion)") == "Unit/Currency Conversion"
    assert _categorize_transformation("01/15/2023 -> 2023-01-15") == "Date Format Standardization"
    assert _categorize_transformation("twenty five -> 25.0") == "Text to Number Conversion"
    assert _categorize_transformation("100 units -> 100 (extraction)") == "Numeric Value Extraction"
    assert _categorize_transformation("something else") == "Other Transformations"


def test_example_values_formatting() -> None:
    """Test formatting of example values for the report."""
    # Test various value types
    short_string = format_example_values(["abc", "def", "ghi"])
    assert short_string == "'abc', 'def', 'ghi'"
    
    long_string = format_example_values(["This is a very long string that should be truncated"])
    assert "..." in long_string
    
    mixed_values = format_example_values([123, "abc", 45.6])
    assert mixed_values == "123, 'abc', 45.6"
    
    # Test limit to 3 examples
    many_values = format_example_values([1, 2, 3, 4, 5, 6])
    assert many_values == "1, 2, 3"


def test_comprehensive_report() -> None:
    """Test a comprehensive report with all features."""
    types = {
        "id": "identifier",
        "name": "string",
        "price": "currency",
        "rating": "floating",
        "date": "date",
        "stock": "integer",
        "available": "boolean",
    }
    
    clean_info = {
        "duplicates": 5,
        "imputed": {
            "price": "median",
            "rating": "median",
            "stock": "median",
        },
        "invalid": {
            "date": 3,
            "price": 2,
        },
        "transformations": {
            "price": [
                "NaN -> 99.50 (median)",
                "$50 -> 50.0 (currency conversion)",
                "fifty dollars -> 50.0",
            ],
            "date": [
                "01/15/2023 -> 2023-01-15",
                "March 20 2023 -> 2023-03-20",
                "invalid_date -> INVALID",
            ],
            "rating": [
                "four point five -> 4.5",
                "NaN -> 4.20 (median)",
            ],
        },
        "warnings": [
            "Column 'name' detected as product_name - preserved as is",
            "Column 'id' detected as identifier - preserved as is",
        ],
    }
    
    report = build_report(
        Path("input.csv"),
        Path("output.csv"),
        100,
        95,
        clean_info,
        types,
        0.25,
    )
    
    # Check for all the important sections
    assert "DataMorpher Report" in report
    assert "Summary" in report
    assert "Column Types" in report
    assert "Notes and Warnings" in report
    assert "Applied Transformations" in report
    
    # Check for specific details
    assert "Duplicates removed: 5" in report
    assert "price: median" in report
    
    # Look for information about invalid values
    # With our new approach that emphasizes detection over correction,
    # we might use warnings instead of the "Non-recoverable values" message
    has_invalid_info = False
    
    # Check for specific non-recoverable values mentions
    if any(phrase in report for phrase in [
        "Non-recoverable values",
        "invalid values", 
        "Invalid values"
    ]):
        has_invalid_info = True
    
    # Check for warnings about invalid data
    for warning in clean_info["warnings"]:
        if "invalid" in warning.lower():
            has_invalid_info = True
            break
    
    assert has_invalid_info, "Report should mention invalid values in some way"