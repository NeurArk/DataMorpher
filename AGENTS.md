# AGENTS.md - AI Assistant Guidelines for DataMorpher

## Project Overview

DataMorpher is a Python utility for data conversion and cleaning operations. It provides:
- Data format conversion (CSV/JSON/Excel)
- Data cleaning and validation
- Report generation
- Command-line interface
- Streamlit web interface

## Repository Structure

```
DataMorpher/
├── datamorpher/         # Main package directory
│   ├── __init__.py     # Package initialization
│   ├── __main__.py     # CLI entry point  
│   ├── cleaner.py      # Data cleaning functions
│   ├── converter.py    # Format conversion functions
│   ├── reporter.py     # Report generation
│   └── streamlit_app.py # Web interface
├── tests/              # Test directory
├── README.md           # User documentation
├── pyproject.toml      # Project configuration
└── requirements.txt    # Dependencies
```

## Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd DataMorpher
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install the package in development mode**
   ```bash
   pip install -e .
   ```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=datamorpher

# Run specific test file
pytest tests/test_cleaner.py

# Run with verbose output
pytest -v
```

## Linting and Code Style

This project uses `ruff` for linting:

```bash
# Run linter
ruff check .

# Auto-fix issues when possible
ruff check --fix .

# Check code formatting
ruff format --check .

# Format code
ruff format .
```

## Common Commands

```bash
# Run the CLI
python -m datamorpher --help

# Convert a file
python -m datamorpher convert input.csv output.json

# Clean a dataset
python -m datamorpher clean input.csv output.csv

# Generate report
python -m datamorpher report input.csv report.md

# Run Streamlit app
streamlit run datamorpher/streamlit_app.py
```

## Code Conventions

1. **File Naming**: Use lowercase with underscores (snake_case)
2. **Python Style**: Follow PEP 8 and maintained by ruff
3. **Imports**: Sort imports alphabetically, stdlib first, then third-party, then local
4. **Docstrings**: Use Google style docstrings for all public functions
5. **Type Hints**: Use type hints for function signatures
6. **Error Handling**: Raise appropriate exceptions with meaningful messages

## Testing Guidelines

1. **Test Coverage**: Aim for >90% code coverage
2. **Test Structure**: Mirror the source structure (test_<module>.py)
3. **Test Data**: Use minimal fixture data when possible
4. **Mocking**: Mock external dependencies like file I/O when appropriate
5. **Edge Cases**: Test error conditions and edge cases

## Making Changes

1. **Before Changes**:
   - Read existing code to understand patterns
   - Run tests to ensure everything is working
   - Check current linting status

2. **During Development**:
   - Follow existing code patterns
   - Add tests for new functionality
   - Update docstrings and type hints
   - Handle errors appropriately

3. **Before Committing**:
   - Run all tests: `pytest`
   - Check linting: `ruff check .`
   - Format code: `ruff format .`
   - Update documentation if needed

## Key Implementation Details

### DataCleaner (cleaner.py)
- Handles missing values, duplicates, data type standardization
- Main method: `clean_data(data: pd.DataFrame) -> pd.DataFrame`
- Validates data before cleaning

### DataConverter (converter.py)
- Converts between CSV, JSON, and Excel formats
- Main method: `convert(input_file, output_file)`
- Auto-detects input format, infers output from extension

### Reporter (reporter.py)
- Generates Markdown reports about datasets
- Includes statistics, data types, missing values, samples
- Main method: `generate_report(data) -> str`

### CLI (__main__.py)
- Uses argparse for command parsing
- Commands: convert, clean, report
- Handles file I/O and error reporting

### Streamlit App
- Web interface for all functionality
- File upload/download support
- Interactive data preview

## Common Issues and Solutions

1. **Import Errors**: Ensure package is installed with `pip install -e .`
2. **File Not Found**: Use absolute paths or run from project root
3. **Type Errors**: Check that input data matches expected format
4. **Memory Issues**: For large files, consider chunked processing

## CI/CD Considerations

1. Tests must pass before merging
2. Linting must pass (ruff)
3. Code coverage should not decrease
4. Documentation should be updated for new features

## Important Notes

- Always validate user input to prevent errors
- Preserve data integrity during conversions
- Provide meaningful error messages
- Keep dependencies minimal and well-documented
- Maintain backward compatibility