# DataMorpher

[![CI](https://github.com/NeurArk/DataMorpher/actions/workflows/ci.yml/badge.svg)](https://github.com/NeurArk/DataMorpher/actions/workflows/ci.yml)

A compact Python utility for seamless data conversion and cleaning, featuring both CLI and web interfaces. DataMorpher automatically detects data types, handles missing values, removes duplicates, and generates detailed conversion reports.

## Features

### Core Capabilities
- **Multi-format Support**: Convert between CSV, Excel (.xlsx/.xls), and JSON formats
- **Automatic Type Detection**: Intelligently identifies numeric, categorical, date, and boolean columns
- **Data Cleaning**: Remove duplicates, detect anomalies, and impute missing values
- **Smart JSON Detection**: Handles standard and newline-delimited JSON
- **Robust Validation**: Cleans malformed numbers and dates
- **Detailed Reports**: Generate Markdown reports with conversion statistics
- **Dual Interface**: Choose between CLI for power users or Streamlit web app for non-technical users

### Performance
- Process 50,000+ row datasets in under 5 seconds
- Minimal memory footprint with streaming processing
- Type-safe operations with pandas backend

## Installation

```bash
# Clone the repository
git clone https://github.com/NeurArk/DataMorpher.git
cd DataMorpher

# Install dependencies
pip install .

# For development (includes testing tools)
pip install .[dev]
```

## Usage

### Command Line Interface

Basic conversion:
```bash
python -m datamorpher --input data.csv --output data.xlsx
```

With cleaning and report:
```bash
python -m datamorpher --input sales.csv --output sales.json --clean --report report.md
```

Force overwrite existing files:
```bash
python -m datamorpher --input data.json --output data.csv --force
```

### Web Interface (Streamlit)

Launch the web application:
```bash
streamlit run datamorpher/streamlit_app.py
```

The web interface provides:
- Drag-and-drop file upload
- Format selection via radio buttons
- Cleaning options with checkboxes
- One-click report generation
- Instant file download

## Project Structure

```
datamorpher/
├── __main__.py        # CLI entry point using Typer
├── converter.py       # Core conversion logic
├── cleaner.py         # Data cleaning operations
├── reporter.py        # Markdown report generation
└── streamlit_app.py   # Web interface
tests/
├── test_converter.py
├── test_cleaner.py
└── test_reporter.py
.github/
└── workflows/
    └── ci.yml         # Continuous integration
pyproject.toml         # Project configuration
```

## Data Cleaning Options

When enabled, DataMorpher performs:
- **Duplicate Removal**: Eliminates exact row duplicates
- **Anomaly Detection**: Detects and reports issues like negative values in stock, infinity values, etc.
- **Missing Value Imputation**:
  - Numeric columns: Median value
  - Categorical columns: Mode (most frequent value)
  - Date columns: Left as missing
  - Boolean columns: Mode
- **Smart Parsing**: 
  - Extracts numbers from corrupted strings (e.g. `"8000foo0" -> 8000`)
  - Converts textual expressions (e.g. `"four hundred fifty" -> 450`)
  - Handles special patterns like `"95ABC.50" -> 95.50`
- **Date Formats**: Supports multiple formats including `%Y-%m-%d`, `%d/%m/%Y`, `%m/%d/%Y`, `%Y/%m/%d` and textual formats like "March 20 2023"

## Conversion Report

Each conversion generates a Markdown report containing:
- Number of rows read/written
- Duplicates removed count
- Values imputed per column
- Detected data types
- Total execution time
- Detailed transformations of cleaned values
- Detected anomalies and warnings

Example report snippet:
```markdown
# DataMorpher Conversion Report

## Summary
- Input: sales.csv (50,000 rows)
- Output: sales.xlsx (49,850 rows)
- Duplicates removed: 150
- Execution time: 2.3s

## Column Types Detected
- OrderID: numeric
- CustomerName: categorical
- OrderDate: date
- Amount: numeric
- Status: categorical
```

The report also lists transformations applied and anomalies detected, for example:

```markdown
## Applied Transformations
### Column 'price'
- $49.99 -> 49.99 (currency conversion)
- 200$ -> 200.0 (currency conversion)

## Notes and Warnings
- Column 'stock' contains 1 negative value(s)
- Column 'stock' contains 1 infinite value(s)
```

## Sample Data

A messy CSV file is included in `sample_data/test_messy_data_improved.csv` to test the
cleaning features, anomaly detection, and edge cases.

## Development

### Running Tests
```bash
pytest
```

### Linting
```bash
ruff check .
```

### Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Requirements

- Python 3.12+
- pandas
- openpyxl
- streamlit
- typer
- tabulate

## License

MIT License - see LICENSE file for details

## Acknowledgments

Built with modern Python tooling including:
- pandas for data manipulation
- Typer for CLI development
- Streamlit for web interfaces
- Ruff for code quality
- pytest for testing