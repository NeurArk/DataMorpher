# DataMorpher

[![CI](https://github.com/NeurArk/DataMorpher/actions/workflows/ci.yml/badge.svg)](https://github.com/NeurArk/DataMorpher/actions/workflows/ci.yml)

A compact Python utility for seamless data conversion and cleaning, featuring both CLI and web interfaces. DataMorpher automatically detects data types, handles missing values, removes duplicates, and generates detailed conversion reports.

## Features

### Core Capabilities
- **Multi-format Support**: Convert between CSV, Excel (.xlsx/.xls), and JSON formats
- **Automatic Type Detection**: Intelligently identifies numeric, categorical, date, and boolean columns
- **Data Cleaning**: Remove duplicates and impute missing values
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
- **Missing Value Imputation**:
  - Numeric columns: Mean value
  - Categorical columns: Mode (most frequent value)
  - Date columns: Left as missing
  - Boolean columns: Mode

## Conversion Report

Each conversion generates a Markdown report containing:
- Number of rows read/written
- Duplicates removed count
- Values imputed per column
- Detected data types
- Total execution time

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