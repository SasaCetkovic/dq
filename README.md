# dq — Query Any Data File with SQL

A command-line tool that lets you run SQL queries against CSV, JSON, and TSV files directly from your terminal.

## Quick Start

```bash
# Query a CSV file
dq "SELECT * FROM data.csv WHERE amount > 100"

# Aggregate data
dq "SELECT category, SUM(amount) as total FROM sales.csv GROUP BY category"

# Join across files (CSV + JSON)
dq "SELECT c.name, o.total FROM customers.csv c JOIN orders.json o ON c.id = o.customer_id"

# Describe a file's schema
dq describe data.csv

# Output as CSV or JSON
dq --format csv "SELECT * FROM data.csv"
dq --format json "SELECT * FROM data.csv"

# Interactive REPL
dq
```     

## Installation

### From PyPI (Recommended)

```bash
uv tool install dq-cli
```

### From Source

```bash
uv sync
uv run main.py "SELECT * FROM your_file.csv"
```

## Features

- **SQL on files** — Use standard SQL (SELECT, WHERE, GROUP BY, JOIN, ORDER BY, etc.)
- **Auto-detect formats** — CSV, TSV, JSON, Parquet, Excel
- **Cross-file joins** — Join data across different file formats
- **Remote files** — Query files from URLs directly
- **Schema inspection** — `dq describe file.csv` shows columns, types, and sample values
- **Multiple output formats** — Table (default), CSV, JSON, TSV
- **Interactive REPL** — Load files and run queries interactively

## REPL Commands

| Command | Description |
|---------|-------------|
| `/load file.csv [as alias]` | Load a file into the session |
| `/tables` | List loaded tables |
| `/describe <table>` | Show table schema |
| `/quit` | Exit |
