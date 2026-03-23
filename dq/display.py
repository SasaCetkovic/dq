"""Pretty-print query results as formatted tables."""

from __future__ import annotations

import sys

import pandas as pd


def _truncate(val: str, max_len: int = 40) -> str:
    if len(val) > max_len:
        return val[: max_len - 1] + "\u2026"
    return val


def format_table(df: pd.DataFrame) -> str:
    """Format a DataFrame as an aligned text table."""
    if df.empty:
        return "(0 rows)"

    # Convert to strings and truncate
    str_df = df.astype(str).map(lambda v: _truncate(v))
    headers = list(str_df.columns)

    # Calculate column widths
    widths = []
    for i, col in enumerate(headers):
        max_val_len = str_df[col].str.len().max()
        col_width = max(len(col), int(max_val_len) if pd.notna(max_val_len) else 0)
        widths.append(min(col_width, 40))

    # Build header
    lines = []
    header = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)

    # Build rows
    for _, row in str_df.iterrows():
        line = " | ".join(
            str(row[col]).ljust(w) for col, w in zip(headers, widths)
        )
        lines.append(line)

    lines.append(f"({len(df)} row{'s' if len(df) != 1 else ''})")
    return "\n".join(lines)


def format_describe(info: dict) -> str:
    """Format a describe result as a readable summary."""
    lines = [
        f"File:    {info['file']}",
        f"Rows:    {info['rows']}",
        f"Columns: {len(info['columns'])}",
        "",
    ]

    # Column details table
    col_headers = ["Column", "Type", "Nulls", "Unique", "Sample"]
    rows = []
    for c in info["columns"]:
        sample = ", ".join(c["sample"])
        rows.append([c["name"], c["dtype"], str(c["nulls"]), str(c["unique"]), sample])

    # Calculate widths
    widths = [len(h) for h in col_headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], min(len(_truncate(val)), 40))

    header = " | ".join(h.ljust(w) for h, w in zip(col_headers, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)
    for row in rows:
        line = " | ".join(_truncate(val).ljust(w) for val, w in zip(row, widths))
        lines.append(line)

    return "\n".join(lines)


def output_result(df: pd.DataFrame, fmt: str = "table", file=None) -> None:
    """Output a DataFrame in the specified format."""
    file = file or sys.stdout
    if fmt == "csv":
        df.to_csv(file, index=False)
    elif fmt == "json":
        file.write(df.to_json(orient="records", indent=2))
        file.write("\n")
    elif fmt == "tsv":
        df.to_csv(file, index=False, sep="\t")
    else:
        print(format_table(df), file=file)
