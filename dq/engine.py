"""Core SQL engine: load data files into SQLite and query them."""

from __future__ import annotations

import io
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import requests


def _table_name(path: str) -> str:
    """Derive a SQL table name from a file path or URL."""
    if path.startswith(("http://", "https://")):
        name = path.rsplit("/", 1)[-1]
    else:
        name = Path(path).stem
    # Sanitize: keep only alphanumeric and underscores
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if name[0:1].isdigit():
        name = "_" + name
    return name


def _detect_format(path: str) -> str:
    """Detect file format from extension."""
    lower = path.lower().split("?")[0]  # strip query params for URLs
    if lower.endswith(".json") or lower.endswith(".jsonl"):
        return "json"
    if lower.endswith(".tsv") or lower.endswith(".tab"):
        return "tsv"
    if lower.endswith(".parquet"):
        return "parquet"
    if lower.endswith((".xls", ".xlsx")):
        return "excel"
    return "csv"  # default


def _read_file(path: str) -> pd.DataFrame:
    """Read a data file (local or remote) into a DataFrame."""
    fmt = _detect_format(path)

    if path.startswith(("http://", "https://")):
        resp = requests.get(path, timeout=30)
        resp.raise_for_status()
        # Use BytesIO for binary formats, StringIO for text formats
        if fmt in ("parquet", "excel"):
            content = io.BytesIO(resp.content)
        else:
            content = io.StringIO(resp.text)
    else:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {path}")
        content = p

    if fmt == "json":
        return pd.read_json(content)
    if fmt == "tsv":
        return pd.read_csv(content, sep="\t")
    if fmt == "parquet":
        return pd.read_parquet(content)
    if fmt == "excel":
        return pd.read_excel(content)
    return pd.read_csv(content)


# Regex to find potential file references in SQL.
# Matches: word.ext, path/to/word.ext, or URLs.
_FILE_RE = re.compile(
    r"""(?:https?://\S+\.(?:csv|tsv|json|jsonl|parquet|xlsx?|tab)(?:\?\S*)?)"""
    r"""|(?:(?:[\w./\\-]+\.(?:csv|tsv|json|jsonl|parquet|xlsx?|tab)))""",
    re.IGNORECASE,
)


def _extract_file_refs(sql: str) -> list[tuple[str, int, int]]:
    """Extract file path/URL references from a SQL query with positions."""
    results = []
    for m in _FILE_RE.finditer(sql):
        results.append((m.group(), m.start(), m.end()))
    return results


class Engine:
    """In-memory SQLite engine backed by pandas for file I/O."""

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.tables: dict[str, str] = {}  # table_name -> original path

    def load(self, path: str, alias: str | None = None) -> str:
        """Load a file into the database. Returns the table name."""
        df = _read_file(path)
        name = alias or _table_name(path)
        # Handle name collisions: append suffix if name already taken by a different file
        if name in self.tables and self.tables[name] != path:
            base = name
            i = 2
            while name in self.tables:
                name = f"{base}_{i}"
                i += 1
            print(f"Warning: table '{base}' already exists, using '{name}'", file=sys.stderr)
        df.to_sql(name, self.conn, if_exists="replace", index=False)
        self.tables[name] = path
        return name

    def query(self, sql: str) -> pd.DataFrame:
        """Run a SQL query. Auto-loads any file references found in the SQL."""
        # Process refs in reverse order so position-based replacement stays correct
        refs = _extract_file_refs(sql)
        refs.sort(key=lambda r: r[1], reverse=True)
        for ref, start, end in refs:
            name = _table_name(ref)
            if name not in self.tables:
                name = self.load(ref)
            # Replace only this specific occurrence by position
            sql = sql[:start] + name + sql[end:]
        return pd.read_sql_query(sql, self.conn)

    def describe(self, path: str) -> dict:
        """Describe a data file: columns, types, row count, sample values."""
        df = _read_file(path)
        info = {
            "file": path,
            "rows": len(df),
            "columns": [],
        }
        for col in df.columns:
            series = df[col]
            col_info = {
                "name": col,
                "dtype": str(series.dtype),
                "nulls": int(series.isna().sum()),
                "unique": int(series.nunique()),
                "sample": [str(v) for v in series.head(3).tolist()],
            }
            info["columns"].append(col_info)
        return info

    def list_tables(self) -> list[str]:
        """List all loaded tables."""
        return list(self.tables.keys())

    def close(self) -> None:
        self.conn.close()
