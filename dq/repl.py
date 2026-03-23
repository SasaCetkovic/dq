"""Interactive REPL for dq."""

from __future__ import annotations

import sys

try:
    import readline  # noqa: F401 — enables arrow-key history on POSIX
except ImportError:
    pass

from .display import format_describe, format_table, output_result
from .engine import Engine


def run_repl() -> None:
    """Start the interactive dq REPL."""
    engine = Engine()
    print("dq — interactive SQL shell")
    print("Commands: \\load <file> [as <alias>], \\tables, \\describe <table>, \\quit")
    print("Type SQL to query. File references (e.g. data.csv) are auto-loaded.\n")

    while True:
        try:
            line = input("dq> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        if line.lower() in ("\\quit", "\\q", "exit", "quit"):
            break

        if line.lower().startswith("\\load"):
            _handle_load(engine, line)
            continue

        if line.lower() in ("\\tables", "\\t"):
            tables = engine.list_tables()
            if tables:
                for t in tables:
                    print(f"  {t}  ({engine.tables[t]})")
            else:
                print("  (no tables loaded)")
            continue

        if line.lower().startswith("\\describe") or line.lower().startswith("\\d "):
            _handle_describe(engine, line)
            continue

        # Treat as SQL
        try:
            df = engine.query(line)
            print(format_table(df))
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)

    engine.close()


def _handle_load(engine: Engine, line: str) -> None:
    """Parse and execute: \\load <file> [as <alias>]"""
    parts = line.split()
    if len(parts) < 2:
        print("Usage: \\load <file> [as <alias>]")
        return
    path = parts[1]
    alias = None
    if len(parts) >= 4 and parts[2].lower() == "as":
        alias = parts[3]
    try:
        name = engine.load(path, alias)
        print(f"Loaded '{path}' as table '{name}'")
    except Exception as e:
        print(f"Error loading '{path}': {e}", file=sys.stderr)


def _handle_describe(engine: Engine, line: str) -> None:
    """Parse and execute: \\describe <file_or_table>"""
    parts = line.split()
    if len(parts) < 2:
        print("Usage: \\describe <file_or_table>")
        return
    target = parts[1]

    # If it's a loaded table name, describe from the database
    if target in engine.tables:
        target = engine.tables[target]

    try:
        info = engine.describe(target)
        print(format_describe(info))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
