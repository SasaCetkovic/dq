"""CLI argument parsing and dispatch for dq."""

from __future__ import annotations

import argparse
import sys

from .display import format_describe, output_result
from .engine import Engine
from .repl import run_repl


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="dq",
        description="Query any data file (CSV, JSON, TSV) with SQL.",
        epilog="Examples:\n"
        '  dq "SELECT * FROM sales.csv WHERE amount > 100"\n'
        '  dq "SELECT a.name, b.total FROM users.csv a JOIN orders.csv b ON a.id = b.user_id"\n'
        "  dq describe data.csv\n"
        "  dq   (opens interactive REPL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", nargs="*", help="SQL query or 'describe <file>'")
    parser.add_argument(
        "--format", "-f",
        choices=["table", "csv", "json", "tsv"],
        default="table",
        dest="output_format",
        help="Output format (default: table)",
    )

    args = parser.parse_args(argv)

    if not args.command:
        run_repl()
        return

    # Handle 'describe' subcommand
    if args.command[0].lower() == "describe":
        if len(args.command) < 2:
            print("Usage: dq describe <file>", file=sys.stderr)
            sys.exit(1)
        engine = Engine()
        try:
            info = engine.describe(args.command[1])
            print(format_describe(info))
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            engine.close()
        return

    # Treat everything as a SQL query
    sql = " ".join(args.command)
    engine = Engine()
    try:
        df = engine.query(sql)
        output_result(df, args.output_format)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        engine.close()
