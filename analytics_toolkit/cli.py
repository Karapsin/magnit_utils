from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from .sql.capabilities import format_support_matrix
from .sql.connection import validate_connections


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 2
    return int(args.handler(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="analytics-toolkit")
    subparsers = parser.add_subparsers(dest="command")

    sql_parser = subparsers.add_parser("sql")
    sql_subparsers = sql_parser.add_subparsers(dest="sql_command")

    validate_parser = sql_subparsers.add_parser("validate")
    validate_parser.add_argument("keys", nargs="*")
    validate_parser.add_argument(
        "--connect",
        action="store_true",
        help="Open and close each configured connection after validating config.",
    )
    validate_parser.set_defaults(handler=_handle_sql_validate)

    support_parser = sql_subparsers.add_parser("support-matrix")
    support_parser.set_defaults(handler=_handle_sql_support_matrix)
    return parser


def _handle_sql_validate(args: argparse.Namespace) -> int:
    results = validate_connections(args.keys or None, connect=args.connect)
    for result in results:
        status = "OK" if result.valid else "ERROR"
        backend = result.backend or "-"
        connected = ""
        if args.connect:
            connected = " connected=yes" if result.connected else " connected=no"
        line = f"{status} {result.connection_key} backend={backend}{connected}"
        if result.error:
            line += f" error={result.error}"
        print(line)
    return 0 if all(result.valid for result in results) else 1


def _handle_sql_support_matrix(args: argparse.Namespace) -> int:
    del args
    print(format_support_matrix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
