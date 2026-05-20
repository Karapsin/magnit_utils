from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .connection.config import BackendName, resolve_connection_backend
from .operation_runner import timed_public_sql_function


WriteMode = Literal["append", "replace", "truncate_insert", "upsert"]


@dataclass(frozen=True)
class BackendCapability:
    name: BackendName
    display_name: str
    sqlglot_dialect: str
    identifier_quote: str
    supports_transactions: bool
    supports_analyze: bool
    uses_stage_tables: bool
    supports_distributed_tables: bool
    truncate_semantics: str
    drop_semantics: str
    create_semantics: str
    type_family: str
    supported_write_modes: frozenset[WriteMode]


BACKEND_CAPABILITIES: dict[BackendName, BackendCapability] = {
    "gp": BackendCapability(
        name="gp",
        display_name="Greenplum",
        sqlglot_dialect="postgres",
        identifier_quote='"',
        supports_transactions=True,
        supports_analyze=True,
        uses_stage_tables=True,
        supports_distributed_tables=False,
        truncate_semantics="TRUNCATE TABLE",
        drop_semantics="DROP TABLE IF EXISTS",
        create_semantics="CREATE TABLE with append-only columnar storage",
        type_family="postgres",
        supported_write_modes=frozenset({"append", "replace", "truncate_insert"}),
    ),
    "trino": BackendCapability(
        name="trino",
        display_name="Trino",
        sqlglot_dialect="trino",
        identifier_quote='"',
        supports_transactions=False,
        supports_analyze=True,
        uses_stage_tables=True,
        supports_distributed_tables=False,
        truncate_semantics="DELETE FROM",
        drop_semantics="DROP TABLE IF EXISTS",
        create_semantics="CREATE TABLE WITH parquet/object-store layout",
        type_family="trino",
        supported_write_modes=frozenset({"append", "replace", "truncate_insert"}),
    ),
    "ch": BackendCapability(
        name="ch",
        display_name="ClickHouse",
        sqlglot_dialect="clickhouse",
        identifier_quote="`",
        supports_transactions=False,
        supports_analyze=False,
        uses_stage_tables=True,
        supports_distributed_tables=True,
        truncate_semantics="TRUNCATE TABLE IF EXISTS",
        drop_semantics="DROP TABLE IF EXISTS plus distributed pair when requested",
        create_semantics="MergeTree or shard plus Distributed pair",
        type_family="clickhouse",
        supported_write_modes=frozenset({"append", "replace", "truncate_insert"}),
    ),
}


def get_backend_capability(connection_type_or_key: str) -> BackendCapability:
    backend = resolve_connection_backend(connection_type_or_key)
    return BACKEND_CAPABILITIES[backend]


def validate_write_mode(
    connection_type_or_key: str,
    write_mode: str,
    *,
    option_name: str = "write_mode",
) -> WriteMode:
    normalized = write_mode.strip().lower()
    if normalized not in {"append", "replace", "truncate_insert", "upsert"}:
        raise ValueError(
            f"{option_name} must be one of: append, replace, truncate_insert, upsert."
        )

    capability = get_backend_capability(connection_type_or_key)
    if normalized not in capability.supported_write_modes:
        raise ValueError(
            f"{capability.display_name} does not support {option_name}={normalized!r}."
        )
    return normalized  # type: ignore[return-value]


@timed_public_sql_function
def support_matrix_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for capability in BACKEND_CAPABILITIES.values():
        rows.append(
            {
                "backend": capability.name,
                "name": capability.display_name,
                "dialect": capability.sqlglot_dialect,
                "transactions": _yes_no(capability.supports_transactions),
                "analyze": _yes_no(capability.supports_analyze),
                "distributed": _yes_no(capability.supports_distributed_tables),
                "write_modes": ", ".join(sorted(capability.supported_write_modes)),
                "truncate": capability.truncate_semantics,
            }
        )
    return rows


@timed_public_sql_function
def format_support_matrix() -> str:
    headers = [
        "Backend",
        "Dialect",
        "Transactions",
        "Analyze",
        "Distributed",
        "Write modes",
        "Truncate",
    ]
    rows = [
        [
            row["backend"],
            row["dialect"],
            row["transactions"],
            row["analyze"],
            row["distributed"],
            row["write_modes"],
            row["truncate"],
        ]
        for row in support_matrix_rows()
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    header_line = "  ".join(
        header.ljust(widths[index]) for index, header in enumerate(headers)
    )
    divider = "  ".join("-" * width for width in widths)
    body = [
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in rows
    ]
    return "\n".join([header_line, divider, *body])


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
