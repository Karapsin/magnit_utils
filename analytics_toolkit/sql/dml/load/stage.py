from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

import pandas as pd
from sqlglot import exp, parse_one

from ...identifiers import sqlglot_dialect as _registry_sqlglot_dialect
from analytics_toolkit.general import time_print
from ...ddl.create_sql_table import create_sql_table
from ..table.table_ops import table_exists


STAGE_TABLE_NAME_MAX_ATTEMPTS = 10


def create_stage_table(
    connection_type: str,
    connection: Any,
    target_table: str,
    batch: pd.DataFrame,
    column_types: Mapping[str, str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    connection_key: str | None = None,
    query_label: str | None = None,
) -> str:
    for attempt in range(1, STAGE_TABLE_NAME_MAX_ATTEMPTS + 1):
        stage_table = build_stage_table_name(connection_type, target_table)
        if table_exists(
            connection_type,
            connection,
            stage_table,
            connection_key=connection_key or connection_type,
        ):
            time_print(
                f"Stage table name collision detected for {stage_table}; "
                f"retrying with a new name ({attempt}/{STAGE_TABLE_NAME_MAX_ATTEMPTS})"
            )
            continue

        create_kwargs: dict[str, Any] = {}
        if query_label is not None:
            create_kwargs["query_label"] = query_label
        create_sql_table(
            connection_type,
            connection,
            stage_table,
            batch,
            column_types=column_types,
            gp_distributed_by_key=gp_distributed_by_key,
            **create_kwargs,
        )
        return stage_table

    raise RuntimeError(
        "Could not generate a unique stage table name after "
        f"{STAGE_TABLE_NAME_MAX_ATTEMPTS} attempts."
    )


def build_stage_table_name(connection_type: str, table_name: str) -> str:
    dialect = sqlglot_dialect(connection_type)
    table = parse_one(table_name, read=dialect, into=exp.Table)
    if not isinstance(table, exp.Table) or not isinstance(table.this, exp.Identifier):
        raise ValueError(f"Invalid target table name: {table_name}")

    stage_suffix = uuid.uuid4().hex[:8]
    base_identifier = table.this.this
    stage_identifier = exp.to_identifier(
        f"{base_identifier}__stage__{stage_suffix}",
        quoted=bool(table.this.args.get("quoted")),
    )
    stage_table = table.copy()
    stage_table.set("this", stage_identifier)
    return stage_table.sql(dialect=dialect)


def sqlglot_dialect(connection_type: str) -> str:
    return _registry_sqlglot_dialect(connection_type)
