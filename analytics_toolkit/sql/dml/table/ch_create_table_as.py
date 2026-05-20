from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlparse

from ...connection.config import get_connection_config
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.get_sql_connection import get_sql_connection
from ...labels import apply_query_label
from ...operation_runner import tracked_sql_operation
from ...plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from ...ch_lifecycle import (
    build_create_ch_distributed_table_pair_sqls,
    build_drop_ch_distributed_table_pair_sqls,
    drop_ch_distributed_table_pair,
)
from ...ddl.create_sql_table import (
    _normalize_non_empty_string,
    _wait_for_ch_table,
    build_ch_shard_table_name,
    quote_identifier,
)
from analytics_toolkit.general import time_print
from .models import ChCreateTableAsOptions
from .table_ops import _execute_ch_command


def ch_create_table_as(
    db_key: str,
    table_name: str,
    query: str,
    *,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    sharding_key: str = "rand()",
    dry_run: bool = False,
    return_sql: bool = False,
    query_label: str | None = None,
    return_metadata: bool = False,
) -> SqlPlan | SqlOperationResult | None:
    config = get_connection_config(db_key)
    if config.backend != "ch":
        raise UnsupportedConnectionTypeError(
            f"ch_create_table_as requires a ch connection, got '{config.backend}'."
        )

    target_table = _normalize_non_empty_string(table_name, "table_name")
    query_sql = _normalize_single_query(query)
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")
    options = ChCreateTableAsOptions(
        connection_key=config.connection_key,
        backend=config.backend,
        target_table=target_table,
        query_sql=query_sql,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=cluster_name,
        ch_sharding_key=sharding_key,
        dry_run=dry_run,
        return_sql=return_sql,
        return_metadata=return_metadata,
        query_label=query_label,
    )

    if options.dry_run or options.return_sql:
        target_shard_table = build_ch_shard_table_name(options.target_table)
        sqls = [
            *build_drop_ch_distributed_table_pair_sqls(
                options.target_table,
                ch_cluster=options.ch_cluster,
            ),
            f"CREATE TABLE IF NOT EXISTS {target_shard_table} (<query schema>)",
            f"CREATE TABLE IF NOT EXISTS {options.target_table} (<query schema>)",
            _build_insert_select_sql(options.target_table, options.query_sql),
        ]
        plan = SqlPlan(
            operation="ch_create_table_as",
            target_alias=options.connection_key,
            target_backend=options.backend,
            target_table=options.target_table,
            options={
                "ch_partition_by": options.ch_partition_by,
                "ch_order_by": options.ch_order_by,
                "ch_engine": options.ch_engine,
                "ch_cluster": options.ch_cluster,
                "sharding_key": options.ch_sharding_key,
            },
            metadata=SqlOperationMetadata(
                statement_count=len(sqls),
                query_label=options.query_label,
            ),
        )
        phases = ["drop_target"] * 4 + ["create_target", "create_target", "insert_target"]
        for sql, phase in zip(sqls, phases):
            plan.add(
                sql,
                alias=options.connection_key,
                backend=options.backend,
                phase=phase,
                target_table=options.target_table,
                query_label=options.query_label,
            )
        return plan

    metadata = SqlOperationMetadata(query_label=options.query_label)
    connection = get_sql_connection(config.connection_key)
    try:
        with tracked_sql_operation(
            metadata=metadata,
            operation_name="ch_create_table_as",
            alias=options.connection_key,
            backend=options.backend,
            phase="create_target",
            query_label=options.query_label,
        ):
            target_shard_table = build_ch_shard_table_name(options.target_table)
            time_print(
                f"Creating ClickHouse table {options.target_table} from query on "
                f"{options.connection_key}"
            )
            time_print(
                f"Dropping target ClickHouse table pair {options.target_table} / "
                f"{target_shard_table}"
            )
            drop_ch_distributed_table_pair(
                connection,
                options.target_table,
                ch_cluster=options.ch_cluster,
                query_label=options.query_label,
            )

            time_print(f"Inferring ClickHouse schema for {options.target_table}")
            joined_columns = _infer_ch_query_columns(connection, options.query_sql)
            shard_sql, distributed_sql, local_distributed_sql = (
                build_ch_create_table_as_sqls(
                    table_name=options.target_table,
                    joined_columns=joined_columns,
                    query=options.query_sql,
                    ch_partition_by=options.ch_partition_by,
                    ch_order_by=options.ch_order_by,
                    ch_engine=options.ch_engine,
                    ch_cluster=options.ch_cluster,
                    ch_sharding_key=options.ch_sharding_key,
                    query_label=options.query_label,
                )
            )
            metadata.statement_count = 7

            time_print(f"Creating target shard table {target_shard_table}")
            _execute_ch_command(connection, shard_sql)
            time_print(f"Creating target distributed table {options.target_table}")
            _execute_ch_command(connection, distributed_sql)
            time_print(f"Creating local distributed table {options.target_table}")
            _execute_ch_command(connection, local_distributed_sql)
            time_print(f"Waiting for target table {options.target_table}")
            _wait_for_ch_table(connection, options.target_table)
            time_print(f"Inserting query results into {options.target_table}")
            connection.command(
                apply_query_label(
                    _build_insert_select_sql(options.target_table, options.query_sql),
                    options.query_label,
                )
            )
            time_print(f"Finished creating ClickHouse table {options.target_table}")
    finally:
        time_print(f"Closing {config.connection_key} connection")
        connection.close()
    if options.return_metadata:
        return SqlOperationResult(rows=None, metadata=metadata)
    return None


def build_ch_create_table_as_sqls(
    table_name: str,
    joined_columns: str,
    query: str,
    *,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> list[str]:
    target_table = _normalize_non_empty_string(table_name, "table_name")
    _normalize_single_query(query)
    columns_sql = _normalize_non_empty_string(joined_columns, "joined_columns")
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")
    sharding_key = _normalize_non_empty_string(ch_sharding_key, "ch_sharding_key")
    engine = _normalize_non_empty_string(ch_engine, "ch_engine")
    return build_create_ch_distributed_table_pair_sqls(
        table_name=target_table,
        joined_columns=columns_sql,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=engine,
        ch_cluster=cluster_name,
        ch_sharding_key=sharding_key,
        query_label=query_label,
    )


def _normalize_single_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise InvalidSqlInputError("Query string must not be empty.")

    statements = [
        statement.strip().rstrip(";").rstrip()
        for statement in sqlparse.split(normalized)
        if statement.strip()
    ]
    if len(statements) != 1:
        raise InvalidSqlInputError(
            "ch_create_table_as expects exactly one SQL statement."
        )
    return statements[0]


def _infer_ch_query_columns(connection: Any, query: str) -> str:
    result = connection.query(
        "SELECT *\n"
        "FROM (\n"
        f"{query}\n"
        ") AS _ch_create_table_as_source\n"
        "LIMIT 0"
    )
    column_names = list(getattr(result, "column_names", ()) or ())
    column_types = list(getattr(result, "column_types", ()) or ())
    if not column_names:
        raise ValueError("ch_create_table_as query must return at least one column.")
    if len(column_names) != len(column_types):
        raise ValueError("Could not infer ClickHouse column types from query result.")

    column_defs = [
        f"{quote_identifier(str(column_name), 'ch')} {_ch_type_name(column_type)}"
        for column_name, column_type in zip(column_names, column_types)
    ]
    return ", ".join(column_defs)


def _ch_type_name(column_type: Any) -> str:
    type_name = getattr(column_type, "name", None)
    if type_name is None:
        type_name = str(column_type)
    return _normalize_non_empty_string(str(type_name), "column_type")


def _build_insert_select_sql(table_name: str, query: str) -> str:
    return f"INSERT INTO {table_name}\n{query}"
