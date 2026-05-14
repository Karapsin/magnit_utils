from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlparse

from ...connection.config import get_connection_config
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.get_sql_connection import get_sql_connection
from ...labels import apply_query_label
from ...plans import SqlPlan
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
) -> SqlPlan | None:
    config = get_connection_config(db_key)
    if config.backend != "ch":
        raise UnsupportedConnectionTypeError(
            f"ch_create_table_as requires a ch connection, got '{config.backend}'."
        )

    target_table = _normalize_non_empty_string(table_name, "table_name")
    query_sql = _normalize_single_query(query)
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")

    if dry_run or return_sql:
        target_shard_table = build_ch_shard_table_name(target_table)
        plan = SqlPlan(
            operation="ch_create_table_as",
            target_alias=config.connection_key,
            target_backend=config.backend,
            target_table=target_table,
            options={
                "ch_partition_by": ch_partition_by,
                "ch_order_by": ch_order_by,
                "ch_engine": ch_engine,
                "ch_cluster": cluster_name,
                "sharding_key": sharding_key,
            },
        )
        for sql in [
            *build_drop_ch_distributed_table_pair_sqls(
                target_table,
                ch_cluster=cluster_name,
            ),
            f"CREATE TABLE IF NOT EXISTS {target_shard_table} (<query schema>)",
            f"CREATE TABLE IF NOT EXISTS {target_table} (<query schema>)",
            _build_insert_select_sql(target_table, query_sql),
        ]:
            plan.add(
                sql,
                alias=config.connection_key,
                backend=config.backend,
                target_table=target_table,
                query_label=query_label,
            )
        return plan

    connection = get_sql_connection(config.connection_key)
    try:
        target_shard_table = build_ch_shard_table_name(target_table)
        time_print(
            f"Creating ClickHouse table {target_table} from query on "
            f"{config.connection_key}"
        )
        time_print(
            f"Dropping target ClickHouse table pair {target_table} / "
            f"{target_shard_table}"
        )
        drop_ch_distributed_table_pair(
            connection,
            target_table,
            ch_cluster=cluster_name,
            query_label=query_label,
        )

        time_print(f"Inferring ClickHouse schema for {target_table}")
        joined_columns = _infer_ch_query_columns(connection, query_sql)
        shard_sql, distributed_sql, local_distributed_sql = (
            build_ch_create_table_as_sqls(
                table_name=target_table,
                joined_columns=joined_columns,
                query=query_sql,
                ch_partition_by=ch_partition_by,
                ch_order_by=ch_order_by,
                ch_engine=ch_engine,
                ch_cluster=cluster_name,
                ch_sharding_key=sharding_key,
                query_label=query_label,
            )
        )

        time_print(f"Creating target shard table {target_shard_table}")
        _execute_ch_command(connection, shard_sql)
        time_print(f"Creating target distributed table {target_table}")
        _execute_ch_command(connection, distributed_sql)
        time_print(f"Creating local distributed table {target_table}")
        _execute_ch_command(connection, local_distributed_sql)
        time_print(f"Waiting for target table {target_table}")
        _wait_for_ch_table(connection, target_table)
        time_print(f"Inserting query results into {target_table}")
        connection.command(
            apply_query_label(_build_insert_select_sql(target_table, query_sql), query_label)
        )
        time_print(f"Finished creating ClickHouse table {target_table}")
    finally:
        time_print(f"Closing {config.connection_key} connection")
        connection.close()


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
