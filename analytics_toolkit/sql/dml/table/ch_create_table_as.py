from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlparse

from ...connection.config import get_connection_config
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import (
    _build_ch_order_by_sql,
    _build_ch_partition_by_sql,
    _normalize_non_empty_string,
    _sql_string_literal,
    _wait_for_ch_table,
    build_ch_shard_table_name,
    quote_identifier,
    split_ch_table_name_for_distributed_engine,
)
from analytics_toolkit.general import time_print
from .table_ops import (
    _ch_cluster_clause,
    _execute_ch_command,
    drop_ch_distributed_table_pair,
)


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
) -> None:
    config = get_connection_config(db_key)
    if config.backend != "ch":
        raise UnsupportedConnectionTypeError(
            f"ch_create_table_as requires a ch connection, got '{config.backend}'."
        )

    target_table = _normalize_non_empty_string(table_name, "table_name")
    query_sql = _normalize_single_query(query)
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")

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
        connection.command(_build_insert_select_sql(target_table, query_sql))
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
) -> list[str]:
    target_table = _normalize_non_empty_string(table_name, "table_name")
    _normalize_single_query(query)
    columns_sql = _normalize_non_empty_string(joined_columns, "joined_columns")
    shard_table = build_ch_shard_table_name(target_table)
    cluster_name = _normalize_non_empty_string(ch_cluster, "ch_cluster")
    engine = _normalize_non_empty_string(ch_engine, "ch_engine")
    sharding_key = _normalize_non_empty_string(ch_sharding_key, "ch_sharding_key")
    partition_sql = _build_ch_partition_by_sql(ch_partition_by)
    order_by_sql = _build_ch_order_by_sql(ch_order_by)
    database_name, shard_relation_name = split_ch_table_name_for_distributed_engine(
        shard_table
    )
    distributed_engine_sql = _build_distributed_engine_sql(
        cluster_name=cluster_name,
        database_name=database_name,
        shard_relation_name=shard_relation_name,
        sharding_key=sharding_key,
    )

    shard_sql = (
        f"CREATE TABLE IF NOT EXISTS {shard_table}\n"
        f"{_on_cluster_clause_line(cluster_name)}"
        f"({columns_sql})\n"
        f"ENGINE = {engine}\n"
        f"{partition_sql}"
        f"{order_by_sql}"
    )
    distributed_sql = (
        f"CREATE TABLE IF NOT EXISTS {target_table}\n"
        f"{_on_cluster_clause_line(cluster_name)}"
        f"({columns_sql})\n"
        f"{distributed_engine_sql}"
    )
    local_distributed_sql = (
        f"CREATE TABLE IF NOT EXISTS {target_table}\n"
        f"({columns_sql})\n"
        f"{distributed_engine_sql}"
    )
    return [shard_sql, distributed_sql, local_distributed_sql]


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


def _build_distributed_engine_sql(
    *,
    cluster_name: str,
    database_name: str,
    shard_relation_name: str,
    sharding_key: str,
) -> str:
    return (
        "ENGINE = Distributed(\n"
        f"    {_sql_string_literal(cluster_name)},\n"
        f"    {database_name},\n"
        f"    {_sql_string_literal(shard_relation_name)},\n"
        f"    {sharding_key}\n"
        ")"
    )


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


def _on_cluster_clause_line(cluster_name: str) -> str:
    return f"{_ch_cluster_clause(cluster_name).strip()}\n"
