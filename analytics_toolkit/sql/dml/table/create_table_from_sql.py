from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
import sqlparse

from ...ch_options import (
    normalize_ch_columns_or_expression,
    normalize_ch_string,
    validate_ch_columns_in_columns,
    validate_ch_options_not_used,
)
from ...connection.config import get_connection_config
from ...connection.errors import (
    InvalidSqlInputError,
    SqlOperationContext,
    annotate_sql_exception,
    sql_preview,
)
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import create_sql_table
from ...labels import apply_query_label
from ...operation_runner import tracked_sql_operation
from ...plan_steps import (
    add_create_table_placeholder_step,
    add_drop_target_steps,
    add_inspect_schema_step,
    add_insert_query_step,
)
from ...plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from ..transfer.schema import inspect_source_query_schema, map_source_schema_to_target
from .table_ops import (
    drop_ch_distributed_table_pair,
    drop_table,
    insert_from_query,
)
from .models import CreateTableFromSqlOptions
from .table_validation import normalize_key_columns, validate_key_columns_in_columns
from analytics_toolkit.general import time_print


def transfer_table(**kwargs: Any) -> int:
    from ..transfer.flow.api import transfer_table as _transfer_table

    return _transfer_table(**kwargs)


def create_table_from_sql(
    source_db: str,
    table_name: str,
    sql: str,
    *,
    table_db: str | None = None,
    insert_data: bool = False,
    drop_target_if_exists: bool = False,
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    sharding_key: str = "rand()",
    trino_insert_chunk_size: int | None = None,
    dry_run: bool = False,
    return_sql: bool = False,
    return_metadata: bool = False,
    query_label: str | None = None,
) -> int | None | SqlPlan | SqlOperationResult:
    target_table = _normalize_table_name(table_name)
    source_sql = _normalize_single_query(sql)
    source_config = get_connection_config(source_db)
    target_config = (
        source_config
        if table_db is None
        else get_connection_config(table_db)
    )
    gp_distribution = normalize_key_columns(gp_distributed_by_key)
    ch_partition = normalize_ch_columns_or_expression(
        ch_partition_by,
        "ch_partition_by",
    )
    ch_order = normalize_ch_columns_or_expression(ch_order_by, "ch_order_by")
    ch_engine_name = normalize_ch_string(ch_engine, "ch_engine")
    ch_cluster_name = normalize_ch_string(ch_cluster, "ch_cluster")
    ch_sharding_key = normalize_ch_string(sharding_key, "sharding_key")

    _validate_backend_options(
        target_backend=target_config.backend,
        gp_distributed_by_key=gp_distribution,
        ch_partition_by=ch_partition,
        ch_order_by=ch_order,
        ch_engine=ch_engine_name,
        ch_cluster=ch_cluster_name,
        ch_sharding_key=ch_sharding_key,
    )
    if trino_insert_chunk_size is not None and trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")
    options = CreateTableFromSqlOptions(
        source_key=source_config.connection_key,
        source_backend=source_config.backend,
        target_key=target_config.connection_key,
        target_backend=target_config.backend,
        target_table=target_table,
        source_sql=source_sql,
        insert_data=insert_data,
        drop_target_if_exists=drop_target_if_exists,
        gp_distributed_by_key=gp_distribution,
        ch_partition_by=ch_partition,
        ch_order_by=ch_order,
        ch_engine=ch_engine_name,
        ch_cluster=ch_cluster_name,
        ch_sharding_key=ch_sharding_key,
        trino_insert_chunk_size=trino_insert_chunk_size,
        dry_run=dry_run,
        return_sql=return_sql,
        return_metadata=return_metadata,
        query_label=query_label,
    )

    if options.dry_run or options.return_sql:
        return _build_create_table_from_sql_plan(
            source_key=options.source_key,
            source_backend=options.source_backend,
            target_key=options.target_key,
            target_backend=options.target_backend,
            target_table=options.target_table,
            source_sql=options.source_sql,
            insert_data=options.insert_data,
            drop_target_if_exists=options.drop_target_if_exists,
            ch_cluster=options.ch_cluster,
            query_label=options.query_label,
        )

    source_connection: Any | None = None
    target_connection: Any | None = None
    inserted_rows: int | None = None
    delegate_transfer = False
    operation_metadata = SqlOperationMetadata(query_label=options.query_label)

    try:
        with tracked_sql_operation(
            metadata=operation_metadata,
            operation_name="create_table_from_sql",
            alias=options.target_key,
            backend=options.target_backend,
            phase="create_or_insert",
            query_label=options.query_label,
            preview_sql=options.source_sql,
        ):
            source_connection = get_sql_connection(source_config.connection_key)
            target_connection = (
                source_connection
                if source_config.connection_key == target_config.connection_key
                else get_sql_connection(target_config.connection_key)
            )

            time_print(
                f"Inspecting source query schema on {source_config.connection_key}"
            )
            source_schema = inspect_source_query_schema(
                source_config.backend,
                source_connection,
                apply_query_label(source_sql, query_label),
            )
            source_columns = [column.name for column in source_schema]
            _validate_source_columns(source_columns)
            validate_key_columns_in_columns(gp_distribution, source_columns)
            validate_ch_columns_in_columns(
                ch_partition,
                source_columns,
                "ch_partition_by",
                data_name="source query",
            )
            validate_ch_columns_in_columns(
                ch_order,
                source_columns,
                "ch_order_by",
                data_name="source query",
            )

            target_column_types = map_source_schema_to_target(
                source_schema,
                target_config.backend,
            )
            schema_batch = pd.DataFrame(columns=source_columns)

            if drop_target_if_exists:
                if target_config.backend == "ch":
                    time_print(
                        "Dropping existing ClickHouse distributed table pair "
                        f"{target_table}"
                    )
                    drop_ch_distributed_table_pair(
                        target_connection,
                        target_table,
                        ch_cluster=ch_cluster_name,
                        query_label=query_label,
                    )
                else:
                    time_print(
                        f"Dropping existing table {target_table} "
                        f"on {target_config.connection_key}"
                    )
                    drop_table(
                        target_config.backend,
                        target_connection,
                        target_table,
                        query_label=query_label,
                    )

            create_sql_table(
                target_config.backend,
                target_connection,
                target_table,
                schema_batch,
                column_types=target_column_types,
                gp_distributed_by_key=gp_distribution,
                ch_partition_by=ch_partition,
                ch_order_by=ch_order,
                ch_engine=ch_engine_name,
                ch_cluster=ch_cluster_name,
                ch_sharding_key=ch_sharding_key,
                ch_distributed_table=target_config.backend == "ch",
                query_label=query_label,
            )

            if not insert_data:
                if return_metadata:
                    return SqlOperationResult(
                        rows=None,
                        metadata=operation_metadata,
                    )
                return None

            if source_config.backend == target_config.backend:
                inserted_rows = insert_from_query(
                    target_config.backend,
                    target_connection,
                    target_table,
                    source_sql,
                    target_column_types,
                    query_label=query_label,
                )
            else:
                delegate_transfer = True
    except Exception as exc:
        annotate_sql_exception(
            exc,
            SqlOperationContext(
                operation="create_table_from_sql",
                alias=target_config.connection_key,
                backend=target_config.backend,
                phase="create_or_insert",
                target_table=target_table,
                sql_preview=sql_preview(source_sql),
            ),
        )
        raise
    finally:
        _close_connections(
            source_connection=source_connection,
            source_key=source_config.connection_key,
            target_connection=target_connection,
            target_key=target_config.connection_key,
        )

    if delegate_transfer:
        transfer_kwargs: dict[str, object] = {
            "from_db": source_config.connection_key,
            "to_db": target_config.connection_key,
            "from_sql": source_sql,
            "to_table": target_table,
            "replace_target_table": False,
            "gp_distributed_by_key": gp_distribution,
            "trino_insert_chunk_size": trino_insert_chunk_size,
            "ch_partition_by": ch_partition,
            "ch_order_by": ch_order,
            "ch_engine": ch_engine_name,
            "ch_cluster": ch_cluster_name,
            "sharding_key": ch_sharding_key,
        }
        if query_label is not None:
            transfer_kwargs["query_label"] = query_label
        if return_metadata:
            transfer_kwargs["return_metadata"] = return_metadata
        return transfer_table(**transfer_kwargs)
    if return_metadata:
        operation_metadata.source_rows = inserted_rows
        operation_metadata.inserted_rows = inserted_rows
        operation_metadata.affected_rows = inserted_rows
        return SqlOperationResult(
            rows=inserted_rows,
            metadata=operation_metadata,
        )
    return inserted_rows


def _normalize_table_name(table_name: str) -> str:
    normalized = table_name.strip()
    if not normalized:
        raise InvalidSqlInputError("table_name must not be empty.")
    return normalized


def _build_create_table_from_sql_plan(
    *,
    source_key: str,
    source_backend: str,
    target_key: str,
    target_backend: str,
    target_table: str,
    source_sql: str,
    insert_data: bool,
    drop_target_if_exists: bool,
    ch_cluster: str,
    query_label: str | None,
) -> SqlPlan:
    plan = SqlPlan(
        operation="create_table_from_sql",
        source_alias=source_key,
        target_alias=target_key,
        source_backend=source_backend,
        target_backend=target_backend,
        target_table=target_table,
        options={
            "insert_data": insert_data,
            "drop_target_if_exists": drop_target_if_exists,
        },
    )
    add_inspect_schema_step(
        plan,
        alias=source_key,
        backend=source_backend,
        source_sql=source_sql,
        query_label=query_label,
    )
    if drop_target_if_exists:
        add_drop_target_steps(
            plan,
            alias=target_key,
            backend=target_backend,
            table_name=target_table,
            ch_cluster=ch_cluster,
            query_label=query_label,
        )
    add_create_table_placeholder_step(
        plan,
        alias=target_key,
        backend=target_backend,
        table_name=target_table,
        query_label=query_label,
    )
    if insert_data:
        add_insert_query_step(
            plan,
            alias=target_key,
            backend=target_backend,
            target_table=target_table,
            source_sql=source_sql,
            phase="insert_data",
            query_label=query_label,
        )
    return plan


def _normalize_single_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise InvalidSqlInputError("sql must not be empty.")

    statements = [
        statement.strip().rstrip(";").rstrip()
        for statement in sqlparse.split(normalized)
        if statement.strip()
    ]
    if len(statements) != 1:
        raise InvalidSqlInputError(
            "create_table_from_sql expects exactly one SQL statement."
        )
    return statements[0]


def _validate_source_columns(columns: Sequence[str]) -> None:
    if not columns:
        raise ValueError("sql must return at least one column.")
    duplicates = [column for column in columns if columns.count(column) > 1]
    if duplicates:
        duplicated_columns = ", ".join(dict.fromkeys(duplicates))
        raise ValueError(f"sql must not return duplicate columns: {duplicated_columns}")


def _validate_backend_options(
    *,
    target_backend: str,
    gp_distributed_by_key: list[str] | None,
    ch_partition_by: list[str] | str | None,
    ch_order_by: list[str] | str | None,
    ch_engine: str,
    ch_cluster: str,
    ch_sharding_key: str,
) -> None:
    if gp_distributed_by_key and target_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when table_db has type 'gp'."
        )
    validate_ch_options_not_used(
        target_backend=target_backend,
        option_owner="table_db",
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=ch_sharding_key,
    )


def _close_connections(
    *,
    source_connection: Any | None,
    source_key: str,
    target_connection: Any | None,
    target_key: str,
) -> None:
    if target_connection is not None and target_connection is not source_connection:
        time_print(f"Closing {target_key} connection")
        target_connection.close()
    if source_connection is not None:
        time_print(f"Closing {source_key} connection")
        source_connection.close()
