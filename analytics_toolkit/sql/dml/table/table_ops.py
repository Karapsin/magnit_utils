from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from ...backend_adapters import (
    ch_cluster_clause,
    extract_row_count,
    format_ch_cluster_name,
    format_gp_information_schema_type,
    get_backend_adapter,
    is_simple_identifier,
    split_gp_table_name,
    split_trino_table_name as _adapter_split_trino_table_name,
)
from ...ch_lifecycle import (
    build_drop_ch_distributed_table_pair_sqls as _build_ch_pair_drop_sqls,
    drop_ch_distributed_table_pair as _drop_ch_pair,
    truncate_ch_distributed_table_pair as _truncate_ch_pair,
)
from ...connection.config import (
    get_connection_config,
    resolve_connection_backend,
)
from ...connection.get_sql_connection import get_sql_connection
from ...ddl.create_sql_table import create_sql_table
from ...ddl.create_sql_table import quote_identifier
from ...connection.errors import InvalidSqlInputError, UnsupportedConnectionTypeError
from ...labels import apply_query_label
from analytics_toolkit.general import time_print


def table_exists(
    connection_type: str,
    connection: Any,
    table_name: str,
    connection_key: str | None = None,
) -> bool:
    backend = resolve_connection_backend(connection_type)
    return get_backend_adapter(backend).table_exists(
        connection,
        table_name,
        connection_key=connection_key or connection_type,
    )


def build_clear_table_sqls(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> list[str]:
    return get_backend_adapter(connection_type).clear_table_sqls(
        table_name,
        query_label=query_label,
    )


def build_drop_table_sql(
    connection_type: str,
    table_name: str,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> str:
    return get_backend_adapter(connection_type).drop_table_sql(
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def build_drop_ch_distributed_table_pair_sqls(
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> list[str]:
    return _build_ch_pair_drop_sqls(
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def build_analyze_table_sql(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    return get_backend_adapter(backend).analyze_table_sql(
        table_name,
        query_label=query_label,
    )


def clear_target_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> None:
    time_print(f"Clearing target table {table_name} on {connection_type}")
    backend = resolve_connection_backend(connection_type)
    get_backend_adapter(backend).clear_table(
        connection,
        table_name,
        query_label=query_label,
    )


def apply_target_write_mode(
    connection_type: str,
    connection: Any,
    table_name: str,
    *,
    write_mode: str,
    target_exists: bool,
    replace_existing_non_ch: str,
    ch_cluster: str = "{cluster}",
    connection_label: str | None = None,
    drop_missing_ch_truncate_target: bool = True,
    query_label: str | None = None,
) -> bool:
    backend = resolve_connection_backend(connection_type)
    log_connection = connection_label or connection_type
    if write_mode == "append":
        return target_exists

    if backend == "ch":
        if write_mode == "truncate_insert" and target_exists:
            clear_ch_distributed_table_data(
                connection,
                table_name,
                ch_cluster=ch_cluster,
                query_label=query_label,
            )
            return True
        if write_mode == "truncate_insert" and not drop_missing_ch_truncate_target:
            return False

        time_print(
            "Dropping existing ClickHouse distributed table pair "
            f"{table_name}"
        )
        drop_ch_distributed_table_pair(
            connection,
            table_name,
            ch_cluster=ch_cluster,
            query_label=query_label,
        )
        return False

    if not target_exists:
        return False

    if write_mode == "truncate_insert" or replace_existing_non_ch == "clear":
        clear_target_table(
            backend,
            connection,
            table_name,
            query_label=query_label,
        )
        return True

    if replace_existing_non_ch == "drop":
        time_print(f"Dropping existing table {table_name} on {log_connection}")
        drop_table(
            backend,
            connection,
            table_name,
            query_label=query_label,
        )
        return False

    raise ValueError("replace_existing_non_ch must be one of: clear, drop.")


def finalize_stage_table(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    replace_target_table: bool,
    target_exists: bool,
    sample_batch: pd.DataFrame,
    target_column_types: Mapping[str, str] | None = None,
    insert_column_types: Mapping[str, str] | None = None,
    write_mode: str = "replace",
    gp_distributed_by_key: list[str] | None = None,
    ch_partition_by: list[str] | str | None = None,
    ch_order_by: list[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> None:
    time_print(
        f"Finalizing staged transfer from {stage_table} into {target_table} on {connection_type}"
    )
    backend = resolve_connection_backend(connection_type)

    if replace_target_table:
        target_exists = apply_target_write_mode(
            backend,
            connection,
            target_table,
            write_mode=write_mode,
            target_exists=target_exists,
            replace_existing_non_ch="clear",
            ch_cluster=ch_cluster,
            query_label=query_label,
        )

    if backend == "ch":
        if not target_exists:
            create_sql_table(
                connection_type,
                connection,
                target_table,
                sample_batch,
                column_types=target_column_types,
                gp_distributed_by_key=gp_distributed_by_key,
                ch_partition_by=ch_partition_by,
                ch_order_by=ch_order_by,
                ch_engine=ch_engine,
                ch_cluster=ch_cluster,
                ch_sharding_key=ch_sharding_key,
                ch_distributed_table=True,
                query_label=query_label,
            )
        insert_from_table(
            backend,
            connection,
            target_table,
            stage_table,
            column_types=insert_column_types,
            query_label=query_label,
        )
        return

    if not target_exists:
        create_sql_table(
            backend,
            connection,
            target_table,
            sample_batch,
            column_types=target_column_types,
            gp_distributed_by_key=gp_distributed_by_key,
            query_label=query_label,
        )

    insert_from_table(
        backend,
        connection,
        target_table,
        stage_table,
        column_types=insert_column_types,
        query_label=query_label,
    )


def analyze_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    if backend == "ch":
        return

    time_print(f"Analyzing target table {table_name} on {connection_type}")
    get_backend_adapter(backend).analyze_table(
        connection,
        table_name,
        query_label=query_label,
    )


def gp_vacuum(
    table_name: str,
    analyze: bool = False,
    full: bool = False,
    verbose: bool = True,
    connection_key: str = "gp",
) -> None:
    config = get_connection_config(connection_key)
    if config.backend != "gp":
        raise UnsupportedConnectionTypeError(
            f"gp_vacuum requires a gp connection, got '{config.backend}'."
        )

    conn = get_sql_connection(config.connection_key)
    qualified_table_name = quote_qualified_table_name(table_name, "gp")
    options: list[str] = []
    if full:
        options.append("FULL")
    if verbose:
        options.append("VERBOSE")
    if analyze:
        options.append("ANALYZE")

    options_sql = f" ({', '.join(options)})" if options else ""
    sql = f"VACUUM{options_sql} {qualified_table_name}"

    time_print(f"Vacuuming table {qualified_table_name} on gp")
    try:
        previous_autocommit = conn.autocommit
        cursor = conn.cursor()
        try:
            conn.autocommit = True
            cursor.execute(sql)
        finally:
            cursor.close()
            conn.autocommit = previous_autocommit
    finally:
        time_print(f"Closing {config.connection_key} connection")
        conn.close()


def drop_table_with_retry(
    connection_backend: str,
    connection_key: str,
    connection_ref: dict[str, Any],
    table_name: str,
    retry_fn: Any,
    retry_cnt: int,
    timeout_increment: int | float,
    rollback_fn: Any,
    replace_connection_fn: Any,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_backend)

    def operation(attempt: int) -> None:
        connection = connection_ref["connection"]
        try:
            drop_table(backend, connection, table_name, query_label=query_label)
            return None
        except Exception:
            if backend == "gp":
                rollback_fn(connection)
            replace_connection_fn(connection_key, connection_ref)
            raise

    retry_fn(
        operation_name=f"dropping stage table {table_name} on {connection_key}",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def drop_table(
    connection_type: str,
    connection: Any,
    table_name: str,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    get_backend_adapter(backend).drop_table(
        connection,
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def drop_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> None:
    _drop_ch_pair(
        connection,
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def clear_ch_distributed_table_data(
    connection: Any,
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> None:
    _truncate_ch_pair(
        connection,
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def get_trino_table_column_types(
    connection: Any,
    table_name: str,
    connection_key: str = "trino",
) -> dict[str, str]:
    return get_backend_adapter("trino").get_table_column_types(
        connection,
        table_name,
        connection_key=connection_key,
    )


def get_table_column_types(
    connection_type: str,
    connection: Any,
    table_name: str,
    connection_key: str | None = None,
) -> dict[str, str]:
    backend = resolve_connection_backend(connection_type)
    return get_backend_adapter(backend).get_table_column_types(
        connection,
        table_name,
        connection_key=connection_key or connection_type,
    )


def _get_gp_table_column_types(connection: Any, table_name: str) -> dict[str, str]:
    return get_backend_adapter("gp").get_table_column_types(
        connection,
        table_name,
        connection_key="gp",
    )


def _split_gp_table_name(table_name: str) -> tuple[str, str]:
    return split_gp_table_name(table_name)


def _format_gp_information_schema_type(
    data_type: str,
    udt_name: Any,
    numeric_precision: Any,
    numeric_scale: Any,
) -> str:
    return format_gp_information_schema_type(
        data_type,
        udt_name,
        numeric_precision,
        numeric_scale,
    )


def _get_ch_table_column_types(connection: Any, table_name: str) -> dict[str, str]:
    return get_backend_adapter("ch").get_table_column_types(
        connection,
        table_name,
        connection_key="ch",
    )


def insert_from_table(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None = None,
    query_label: str | None = None,
) -> None:
    backend = resolve_connection_backend(connection_type)
    get_backend_adapter(backend).insert_from_table(
        connection,
        target_table,
        source_table,
        column_types=column_types,
        query_label=query_label,
    )


def insert_from_query(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_sql: str,
    column_types: Mapping[str, str],
    query_label: str | None = None,
) -> int:
    backend = resolve_connection_backend(connection_type)
    return get_backend_adapter(backend).insert_from_query(
        connection,
        target_table,
        source_sql,
        column_types,
        query_label=query_label,
    )


def build_insert_from_query_sql(
    connection_type: str,
    target_table: str,
    source_sql: str,
    column_types: Mapping[str, str],
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    return apply_query_label(
        get_backend_adapter(backend).build_insert_from_query_sql(
            target_table,
            source_sql,
            column_types,
        ),
        query_label,
    )


def build_insert_from_table_sql(
    connection_type: str,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None = None,
    query_label: str | None = None,
) -> str:
    backend = resolve_connection_backend(connection_type)
    return apply_query_label(
        get_backend_adapter(backend).build_insert_from_table_sql(
            target_table,
            source_table,
            column_types,
        ),
        query_label,
    )


def count_table_rows(
    connection_type: str,
    connection: Any,
    table_name: str,
    query_label: str | None = None,
) -> int:
    backend = resolve_connection_backend(connection_type)
    return get_backend_adapter(backend).count_table_rows(
        connection,
        table_name,
        query_label=query_label,
    )


def build_count_table_rows_sql(
    connection_type: str,
    table_name: str,
    query_label: str | None = None,
) -> str:
    return get_backend_adapter(connection_type).count_table_rows_sql(
        table_name,
        query_label=query_label,
    )


def _build_insert_from_table_sql(
    connection_type: str,
    target_table: str,
    source_table: str,
    column_types: Mapping[str, str] | None,
) -> str:
    return get_backend_adapter(connection_type).build_insert_from_table_sql(
        target_table,
        source_table,
        column_types,
    )


def _build_typed_insert_select_sql(
    connection_type: str,
    target_table: str,
    from_sql: str,
    column_types: Mapping[str, str],
) -> str:
    return get_backend_adapter(connection_type)._build_typed_insert_select_sql(
        target_table,
        from_sql,
        column_types,
    )


def _cast_select_expression(
    connection_type: str,
    column_name: str,
    target_type: str,
) -> str:
    return get_backend_adapter(connection_type).cast_select_expression(
        column_name,
        target_type,
    )


def _extract_row_count(executed: Any) -> int:
    return extract_row_count(executed)


def _extract_row_count_from_mapping(value: Mapping[str, Any]) -> int | None:
    for key in (
        "rowcount",
        "row_count",
        "written_rows",
        "writtenRows",
        "processedRows",
        "rows",
    ):
        row_count = _coerce_row_count(value.get(key))
        if row_count is not None:
            return row_count
    return None


def _coerce_row_count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        row_count = int(value)
    except (TypeError, ValueError):
        return None
    if row_count < 0:
        return None
    return row_count


def split_trino_table_name(
    table_name: str,
    connection_key: str = "trino",
) -> tuple[str, str, str]:
    return _adapter_split_trino_table_name(table_name, connection_key=connection_key)


def quote_qualified_table_name(table_name: str, connection_type: str) -> str:
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or any(not part for part in parts):
        raise InvalidSqlInputError("Table name must be a non-empty identifier.")
    if len(parts) > 3:
        raise InvalidSqlInputError(
            "Table name must be unqualified or dot-qualified up to three parts."
        )
    return ".".join(quote_identifier(part, connection_type) for part in parts)


def _truncate_ch_table(
    connection: Any,
    table_name: str,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> None:
    _execute_ch_command(
        connection,
        apply_query_label(
            f"TRUNCATE TABLE IF EXISTS {table_name}{_ch_cluster_clause(ch_cluster)}",
            query_label,
        ),
    )


def _ch_cluster_clause(ch_cluster: str | None) -> str:
    return ch_cluster_clause(ch_cluster)


def _format_ch_cluster_name(cluster_name: str) -> str:
    return format_ch_cluster_name(cluster_name)


def _is_simple_identifier(identifier: str) -> bool:
    return is_simple_identifier(identifier)


def _execute_ch_command(connection: Any, sql: str) -> None:
    get_backend_adapter("ch").execute_command(connection, sql)


def _gp_table_exists(connection: Any, table_name: str) -> bool:
    return get_backend_adapter("gp").table_exists(
        connection,
        table_name,
        connection_key="gp",
    )


def _trino_table_exists(
    connection: Any,
    table_name: str,
    connection_key: str,
) -> bool:
    return get_backend_adapter("trino").table_exists(
        connection,
        table_name,
        connection_key=connection_key,
    )


def _ch_table_exists(client: Any, table_name: str) -> bool:
    return get_backend_adapter("ch").table_exists(
        client,
        table_name,
        connection_key="ch",
    )
