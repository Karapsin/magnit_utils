from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from ...capabilities import validate_write_mode
from ...connection.errors import (
    SqlOperationContext,
    annotate_sql_exception,
    sql_preview,
)
from ...ddl.create_sql_table import (
    build_ch_shard_table_name,
    build_create_table_sqls,
    column_list_sql,
    create_sql_table,
)
from ...connection.config import TrinoConfig, get_connection_config
from ...connection.get_sql_connection import get_sql_connection
from ...plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from ..transfer.runtime.retry import rollback_quietly, run_with_retry
from analytics_toolkit.general import time_print
from .load_sql_table import insert_table_batch
from .models import LoadOptions, LoadState
from .stage import create_stage_table
from ..table.table_ops import (
    analyze_table,
    build_analyze_table_sql,
    build_clear_table_sqls,
    build_count_table_rows_sql,
    build_drop_ch_distributed_table_pair_sqls,
    build_drop_table_sql,
    build_insert_from_table_sql,
    clear_ch_distributed_table_data,
    clear_target_table,
    count_table_rows,
    drop_ch_distributed_table_pair,
    drop_table,
    insert_from_table,
    get_trino_table_column_types,
    table_exists,
)
from ..table.table_validation import (
    normalize_key_columns,
    validate_key_columns_in_columns,
    validate_stage_target_key_overlap,
)


def load_df(
    connection_type: str,
    destination_table: str,
    df: pd.DataFrame,
    append: bool = False,
    write_mode: str | None = None,
    gp_distributed_by_key: list[str] | None = None,
    key_columns: list[str] | None = None,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    trino_insert_chunk_size: int | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    sharding_key: str = "rand()",
    dry_run: bool = False,
    return_sql: bool = False,
    return_metadata: bool = False,
    query_label: str | None = None,
) -> int | SqlPlan | SqlOperationResult:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if retry_cnt < 1:
        raise ValueError("retry_cnt must be at least 1.")
    if timeout_increment < 0:
        raise ValueError("timeout_increment must be non-negative.")

    options = _build_load_options(
        connection_type=connection_type,
        destination_table=destination_table,
        append=append,
        write_mode=write_mode,
        gp_distributed_by_key=gp_distributed_by_key,
        key_columns=key_columns,
        trino_insert_chunk_size=trino_insert_chunk_size,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=sharding_key,
        query_label=query_label,
    )

    if dry_run or return_sql:
        return build_load_df_plan(options, df)

    def operation(attempt: int) -> int | SqlOperationResult:
        connection_ref = {"connection": get_sql_connection(options.connection_key)}
        state: LoadState | None = None
        try:
            state = LoadState(
                target_exists=table_exists(
                    options.connection_backend,
                    connection_ref["connection"],
                    options.destination_table,
                    connection_key=options.connection_key,
                )
            )

            if df.empty:
                if options.append and state.target_exists:
                    time_print(
                        f"Skipping empty DataFrame append into "
                        f"{options.connection_key}.{options.destination_table}"
                    )
                    if return_metadata:
                        return SqlOperationResult(
                            rows=0,
                            metadata=SqlOperationMetadata(
                                source_rows=0,
                                inserted_rows=0,
                                affected_rows=0,
                            ),
                        )
                    return 0
                raise ValueError("Cannot create or replace a table from an empty DataFrame.")

            if options.gp_distributed_by_key:
                validate_key_columns_in_columns(options.gp_distributed_by_key, df.columns)

            validate_key_columns_in_columns(options.key_columns, df.columns)
            _validate_ch_columns_in_dataframe(
                options.ch_partition_by,
                df.columns,
                "ch_partition_by",
            )
            _validate_ch_columns_in_dataframe(
                options.ch_order_by,
                df.columns,
                "ch_order_by",
            )
            _validate_dataframe_key_uniqueness(df, options.key_columns)

            if options.write_mode == "replace":
                if options.connection_backend == "ch":
                    time_print(
                        "Dropping existing ClickHouse distributed table pair "
                        f"{options.destination_table}"
                    )
                    drop_ch_distributed_table_pair(
                        connection_ref["connection"],
                        options.destination_table,
                        ch_cluster=options.ch_cluster,
                        query_label=options.query_label,
                    )
                    state.target_exists = False
                elif state.target_exists:
                    time_print(
                        f"Dropping existing table {options.destination_table} "
                        f"on {options.connection_key}"
                    )
                    drop_table(
                        options.connection_backend,
                        connection_ref["connection"],
                        options.destination_table,
                        query_label=options.query_label,
                    )
                    state.target_exists = False
            elif options.write_mode == "truncate_insert" and state.target_exists:
                if options.connection_backend == "ch":
                    clear_ch_distributed_table_data(
                        connection_ref["connection"],
                        options.destination_table,
                        ch_cluster=options.ch_cluster,
                        query_label=options.query_label,
                    )
                else:
                    clear_target_table(
                        options.connection_backend,
                        connection_ref["connection"],
                        options.destination_table,
                        query_label=options.query_label,
                    )

            if options.connection_backend == "ch":
                create_kwargs: dict[str, Any] = {}
                if options.query_label is not None:
                    create_kwargs["query_label"] = options.query_label
                create_sql_table(
                    options.connection_backend,
                    connection_ref["connection"],
                    options.destination_table,
                    df,
                    gp_distributed_by_key=options.gp_distributed_by_key,
                    ch_partition_by=options.ch_partition_by,
                    ch_order_by=options.ch_order_by,
                    ch_engine=options.ch_engine,
                    ch_cluster=options.ch_cluster,
                    ch_sharding_key=options.ch_sharding_key,
                    ch_distributed_table=True,
                    **create_kwargs,
                )
                state.target_exists = True
            elif not state.target_exists:
                create_kwargs = {}
                if options.query_label is not None:
                    create_kwargs["query_label"] = options.query_label
                create_sql_table(
                    options.connection_backend,
                    connection_ref["connection"],
                    options.destination_table,
                    df,
                    gp_distributed_by_key=options.gp_distributed_by_key,
                    **create_kwargs,
                )

            if options.connection_backend == "trino":
                state.target_column_types = get_trino_table_column_types(
                    connection_ref["connection"],
                    options.destination_table,
                    connection_key=options.connection_key,
                )

            inserted_rows = _load_dataframe(
                options=options,
                state=state,
                connection_ref=connection_ref,
                df=df,
            )

            if options.query_label is None:
                analyze_table(
                    connection_type=options.connection_backend,
                    connection=connection_ref["connection"],
                    table_name=options.destination_table,
                )
            else:
                analyze_table(
                    connection_type=options.connection_backend,
                    connection=connection_ref["connection"],
                    table_name=options.destination_table,
                    query_label=options.query_label,
                )
            time_print(
                f"Finished loading DataFrame into "
                f"{options.connection_key}.{options.destination_table}: "
                f"{inserted_rows} row(s)"
            )
            if return_metadata:
                return SqlOperationResult(
                    rows=inserted_rows,
                    metadata=_build_load_metadata(
                        options=options,
                        state=state,
                        connection=connection_ref["connection"],
                        source_rows=len(df),
                        inserted_rows=inserted_rows,
                    ),
                )
            return inserted_rows
        except Exception as exc:
            annotate_sql_exception(
                exc,
                SqlOperationContext(
                    operation="load_df",
                    alias=options.connection_key,
                    backend=options.connection_backend,
                    phase="load",
                    target_table=options.destination_table,
                    retry_attempt=attempt,
                    sql_preview=sql_preview(options.destination_table),
                ),
            )
            if options.connection_backend == "gp":
                rollback_quietly(connection_ref["connection"])
            raise
        finally:
            _cleanup_load(connection_ref, options, state)

    return run_with_retry(
        operation_name=(
            f"loading DataFrame into {options.connection_key}.{options.destination_table}"
        ),
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def _build_load_options(
    connection_type: str,
    destination_table: str,
    append: bool,
    write_mode: str | None,
    gp_distributed_by_key: list[str] | None,
    key_columns: list[str] | None,
    trino_insert_chunk_size: int | None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> LoadOptions:
    config = get_connection_config(connection_type)
    configured_trino_insert_chunk_size = (
        config.insert_chunk_size if isinstance(config, TrinoConfig) else None
    )
    resolved_write_mode = _resolve_load_write_mode(
        config.backend,
        append=append,
        write_mode=write_mode,
    )
    options = LoadOptions(
        connection_key=config.connection_key,
        connection_backend=config.backend,
        destination_table=destination_table.strip(),
        append=resolved_write_mode == "append",
        write_mode=resolved_write_mode,
        gp_distributed_by_key=_normalize_gp_distributed_by_key(gp_distributed_by_key),
        key_columns=normalize_key_columns(key_columns),
        trino_insert_chunk_size=(
            trino_insert_chunk_size
            if trino_insert_chunk_size is not None
            else configured_trino_insert_chunk_size
        ),
        ch_partition_by=_normalize_ch_columns_or_expression(
            ch_partition_by,
            "ch_partition_by",
        ),
        ch_order_by=_normalize_ch_columns_or_expression(ch_order_by, "ch_order_by"),
        ch_engine=_normalize_ch_string(ch_engine, "ch_engine"),
        ch_cluster=_normalize_ch_string(ch_cluster, "ch_cluster"),
        ch_sharding_key=_normalize_ch_string(ch_sharding_key, "sharding_key"),
        query_label=query_label,
    )

    if not options.destination_table:
        raise ValueError("destination_table must not be empty.")
    if options.gp_distributed_by_key and options.connection_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when connection_type has type 'gp'."
        )
    if options.trino_insert_chunk_size is not None and options.trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")
    if options.connection_backend != "ch":
        _validate_ch_options_not_used(options)
    return options


def _resolve_load_write_mode(
    connection_backend: str,
    *,
    append: bool,
    write_mode: str | None,
) -> str:
    if write_mode is None:
        return "append" if append else "replace"

    normalized = validate_write_mode(connection_backend, write_mode)
    if append and normalized != "append":
        raise ValueError("append=True cannot be combined with write_mode other than 'append'.")
    return normalized


def build_load_df_plan(options: LoadOptions, df: pd.DataFrame) -> SqlPlan:
    metadata = SqlOperationMetadata(
        source_rows=len(df),
        staged_rows=len(df) if options.append and options.key_columns else None,
        inserted_rows=len(df),
        affected_rows=len(df),
    )
    plan = SqlPlan(
        operation="load_df",
        target_alias=options.connection_key,
        target_backend=options.connection_backend,
        target_table=options.destination_table,
        options={
            "write_mode": options.write_mode,
            "append": options.append,
            "key_columns": options.key_columns,
            "gp_distributed_by_key": options.gp_distributed_by_key,
            "trino_insert_chunk_size": options.trino_insert_chunk_size,
            "ch_partition_by": options.ch_partition_by,
            "ch_order_by": options.ch_order_by,
            "ch_engine": options.ch_engine,
            "ch_cluster": options.ch_cluster,
            "ch_sharding_key": options.ch_sharding_key,
        },
        metadata=metadata,
    )

    if df.empty:
        return plan

    if options.write_mode == "replace":
        if options.connection_backend == "ch":
            plan.extend(
                build_drop_ch_distributed_table_pair_sqls(
                    options.destination_table,
                    ch_cluster=options.ch_cluster,
                    query_label=options.query_label,
                ),
                alias=options.connection_key,
                backend=options.connection_backend,
                phase="drop_target",
                target_table=options.destination_table,
            )
        else:
            plan.add(
                build_drop_table_sql(
                    options.connection_backend,
                    options.destination_table,
                    query_label=options.query_label,
                ),
                alias=options.connection_key,
                backend=options.connection_backend,
                phase="drop_target",
                target_table=options.destination_table,
            )
    elif options.write_mode == "truncate_insert":
        for sql in _build_load_truncate_sqls(options):
            plan.add(
                sql,
                alias=options.connection_key,
                backend=options.connection_backend,
                phase="clear_target",
                target_table=options.destination_table,
            )

    if options.write_mode in {"replace", "truncate_insert"} or options.connection_backend == "ch":
        plan.extend(
            build_create_table_sqls(
                options.connection_backend,
                options.destination_table,
                df,
                gp_distributed_by_key=options.gp_distributed_by_key,
                ch_partition_by=options.ch_partition_by,
                ch_order_by=options.ch_order_by,
                ch_engine=options.ch_engine,
                ch_cluster=options.ch_cluster,
                ch_sharding_key=options.ch_sharding_key,
                ch_distributed_table=options.connection_backend == "ch",
                query_label=options.query_label,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="create_target",
            target_table=options.destination_table,
        )

    if options.append and options.key_columns:
        stage_table = f"{options.destination_table}__stage__dry_run"
        metadata.stage_table = stage_table
        plan.extend(
            build_create_table_sqls(
                options.connection_backend,
                stage_table,
                df,
                gp_distributed_by_key=options.gp_distributed_by_key,
                query_label=options.query_label,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="create_stage",
            target_table=stage_table,
        )
        plan.add(
            _build_dataframe_insert_placeholder(
                options.connection_backend,
                stage_table,
                df,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="load_stage",
            target_table=stage_table,
            query_label=options.query_label,
        )
        plan.add(
            build_insert_from_table_sql(
                options.connection_backend,
                options.destination_table,
                stage_table,
                query_label=options.query_label,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="insert_from_stage",
            target_table=options.destination_table,
            source_table=stage_table,
        )
        plan.add(
            build_drop_table_sql(
                options.connection_backend,
                stage_table,
                query_label=options.query_label,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="drop_stage",
            target_table=stage_table,
        )
    else:
        plan.add(
            _build_dataframe_insert_placeholder(
                options.connection_backend,
                options.destination_table,
                df,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="load_data",
            target_table=options.destination_table,
            query_label=options.query_label,
        )

    if options.connection_backend != "ch":
        plan.add(
            build_analyze_table_sql(
                options.connection_backend,
                options.destination_table,
                query_label=options.query_label,
            ),
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="analyze",
            target_table=options.destination_table,
        )
    plan.add(
        build_count_table_rows_sql(
            options.connection_backend,
            options.destination_table,
            query_label=options.query_label,
        ),
        alias=options.connection_key,
        backend=options.connection_backend,
        phase="count_target",
        target_table=options.destination_table,
    )
    return plan


def _build_load_truncate_sqls(options: LoadOptions) -> list[str]:
    if options.connection_backend != "ch":
        return build_clear_table_sqls(
            options.connection_backend,
            options.destination_table,
            query_label=options.query_label,
        )
    shard_table = build_ch_shard_table_name(options.destination_table)
    return [
        *build_clear_table_sqls(
            "ch",
            shard_table,
            query_label=options.query_label,
        ),
        *build_clear_table_sqls(
            "ch",
            options.destination_table,
            query_label=options.query_label,
        ),
    ]


def _build_dataframe_insert_placeholder(
    connection_backend: str,
    table_name: str,
    df: pd.DataFrame,
) -> str:
    columns = column_list_sql([str(column) for column in df.columns], connection_backend)
    row_word = "row" if len(df) == 1 else "rows"
    return f"INSERT INTO {table_name} ({columns}) VALUES <{len(df)} dataframe {row_word}>"


def _build_load_metadata(
    *,
    options: LoadOptions,
    state: LoadState,
    connection: Any,
    source_rows: int,
    inserted_rows: int,
) -> SqlOperationMetadata:
    metadata = SqlOperationMetadata(
        source_rows=source_rows,
        staged_rows=source_rows if state.overlap_stage_table is not None else None,
        inserted_rows=inserted_rows,
        affected_rows=inserted_rows,
        stage_table=state.overlap_stage_table,
    )
    try:
        metadata.final_target_rows = count_table_rows(
            options.connection_backend,
            connection,
            options.destination_table,
            query_label=options.query_label,
        )
    except Exception:
        metadata.final_target_rows = None
    return metadata


def _load_dataframe(
    options: LoadOptions,
    state: LoadState,
    connection_ref: dict[str, Any],
    df: pd.DataFrame,
) -> int:
    if options.append and state.target_exists and options.key_columns:
        state.overlap_stage_table = create_stage_table(
            connection_type=options.connection_backend,
            connection=connection_ref["connection"],
            target_table=options.destination_table,
            batch=df,
            gp_distributed_by_key=options.gp_distributed_by_key,
            connection_key=options.connection_key,
            query_label=options.query_label,
        )
        insert_table_batch(
            options.connection_backend,
            connection_ref,
            state.overlap_stage_table,
            df,
            retry_fn=run_with_retry,
            retry_cnt=1,
            timeout_increment=0,
            target_column_types=state.target_column_types,
            trino_insert_chunk_size=options.trino_insert_chunk_size,
            query_label=options.query_label,
        )
        validate_stage_target_key_overlap(
            connection_type=options.connection_backend,
            connection=connection_ref["connection"],
            stage_table=state.overlap_stage_table,
            target_table=options.destination_table,
            key_columns=options.key_columns,
            target_exists=state.target_exists,
            replace_target_table=False,
        )
        insert_from_table(
            options.connection_backend,
            connection_ref["connection"],
            options.destination_table,
            state.overlap_stage_table,
            query_label=options.query_label,
        )
        return len(df)

    return insert_table_batch(
        options.connection_backend,
        connection_ref,
        options.destination_table,
        df,
        retry_fn=run_with_retry,
        retry_cnt=1,
        timeout_increment=0,
        target_column_types=state.target_column_types,
        trino_insert_chunk_size=options.trino_insert_chunk_size,
        query_label=options.query_label,
    )


def _cleanup_load(
    connection_ref: dict[str, Any],
    options: LoadOptions,
    state: LoadState | None,
) -> None:
    if state is not None and state.overlap_stage_table is not None:
        try:
            drop_table(
                options.connection_backend,
                connection_ref["connection"],
                state.overlap_stage_table,
                query_label=options.query_label,
            )
        except Exception:
            time_print(
                f"Failed to drop temporary load_df stage table {state.overlap_stage_table}"
            )
    time_print(f"Closing {options.connection_key} connection")
    connection_ref["connection"].close()


def _normalize_gp_distributed_by_key(
    gp_distributed_by_key: list[str] | None,
) -> list[str] | None:
    if gp_distributed_by_key is None:
        return None

    normalized = [column.strip() for column in gp_distributed_by_key]
    if not normalized:
        raise ValueError("gp_distributed_by_key must not be empty when provided.")
    if any(not column for column in normalized):
        raise ValueError("gp_distributed_by_key must not contain empty column names.")
    if len(set(normalized)) != len(normalized):
        raise ValueError("gp_distributed_by_key must not contain duplicate column names.")
    return normalized


def _normalize_ch_columns_or_expression(
    value: Sequence[str] | str | None,
    option_name: str,
) -> list[str] | str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_ch_string(value, option_name)

    normalized = [_normalize_ch_string(column, option_name) for column in value]
    if not normalized:
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    return normalized


def _normalize_ch_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def _validate_ch_options_not_used(options: LoadOptions) -> None:
    if options.ch_partition_by is not None:
        raise ValueError(
            "ch_partition_by can only be used when connection_type has type 'ch'."
        )
    if options.ch_order_by is not None:
        raise ValueError(
            "ch_order_by can only be used when connection_type has type 'ch'."
        )
    if options.ch_engine != "ReplicatedMergeTree":
        raise ValueError(
            "ch_engine can only be used when connection_type has type 'ch'."
        )
    if options.ch_cluster != "{cluster}":
        raise ValueError(
            "ch_cluster can only be used when connection_type has type 'ch'."
        )
    if options.ch_sharding_key != "rand()":
        raise ValueError(
            "sharding_key can only be used when connection_type has type 'ch'."
        )


def _validate_ch_columns_in_dataframe(
    value: list[str] | str | None,
    columns: Sequence[str],
    option_name: str,
) -> None:
    if value is None or isinstance(value, str):
        return

    available_columns = {str(column) for column in columns}
    missing_columns = [column for column in value if column not in available_columns]
    if missing_columns:
        raise ValueError(
            f"{option_name} columns were not found in the staged data: "
            + ", ".join(missing_columns)
        )


def _validate_dataframe_key_uniqueness(
    df: pd.DataFrame,
    key_columns: list[str] | None,
) -> None:
    if not key_columns:
        return

    if df.duplicated(subset=key_columns, keep=False).any():
        raise ValueError(
            "Duplicate key values found in DataFrame for key_columns: "
            + ", ".join(key_columns)
        )
