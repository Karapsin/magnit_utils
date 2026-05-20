from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
from tqdm import tqdm

from ...capabilities import validate_write_mode
from ...ch_options import (
    normalize_ch_columns_or_expression,
    normalize_ch_string,
    validate_ch_columns_in_columns,
    validate_ch_options_not_used,
)
from ...connection.errors import (
    SqlOperationContext,
    sql_preview,
)
from ...ddl.create_sql_table import (
    build_create_table_sqls,
    column_list_sql,
    create_sql_table,
)
from ...connection.config import TrinoConfig, get_connection_config
from ...connection.get_sql_connection import get_sql_connection
from ...operation_runner import run_connection_operation, tracked_sql_operation
from ...plan_steps import (
    add_analyze_step,
    add_clear_target_steps,
    add_count_step,
    add_cleanup_stage_step,
    add_create_table_steps,
    add_drop_target_steps,
    add_insert_from_stage_step,
    add_load_stage_step,
)
from ...plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from ..transfer.runtime.retry import run_with_retry
from analytics_toolkit.general import time_print
from .load_sql_table import insert_table_batch
from .models import LoadOptions, LoadState
from .stage import create_stage_table
from ..table.table_ops import (
    analyze_table,
    apply_target_write_mode,
    count_table_rows,
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
    gp_insert_chunk_size: int | None = None,
    progress: bool = True,
) -> int | SqlPlan | SqlOperationResult:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if retry_cnt < 1:
        raise ValueError("retry_cnt must be at least 1.")
    if timeout_increment < 0:
        raise ValueError("timeout_increment must be non-negative.")
    _validate_progress(progress)

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
        gp_insert_chunk_size=gp_insert_chunk_size,
    )

    if dry_run or return_sql:
        return build_load_df_plan(options, df)

    state_holder: dict[str, LoadState | None] = {"state": None}
    operation_metadata = SqlOperationMetadata(
        source_rows=len(df),
        query_label=options.query_label,
    )

    def operation(
        connection_ref: dict[str, Any],
        attempt: int,
    ) -> int | SqlOperationResult:
        with tracked_sql_operation(
            metadata=operation_metadata,
            operation_name="load_df",
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="load",
            retry_attempt=attempt,
            query_label=options.query_label,
        ):
            state_holder["state"] = None
            state = _initialize_load_state(options, connection_ref["connection"])
            state_holder["state"] = state
            if df.empty:
                return _handle_empty_dataframe_load(
                    options,
                    state,
                    operation_metadata=operation_metadata,
                    return_metadata=return_metadata,
                )

            _validate_load_dataframe(options, df)
            _prepare_load_target(
                options=options,
                state=state,
                connection=connection_ref["connection"],
                df=df,
            )

            progress_bar = _make_load_progress_bar(
                total=len(df),
                options=options,
                progress=progress,
            )
            progress_tracker = _ProgressTracker(progress_bar)
            try:
                inserted_rows = _load_dataframe(
                    options=options,
                    state=state,
                    connection_ref=connection_ref,
                    df=df,
                    on_progress=progress_tracker.update,
                )
                progress_tracker.complete_to(inserted_rows)
            finally:
                progress_bar.close()

            _analyze_load_target(options, connection_ref["connection"])
            time_print(
                f"Finished loading DataFrame into "
                f"{options.connection_key}.{options.destination_table}: "
                f"{inserted_rows} row(s)"
            )
            return _build_load_result(
                options=options,
                state=state,
                connection=connection_ref["connection"],
                source_rows=len(df),
                inserted_rows=inserted_rows,
                operation_metadata=operation_metadata,
                return_metadata=return_metadata,
            )

    def context(attempt: int) -> SqlOperationContext:
        return SqlOperationContext(
            operation="load_df",
            alias=options.connection_key,
            backend=options.connection_backend,
            phase="load",
            target_table=options.destination_table,
            retry_attempt=attempt,
            sql_preview=sql_preview(options.destination_table),
        )

    def cleanup(connection_ref: dict[str, Any]) -> None:
        _cleanup_load(connection_ref, options, state_holder["state"])

    return run_connection_operation(
        operation_name=(
            f"loading DataFrame into {options.connection_key}.{options.destination_table}"
        ),
        connection_key=options.connection_key,
        backend=options.connection_backend,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        open_connection=get_sql_connection,
        operation=operation,
        context_factory=context,
        cleanup=cleanup,
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
    gp_insert_chunk_size: int | None = None,
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
        ch_partition_by=normalize_ch_columns_or_expression(
            ch_partition_by,
            "ch_partition_by",
        ),
        ch_order_by=normalize_ch_columns_or_expression(ch_order_by, "ch_order_by"),
        ch_engine=normalize_ch_string(ch_engine, "ch_engine"),
        ch_cluster=normalize_ch_string(ch_cluster, "ch_cluster"),
        ch_sharding_key=normalize_ch_string(ch_sharding_key, "sharding_key"),
        query_label=query_label,
        gp_insert_chunk_size=gp_insert_chunk_size,
    )

    if not options.destination_table:
        raise ValueError("destination_table must not be empty.")
    if options.gp_distributed_by_key and options.connection_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when connection_type has type 'gp'."
        )
    if options.gp_insert_chunk_size is not None:
        if options.connection_backend != "gp":
            raise ValueError(
                "gp_insert_chunk_size can only be used when connection_type has type 'gp'."
            )
        if options.gp_insert_chunk_size <= 0:
            raise ValueError("gp_insert_chunk_size must be a positive integer.")
    if options.trino_insert_chunk_size is not None and options.trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")
    validate_ch_options_not_used(
        target_backend=options.connection_backend,
        option_owner="connection_type",
        ch_partition_by=options.ch_partition_by,
        ch_order_by=options.ch_order_by,
        ch_engine=options.ch_engine,
        ch_cluster=options.ch_cluster,
        ch_sharding_key=options.ch_sharding_key,
    )
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


def _initialize_load_state(options: LoadOptions, connection: Any) -> LoadState:
    return LoadState(
        target_exists=table_exists(
            options.connection_backend,
            connection,
            options.destination_table,
            connection_key=options.connection_key,
        )
    )


def _handle_empty_dataframe_load(
    options: LoadOptions,
    state: LoadState,
    *,
    operation_metadata: SqlOperationMetadata,
    return_metadata: bool,
) -> int | SqlOperationResult:
    if options.append and state.target_exists:
        time_print(
            f"Skipping empty DataFrame append into "
            f"{options.connection_key}.{options.destination_table}"
        )
        if return_metadata:
            operation_metadata.inserted_rows = 0
            operation_metadata.affected_rows = 0
            return SqlOperationResult(
                rows=0,
                metadata=operation_metadata,
            )
        return 0
    raise ValueError("Cannot create or replace a table from an empty DataFrame.")


def _validate_load_dataframe(options: LoadOptions, df: pd.DataFrame) -> None:
    if options.gp_distributed_by_key:
        validate_key_columns_in_columns(options.gp_distributed_by_key, df.columns)

    validate_key_columns_in_columns(options.key_columns, df.columns)
    validate_ch_columns_in_columns(
        options.ch_partition_by,
        df.columns,
        "ch_partition_by",
        data_name="staged data",
    )
    validate_ch_columns_in_columns(
        options.ch_order_by,
        df.columns,
        "ch_order_by",
        data_name="staged data",
    )
    _validate_dataframe_key_uniqueness(df, options.key_columns)


def _prepare_load_target(
    *,
    options: LoadOptions,
    state: LoadState,
    connection: Any,
    df: pd.DataFrame,
) -> None:
    _apply_load_target_write_mode(options, state, connection)
    _ensure_load_target_table(options, state, connection, df)
    _load_target_column_metadata(options, state, connection)


def _apply_load_target_write_mode(
    options: LoadOptions,
    state: LoadState,
    connection: Any,
) -> None:
    if options.write_mode == "append":
        return

    state.target_exists = apply_target_write_mode(
        options.connection_backend,
        connection,
        options.destination_table,
        write_mode=options.write_mode,
        target_exists=state.target_exists,
        replace_existing_non_ch="drop",
        ch_cluster=options.ch_cluster,
        connection_label=options.connection_key,
        drop_missing_ch_truncate_target=False,
        query_label=options.query_label,
    )


def _ensure_load_target_table(
    options: LoadOptions,
    state: LoadState,
    connection: Any,
    df: pd.DataFrame,
) -> None:
    if options.connection_backend == "ch":
        _create_load_target_table(options, connection, df, distributed=True)
        state.target_exists = True
        return

    if not state.target_exists:
        _create_load_target_table(options, connection, df, distributed=False)


def _create_load_target_table(
    options: LoadOptions,
    connection: Any,
    df: pd.DataFrame,
    *,
    distributed: bool,
) -> None:
    create_kwargs: dict[str, Any] = {}
    if options.query_label is not None:
        create_kwargs["query_label"] = options.query_label

    if distributed:
        create_sql_table(
            options.connection_backend,
            connection,
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
        return

    create_sql_table(
        options.connection_backend,
        connection,
        options.destination_table,
        df,
        gp_distributed_by_key=options.gp_distributed_by_key,
        **create_kwargs,
    )


def _load_target_column_metadata(
    options: LoadOptions,
    state: LoadState,
    connection: Any,
) -> None:
    if options.connection_backend == "trino":
        state.target_column_types = get_trino_table_column_types(
            connection,
            options.destination_table,
            connection_key=options.connection_key,
        )


def _analyze_load_target(options: LoadOptions, connection: Any) -> None:
    if options.query_label is None:
        analyze_table(
            connection_type=options.connection_backend,
            connection=connection,
            table_name=options.destination_table,
        )
        return

    analyze_table(
        connection_type=options.connection_backend,
        connection=connection,
        table_name=options.destination_table,
        query_label=options.query_label,
    )


def _build_load_result(
    *,
    options: LoadOptions,
    state: LoadState,
    connection: Any,
    source_rows: int,
    inserted_rows: int,
    operation_metadata: SqlOperationMetadata,
    return_metadata: bool,
) -> int | SqlOperationResult:
    if not return_metadata:
        return inserted_rows

    return SqlOperationResult(
        rows=inserted_rows,
        metadata=_build_load_metadata(
            options=options,
            state=state,
            connection=connection,
            source_rows=source_rows,
            inserted_rows=inserted_rows,
            operation_metadata=operation_metadata,
        ),
    )


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
            "gp_insert_chunk_size": options.gp_insert_chunk_size,
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
        add_drop_target_steps(
            plan,
            alias=options.connection_key,
            backend=options.connection_backend,
            table_name=options.destination_table,
            ch_cluster=options.ch_cluster,
            query_label=options.query_label,
        )
    elif options.write_mode == "truncate_insert":
        add_clear_target_steps(
            plan,
            alias=options.connection_key,
            backend=options.connection_backend,
            table_name=options.destination_table,
            query_label=options.query_label,
            include_ch_shard=options.connection_backend == "ch",
            ch_cluster=options.ch_cluster,
        )

    if options.write_mode in {"replace", "truncate_insert"} or options.connection_backend == "ch":
        add_create_table_steps(
            plan,
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
            table_name=options.destination_table,
        )

    if options.append and options.key_columns:
        stage_table = f"{options.destination_table}__stage__dry_run"
        metadata.stage_table = stage_table
        add_create_table_steps(
            plan,
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
            table_name=stage_table,
        )
        add_load_stage_step(
            plan,
            alias=options.connection_key,
            backend=options.connection_backend,
            stage_table=stage_table,
            sql=_build_dataframe_insert_placeholder(
                options.connection_backend,
                stage_table,
                df,
            ),
            query_label=options.query_label,
        )
        add_insert_from_stage_step(
            plan,
            alias=options.connection_key,
            backend=options.connection_backend,
            target_table=options.destination_table,
            stage_table=stage_table,
            phase="insert_from_stage",
            query_label=options.query_label,
        )
        add_cleanup_stage_step(
            plan,
            alias=options.connection_key,
            backend=options.connection_backend,
            stage_table=stage_table,
            query_label=options.query_label,
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

    add_analyze_step(
        plan,
        alias=options.connection_key,
        backend=options.connection_backend,
        table_name=options.destination_table,
        query_label=options.query_label,
    )
    add_count_step(
        plan,
        alias=options.connection_key,
        backend=options.connection_backend,
        table_name=options.destination_table,
        query_label=options.query_label,
    )
    return plan


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
    operation_metadata: SqlOperationMetadata,
) -> SqlOperationMetadata:
    metadata = operation_metadata
    metadata.source_rows = source_rows
    metadata.staged_rows = source_rows if state.overlap_stage_table is not None else None
    metadata.inserted_rows = inserted_rows
    metadata.affected_rows = inserted_rows
    metadata.stage_table = state.overlap_stage_table
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
    on_progress: Any | None = None,
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
            gp_insert_chunk_size=options.gp_insert_chunk_size,
            query_label=options.query_label,
            on_progress=on_progress,
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
        gp_insert_chunk_size=options.gp_insert_chunk_size,
        query_label=options.query_label,
        on_progress=on_progress,
    )


class _ProgressTracker:
    def __init__(self, progress_bar: Any) -> None:
        self.progress_bar = progress_bar
        self.rows = 0

    def update(self, rows: int) -> None:
        self.rows += rows
        self.progress_bar.update(rows)

    def complete_to(self, rows: int) -> None:
        remaining_rows = rows - self.rows
        if remaining_rows > 0:
            self.update(remaining_rows)


def _make_load_progress_bar(
    *,
    total: int,
    options: LoadOptions,
    progress: bool,
) -> Any:
    return tqdm(
        total=total,
        desc=f"load_df {options.connection_key}.{options.destination_table}",
        unit="row",
        disable=not progress,
    )


def _validate_progress(progress: bool) -> None:
    if not isinstance(progress, bool):
        raise ValueError("progress must be a boolean.")


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
