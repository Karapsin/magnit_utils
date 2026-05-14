from __future__ import annotations

from collections.abc import Sequence

from ....capabilities import validate_write_mode
from ....connection.config import TrinoConfig, get_connection_config
from ....connection.errors import (
    SqlOperationContext,
    annotate_sql_exception,
    sql_preview,
)
from ....connection.get_sql_connection import get_sql_connection
from ....plans import SqlOperationMetadata, SqlOperationResult, SqlPlan
from analytics_toolkit.general import time_print
from ...load.load_sql_table import AmbiguousTableLoadError
from ...load.stage import build_stage_table_name
from ...table.table_ops import (
    build_analyze_table_sql,
    build_clear_table_sqls,
    build_count_table_rows_sql,
    build_drop_ch_distributed_table_pair_sqls,
    build_insert_from_table_sql,
    count_table_rows,
)
from ...table.table_validation import normalize_key_columns
from .attempt import run_transfer_attempt
from ..runtime.models import TransferOptions
from ..runtime.retry import run_with_retry


def transfer_table(
    from_db: str,
    to_db: str,
    from_sql: str,
    to_table: str,
    replace_target_table: bool = True,
    write_mode: str | None = None,
    batch_size: int = 100_000,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    full_retry_cnt: int = 5,
    full_timeout_increment: int | float = 60 * 10,
    key_columns: list[str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
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
    options = build_transfer_options(
        from_db=from_db,
        to_db=to_db,
        from_sql=from_sql,
        to_table=to_table,
        replace_target_table=replace_target_table,
        write_mode=write_mode,
        batch_size=batch_size,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        full_retry_cnt=full_retry_cnt,
        full_timeout_increment=full_timeout_increment,
        key_columns=key_columns,
        gp_distributed_by_key=gp_distributed_by_key,
        trino_insert_chunk_size=trino_insert_chunk_size,
        ch_partition_by=ch_partition_by,
        ch_order_by=ch_order_by,
        ch_engine=ch_engine,
        ch_cluster=ch_cluster,
        ch_sharding_key=sharding_key,
        query_label=query_label,
    )

    if dry_run or return_sql:
        return build_transfer_table_plan(options)

    time_print(
        f"Starting table transfer from {options.from_db_key} "
        f"to {options.to_db_key}: {options.target_table}"
    )

    def transfer_operation(attempt: int) -> int:
        try:
            if options.to_db_backend == "gp":
                return run_transfer_attempt(
                    options=options,
                    read_retry_cnt=options.retry_cnt,
                    insert_retry_cnt=options.retry_cnt,
                )

            def stage_restart_operation(inner_attempt: int) -> int:
                try:
                    return run_transfer_attempt(
                        options=options,
                        read_retry_cnt=options.retry_cnt,
                        insert_retry_cnt=1,
                    )
                except AmbiguousTableLoadError as exc:
                    time_print(
                        f"Discarding staged load for {options.to_db_key} "
                        f"and restarting from scratch: {exc!r}"
                    )
                    raise

            return run_with_retry(
                operation_name=(
                    f"restarting staged transfer from {options.from_db_key} "
                    f"to {options.to_db_key}: {options.target_table}"
                ),
                retry_cnt=options.retry_cnt,
                timeout_increment=options.timeout_increment,
                operation=stage_restart_operation,
                retryable_exceptions=(AmbiguousTableLoadError,),
            )
        except Exception as exc:
            annotate_sql_exception(
                exc,
                SqlOperationContext(
                    operation="transfer_table",
                    alias=options.to_db_key,
                    backend=options.to_db_backend,
                    phase="transfer",
                    target_table=options.target_table,
                    retry_attempt=attempt,
                    sql_preview=sql_preview(options.source_sql),
                ),
            )
            raise

    if options.replace_target_table:
        total_rows = run_with_retry(
            operation_name=(
                f"restarting full transfer from {options.from_db_key} "
                f"to {options.to_db_key}: {options.target_table}"
            ),
            retry_cnt=options.full_retry_cnt,
            timeout_increment=options.full_timeout_increment,
            operation=transfer_operation,
        )
    else:
        total_rows = transfer_operation(1)

    time_print(
        f"Finished table transfer from {options.from_db_key} "
        f"to {options.to_db_key}: {total_rows} row(s)"
    )
    if return_metadata:
        metadata = SqlOperationMetadata(
            source_rows=total_rows,
            staged_rows=total_rows,
            inserted_rows=total_rows,
            affected_rows=total_rows,
        )
        metadata.final_target_rows = _best_effort_transfer_target_count(options)
        return SqlOperationResult(
            rows=total_rows,
            metadata=metadata,
        )
    return total_rows


def build_transfer_options(
    from_db: str,
    to_db: str,
    from_sql: str,
    to_table: str,
    replace_target_table: bool = True,
    write_mode: str | None = None,
    batch_size: int = 100_000,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    full_retry_cnt: int = 5,
    full_timeout_increment: int | float = 60 * 10,
    key_columns: list[str] | None = None,
    gp_distributed_by_key: list[str] | None = None,
    trino_insert_chunk_size: int | None = None,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> TransferOptions:
    from_config = get_connection_config(from_db)
    to_config = get_connection_config(to_db)
    configured_trino_insert_chunk_size = (
        to_config.insert_chunk_size if isinstance(to_config, TrinoConfig) else None
    )
    resolved_write_mode = _resolve_transfer_write_mode(
        to_config.backend,
        replace_target_table=replace_target_table,
        write_mode=write_mode,
    )
    options = TransferOptions(
        from_db_key=from_config.connection_key,
        from_db_backend=from_config.backend,
        to_db_key=to_config.connection_key,
        to_db_backend=to_config.backend,
        source_sql=from_sql.strip(),
        target_table=to_table.strip(),
        replace_target_table=resolved_write_mode != "append",
        write_mode=resolved_write_mode,
        batch_size=batch_size,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        full_retry_cnt=full_retry_cnt,
        full_timeout_increment=full_timeout_increment,
        key_columns=normalize_key_columns(key_columns),
        gp_distributed_by_key=normalize_key_columns(gp_distributed_by_key),
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

    if options.from_db_key == options.to_db_key:
        raise ValueError("from_db and to_db must be different.")
    if not options.source_sql:
        raise ValueError("from_sql must not be empty.")
    if not options.target_table:
        raise ValueError("to_table must not be empty.")
    if options.batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if options.retry_cnt < 1:
        raise ValueError("retry_cnt must be at least 1.")
    if options.timeout_increment < 0:
        raise ValueError("timeout_increment must be non-negative.")
    if options.full_retry_cnt < 1:
        raise ValueError("full_retry_cnt must be at least 1.")
    if options.full_timeout_increment < 0:
        raise ValueError("full_timeout_increment must be non-negative.")
    if options.gp_distributed_by_key is not None and options.to_db_backend != "gp":
        raise ValueError(
            "gp_distributed_by_key can only be used when to_db has type 'gp'."
        )
    if options.trino_insert_chunk_size is not None and options.trino_insert_chunk_size <= 0:
        raise ValueError("trino_insert_chunk_size must be a positive integer.")
    if options.to_db_backend != "ch":
        _validate_ch_options_not_used(options)
    return options


def _resolve_transfer_write_mode(
    to_db_backend: str,
    *,
    replace_target_table: bool,
    write_mode: str | None,
) -> str:
    if write_mode is None:
        return "replace" if replace_target_table else "append"

    normalized = validate_write_mode(to_db_backend, write_mode)
    if not replace_target_table and normalized != "append":
        raise ValueError(
            "replace_target_table=False cannot be combined with write_mode "
            "other than 'append'."
        )
    return normalized


def build_transfer_table_plan(options: TransferOptions) -> SqlPlan:
    stage_table = _dry_run_stage_table_name(options)
    plan = SqlPlan(
        operation="transfer_table",
        source_alias=options.from_db_key,
        target_alias=options.to_db_key,
        source_backend=options.from_db_backend,
        target_backend=options.to_db_backend,
        target_table=options.target_table,
        options={
            "write_mode": options.write_mode,
            "batch_size": options.batch_size,
            "key_columns": options.key_columns,
            "gp_distributed_by_key": options.gp_distributed_by_key,
            "trino_insert_chunk_size": options.trino_insert_chunk_size,
            "ch_partition_by": options.ch_partition_by,
            "ch_order_by": options.ch_order_by,
            "ch_engine": options.ch_engine,
            "ch_cluster": options.ch_cluster,
            "ch_sharding_key": options.ch_sharding_key,
        },
        metadata=SqlOperationMetadata(stage_table=stage_table),
    )
    plan.add(
        options.source_sql,
        alias=options.from_db_key,
        backend=options.from_db_backend,
        phase="read_source",
        query_label=options.query_label,
    )
    plan.add(
        f"CREATE TABLE {stage_table} (<source query schema>)",
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="create_stage",
        target_table=stage_table,
        query_label=options.query_label,
    )
    plan.add(
        f"INSERT INTO {stage_table} SELECT * FROM (<source batches>)",
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="load_stage",
        target_table=stage_table,
        query_label=options.query_label,
    )
    if options.write_mode == "replace":
        if options.to_db_backend == "ch":
            plan.extend(
                build_drop_ch_distributed_table_pair_sqls(
                    options.target_table,
                    ch_cluster=options.ch_cluster,
                    query_label=options.query_label,
                ),
                alias=options.to_db_key,
                backend=options.to_db_backend,
                phase="drop_target",
                target_table=options.target_table,
            )
        else:
            for sql in build_clear_table_sqls(
                options.to_db_backend,
                options.target_table,
                query_label=options.query_label,
            ):
                plan.add(
                    sql,
                    alias=options.to_db_key,
                    backend=options.to_db_backend,
                    phase="clear_target",
                    target_table=options.target_table,
                )
    elif options.write_mode == "truncate_insert":
        for sql in build_clear_table_sqls(
            options.to_db_backend,
            options.target_table,
            query_label=options.query_label,
        ):
            plan.add(
                sql,
                alias=options.to_db_key,
                backend=options.to_db_backend,
                phase="clear_target",
                target_table=options.target_table,
            )
    plan.add(
        f"CREATE TABLE {options.target_table} (<source query schema>)",
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="create_target",
        target_table=options.target_table,
        query_label=options.query_label,
    )
    plan.add(
        build_insert_from_table_sql(
            options.to_db_backend,
            options.target_table,
            stage_table,
            query_label=options.query_label,
        ),
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="insert_target",
        target_table=options.target_table,
        source_table=stage_table,
    )
    if options.to_db_backend != "ch":
        plan.add(
            build_analyze_table_sql(
                options.to_db_backend,
                options.target_table,
                query_label=options.query_label,
            ),
            alias=options.to_db_key,
            backend=options.to_db_backend,
            phase="analyze",
            target_table=options.target_table,
        )
    plan.add(
        build_count_table_rows_sql(
            options.to_db_backend,
            options.target_table,
            query_label=options.query_label,
        ),
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="count_target",
        target_table=options.target_table,
    )
    plan.add(
        f"DROP TABLE IF EXISTS {stage_table}",
        alias=options.to_db_key,
        backend=options.to_db_backend,
        phase="drop_stage",
        target_table=stage_table,
        query_label=options.query_label,
    )
    return plan


def _dry_run_stage_table_name(options: TransferOptions) -> str:
    try:
        return build_stage_table_name(options.to_db_backend, options.target_table).rsplit(
            "__stage__",
            1,
        )[0] + "__stage__dryrun"
    except Exception:
        return f"{options.target_table}__stage__dryrun"


def _best_effort_transfer_target_count(options: TransferOptions) -> int | None:
    connection = None
    try:
        connection = get_sql_connection(options.to_db_key)
        return count_table_rows(
            options.to_db_backend,
            connection,
            options.target_table,
            query_label=options.query_label,
        )
    except Exception:
        return None
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


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


def _validate_ch_options_not_used(options: TransferOptions) -> None:
    if options.ch_partition_by is not None:
        raise ValueError("ch_partition_by can only be used when to_db has type 'ch'.")
    if options.ch_order_by is not None:
        raise ValueError("ch_order_by can only be used when to_db has type 'ch'.")
    if options.ch_engine != "ReplicatedMergeTree":
        raise ValueError("ch_engine can only be used when to_db has type 'ch'.")
    if options.ch_cluster != "{cluster}":
        raise ValueError("ch_cluster can only be used when to_db has type 'ch'.")
    if options.ch_sharding_key != "rand()":
        raise ValueError("sharding_key can only be used when to_db has type 'ch'.")
