from __future__ import annotations

from collections.abc import Sequence

from .plans import SqlPlan


def add_inspect_schema_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    source_sql: str,
    phase: str = "inspect_source_schema",
    query_label: str | None = None,
) -> None:
    plan.add(
        f"SELECT * FROM ({source_sql}) AS source_schema_probe WHERE 1 = 0",
        alias=alias,
        backend=backend,
        phase=phase,
        query_label=query_label,
    )


def add_create_table_steps(
    plan: SqlPlan,
    sqls: Sequence[str],
    *,
    alias: str,
    backend: str,
    table_name: str,
    phase: str = "create_target",
) -> None:
    plan.extend(
        list(sqls),
        alias=alias,
        backend=backend,
        phase=phase,
        target_table=table_name,
    )


def add_create_table_placeholder_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    table_name: str,
    phase: str = "create_target",
    query_label: str | None = None,
) -> None:
    plan.add(
        f"CREATE TABLE {table_name} (<source query schema>)",
        alias=alias,
        backend=backend,
        phase=phase,
        target_table=table_name,
        query_label=query_label,
    )


def add_load_stage_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    stage_table: str,
    sql: str,
    query_label: str | None = None,
) -> None:
    plan.add(
        sql,
        alias=alias,
        backend=backend,
        phase="load_stage",
        target_table=stage_table,
        query_label=query_label,
    )


def add_insert_query_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    target_table: str,
    source_sql: str,
    phase: str = "insert_data",
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import build_insert_from_query_sql

    plan.add(
        build_insert_from_query_sql(
            backend,
            target_table,
            source_sql,
            {},
            query_label=query_label,
        ).replace(" ()", "").replace("SELECT  FROM", "SELECT * FROM"),
        alias=alias,
        backend=backend,
        phase=phase,
        target_table=target_table,
    )


def add_cleanup_stage_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    stage_table: str,
    query_label: str | None = None,
) -> None:
    add_drop_stage_step(
        plan,
        alias=alias,
        backend=backend,
        stage_table=stage_table,
        query_label=query_label,
    )


def add_drop_target_steps(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    table_name: str,
    ch_cluster: str = "{cluster}",
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import (
        build_drop_ch_distributed_table_pair_sqls,
        build_drop_table_sql,
    )

    if backend == "ch":
        plan.extend(
            build_drop_ch_distributed_table_pair_sqls(
                table_name,
                ch_cluster=ch_cluster,
                query_label=query_label,
            ),
            alias=alias,
            backend=backend,
            phase="drop_target",
            target_table=table_name,
        )
        return

    plan.add(
        build_drop_table_sql(
            backend,
            table_name,
            query_label=query_label,
        ),
        alias=alias,
        backend=backend,
        phase="drop_target",
        target_table=table_name,
    )


def add_clear_target_steps(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    table_name: str,
    query_label: str | None = None,
    include_ch_shard: bool = False,
    ch_cluster: str = "{cluster}",
) -> None:
    for sql in build_clear_target_sqls(
        backend,
        table_name,
        query_label=query_label,
        include_ch_shard=include_ch_shard,
        ch_cluster=ch_cluster,
    ):
        plan.add(
            sql,
            alias=alias,
            backend=backend,
            phase="clear_target",
            target_table=table_name,
        )


def build_clear_target_sqls(
    backend: str,
    table_name: str,
    *,
    query_label: str | None = None,
    include_ch_shard: bool = False,
    ch_cluster: str = "{cluster}",
) -> list[str]:
    from .ch_lifecycle import build_truncate_ch_distributed_table_pair_sqls
    from .dml.table.table_ops import build_clear_table_sqls

    if backend != "ch" or not include_ch_shard:
        return build_clear_table_sqls(
            backend,
            table_name,
            query_label=query_label,
        )

    return build_truncate_ch_distributed_table_pair_sqls(
        table_name,
        ch_cluster=ch_cluster,
        query_label=query_label,
    )


def add_insert_from_stage_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    target_table: str,
    stage_table: str,
    phase: str,
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import build_insert_from_table_sql

    plan.add(
        build_insert_from_table_sql(
            backend,
            target_table,
            stage_table,
            query_label=query_label,
        ),
        alias=alias,
        backend=backend,
        phase=phase,
        target_table=target_table,
        source_table=stage_table,
    )


def add_analyze_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    table_name: str,
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import build_analyze_table_sql

    if backend == "ch":
        return

    plan.add(
        build_analyze_table_sql(
            backend,
            table_name,
            query_label=query_label,
        ),
        alias=alias,
        backend=backend,
        phase="analyze",
        target_table=table_name,
    )


def add_count_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    table_name: str,
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import build_count_table_rows_sql

    plan.add(
        build_count_table_rows_sql(
            backend,
            table_name,
            query_label=query_label,
        ),
        alias=alias,
        backend=backend,
        phase="count_target",
        target_table=table_name,
    )


def add_drop_stage_step(
    plan: SqlPlan,
    *,
    alias: str,
    backend: str,
    stage_table: str,
    query_label: str | None = None,
) -> None:
    from .dml.table.table_ops import build_drop_table_sql

    plan.add(
        build_drop_table_sql(
            backend,
            stage_table,
            query_label=query_label,
        ),
        alias=alias,
        backend=backend,
        phase="drop_stage",
        target_table=stage_table,
    )
