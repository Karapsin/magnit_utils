from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .backend_adapters import ch_cluster_clause, get_backend_adapter
from .ddl.create_sql_table import (
    build_ch_distributed_create_table_sqls,
    build_ch_shard_table_name,
    _wait_for_ch_table,
)
from .labels import apply_query_label


@dataclass(frozen=True)
class ChDistributedTablePair:
    distributed_table: str
    shard_table: str


def ch_distributed_table_pair(
    table_name: str,
    shard_table: str | None = None,
) -> ChDistributedTablePair:
    return ChDistributedTablePair(
        distributed_table=table_name,
        shard_table=shard_table or build_ch_shard_table_name(table_name),
    )


def build_drop_ch_distributed_table_pair_sqls(
    table_name: str,
    ch_cluster: str | None = "{cluster}",
    *,
    shard_table: str | None = None,
    query_label: str | None = None,
) -> list[str]:
    pair = ch_distributed_table_pair(table_name, shard_table)
    sqls = [
        _build_drop_ch_table_sql(pair.distributed_table, query_label=query_label),
        _build_drop_ch_table_sql(pair.shard_table, query_label=query_label),
    ]
    if ch_cluster is not None:
        sqls.extend(
            [
                _build_drop_ch_table_sql(
                    pair.distributed_table,
                    ch_cluster=ch_cluster,
                    query_label=query_label,
                ),
                _build_drop_ch_table_sql(
                    pair.shard_table,
                    ch_cluster=ch_cluster,
                    query_label=query_label,
                ),
            ]
        )
    return sqls


def drop_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str | None = "{cluster}",
    *,
    shard_table: str | None = None,
    query_label: str | None = None,
) -> None:
    _execute_ch_sqls(
        connection,
        build_drop_ch_distributed_table_pair_sqls(
            table_name,
            ch_cluster=ch_cluster,
            shard_table=shard_table,
            query_label=query_label,
        ),
    )


def build_truncate_ch_distributed_table_pair_sqls(
    table_name: str,
    ch_cluster: str | None = "{cluster}",
    *,
    shard_table: str | None = None,
    query_label: str | None = None,
) -> list[str]:
    pair = ch_distributed_table_pair(table_name, shard_table)
    return [
        _build_truncate_ch_table_sql(
            pair.shard_table,
            ch_cluster=ch_cluster,
            query_label=query_label,
        ),
        _build_truncate_ch_table_sql(
            pair.distributed_table,
            query_label=query_label,
        ),
    ]


def truncate_ch_distributed_table_pair(
    connection: Any,
    table_name: str,
    ch_cluster: str | None = "{cluster}",
    *,
    shard_table: str | None = None,
    query_label: str | None = None,
) -> None:
    _execute_ch_sqls(
        connection,
        build_truncate_ch_distributed_table_pair_sqls(
            table_name,
            ch_cluster=ch_cluster,
            shard_table=shard_table,
            query_label=query_label,
        ),
    )


def build_create_ch_distributed_table_pair_sqls(
    *,
    table_name: str,
    joined_columns: str,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
) -> list[str]:
    return [
        apply_query_label(sql, query_label)
        for sql in build_ch_distributed_create_table_sqls(
            table_name=table_name,
            joined_columns=joined_columns,
            ch_partition_by=ch_partition_by,
            ch_order_by=ch_order_by,
            ch_engine=ch_engine,
            ch_cluster=ch_cluster,
            ch_sharding_key=ch_sharding_key,
        )
    ]


def create_ch_distributed_table_pair(
    connection: Any,
    *,
    table_name: str,
    joined_columns: str,
    ch_partition_by: Sequence[str] | str | None = None,
    ch_order_by: Sequence[str] | str | None = None,
    ch_engine: str = "ReplicatedMergeTree",
    ch_cluster: str = "{cluster}",
    ch_sharding_key: str = "rand()",
    query_label: str | None = None,
    wait_for_table: bool = False,
) -> None:
    _execute_ch_sqls(
        connection,
        build_create_ch_distributed_table_pair_sqls(
            table_name=table_name,
            joined_columns=joined_columns,
            ch_partition_by=ch_partition_by,
            ch_order_by=ch_order_by,
            ch_engine=ch_engine,
            ch_cluster=ch_cluster,
            ch_sharding_key=ch_sharding_key,
            query_label=query_label,
        ),
    )
    if wait_for_table:
        _wait_for_ch_table(connection, table_name)


def _build_drop_ch_table_sql(
    table_name: str,
    *,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> str:
    return apply_query_label(
        f"DROP TABLE IF EXISTS {table_name}{ch_cluster_clause(ch_cluster)}",
        query_label,
    )


def _build_truncate_ch_table_sql(
    table_name: str,
    *,
    ch_cluster: str | None = None,
    query_label: str | None = None,
) -> str:
    return apply_query_label(
        f"TRUNCATE TABLE IF EXISTS {table_name}{ch_cluster_clause(ch_cluster)}",
        query_label,
    )


def _execute_ch_sqls(connection: Any, sqls: list[str]) -> None:
    adapter = get_backend_adapter("ch")
    for sql in sqls:
        adapter.execute_command(connection, sql)
