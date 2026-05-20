from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class CreateTableFromSqlOptions:
    source_key: str
    source_backend: str
    target_key: str
    target_backend: str
    target_table: str
    source_sql: str
    insert_data: bool = False
    drop_target_if_exists: bool = False
    gp_distributed_by_key: list[str] | None = None
    ch_partition_by: Sequence[str] | str | None = None
    ch_order_by: Sequence[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "{cluster}"
    ch_sharding_key: str = "rand()"
    trino_insert_chunk_size: int | None = None
    dry_run: bool = False
    return_sql: bool = False
    return_metadata: bool = False
    query_label: str | None = None


@dataclass(frozen=True)
class ChCreateTableAsOptions:
    connection_key: str
    backend: str
    target_table: str
    query_sql: str
    ch_partition_by: Sequence[str] | str | None = None
    ch_order_by: Sequence[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "{cluster}"
    ch_sharding_key: str = "rand()"
    dry_run: bool = False
    return_sql: bool = False
    return_metadata: bool = False
    query_label: str | None = None


@dataclass(frozen=True)
class ChFullTableMoveOptions:
    connection_key: str
    backend: str
    source_table: str
    target_table: str
    ch_partition_by: str | None = None
    ch_order_by: str | None = None
    ch_engine: str | None = None
    ch_cluster: str | None = "{cluster}"
    sharding_key: str | None = None
    dry_run: bool = False
    return_sql: bool = False
    return_metadata: bool = False
    query_label: str | None = None

