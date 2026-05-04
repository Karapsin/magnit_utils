from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LoadOptions:
    connection_key: str
    connection_backend: str
    destination_table: str
    append: bool = False
    gp_distributed_by_key: list[str] | None = None
    key_columns: list[str] | None = None
    trino_insert_chunk_size: int | None = None
    ch_partition_by: list[str] | str | None = None
    ch_order_by: list[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "core"
    ch_sharding_key: str = "rand()"


@dataclass
class LoadState:
    target_exists: bool
    overlap_stage_table: str | None = None
    target_column_types: dict[str, str] | None = None
