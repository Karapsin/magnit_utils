from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class TransferOptions:
    from_db_key: str
    from_db_backend: str
    to_db_key: str
    to_db_backend: str
    source_sql: str
    target_table: str
    replace_target_table: bool = True
    batch_size: int = 100_000
    retry_cnt: int = 5
    timeout_increment: int | float = 5
    full_retry_cnt: int = 5
    full_timeout_increment: int | float = 60 * 10
    key_columns: list[str] | None = None
    gp_distributed_by_key: list[str] | None = None
    trino_insert_chunk_size: int | None = None
    ch_partition_by: list[str] | str | None = None
    ch_order_by: list[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "core"
    ch_sharding_key: str = "rand()"


@dataclass
class TransferStageState:
    target_exists: bool
    stage_table_created: bool = False
    first_non_empty_batch: pd.DataFrame | None = None
    stage_column_types: dict[str, str] | None = None
    stage_table: str | None = None


@dataclass
class TransferConnectionRefs:
    source: dict[str, Any] = field(default_factory=dict)
    target: dict[str, Any] = field(default_factory=dict)
