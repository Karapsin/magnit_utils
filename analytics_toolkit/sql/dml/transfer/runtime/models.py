from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RowBatch:
    columns: list[str]
    rows: list[tuple[Any, ...]]

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def empty(self) -> bool:
        return self.row_count == 0

    def to_dataframe(self, *, include_rows: bool = False) -> pd.DataFrame:
        if include_rows:
            return pd.DataFrame(self.rows, columns=self.columns)
        return pd.DataFrame(columns=self.columns)


@dataclass
class AdaptiveBatchSizer:
    enabled: bool
    current_size: int
    min_size: int
    max_size: int
    target_seconds: float

    def update(self, duration_seconds: float) -> None:
        if not self.enabled:
            return

        if duration_seconds < self.target_seconds / 2:
            grown_size = max(self.current_size + 1, (self.current_size * 3 + 1) // 2)
            self.current_size = min(grown_size, self.max_size)
            return

        if duration_seconds > self.target_seconds * 2:
            shrunk_size = max(1, int(self.current_size * 0.5))
            self.current_size = max(shrunk_size, self.min_size)


@dataclass(frozen=True)
class TransferOptions:
    from_db_key: str
    from_db_backend: str
    to_db_key: str
    to_db_backend: str
    source_sql: str
    target_table: str
    replace_target_table: bool = True
    write_mode: str = "replace"
    batch_size: int = 100_000
    retry_cnt: int = 5
    timeout_increment: int | float = 5
    full_retry_cnt: int = 5
    full_timeout_increment: int | float = 60 * 10
    key_columns: list[str] | None = None
    gp_distributed_by_key: list[str] | None = None
    trino_insert_chunk_size: int | None = None
    adaptive_batch_size: bool = True
    min_batch_size: int = 1_000
    max_batch_size: int = 400_000
    target_batch_seconds: float = 10.0
    ch_partition_by: list[str] | str | None = None
    ch_order_by: list[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "{cluster}"
    ch_sharding_key: str = "rand()"
    query_label: str | None = None
    progress: bool = True


@dataclass
class TransferStageState:
    target_exists: bool
    stage_table_created: bool = False
    first_non_empty_batch: pd.DataFrame | None = None
    source_column_types: dict[str, str | None] | None = None
    stage_column_types: dict[str, str] | None = None
    insert_column_types: dict[str, str] | None = None
    stage_table: str | None = None


@dataclass
class TransferConnectionRefs:
    source: dict[str, Any] = field(default_factory=dict)
    target: dict[str, Any] = field(default_factory=dict)
