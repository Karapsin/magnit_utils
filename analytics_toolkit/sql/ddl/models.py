from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CreateSqlTableOptions:
    connection_type: str
    backend: str
    connection: Any
    table_name: str
    batch: pd.DataFrame
    column_types: Mapping[str, str] | None = None
    gp_distributed_by_key: list[str] | None = None
    ch_partition_by: Sequence[str] | str | None = None
    ch_order_by: Sequence[str] | str | None = None
    ch_engine: str = "ReplicatedMergeTree"
    ch_cluster: str = "{cluster}"
    ch_sharding_key: str = "rand()"
    ch_distributed_table: bool = False
    dry_run: bool = False
    return_sql: bool = False
    query_label: str | None = None
    return_metadata: bool = False

