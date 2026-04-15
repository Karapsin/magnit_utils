from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LoadOptions:
    connection_type: str
    destination_table: str
    append: bool = False
    gp_distributed_by_key: list[str] | None = None
    key_columns: list[str] | None = None


@dataclass
class LoadState:
    target_exists: bool
    overlap_stage_table: str | None = None
    target_column_types: dict[str, str] | None = None
