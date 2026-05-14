from __future__ import annotations

from .base import BackendAdapter, UNSUPPORTED_BACKEND_MESSAGE
from .clickhouse import (
    ClickHouseAdapter,
    ch_cluster_clause,
    format_ch_cluster_name,
    is_simple_identifier,
)
from .dbapi import DbApiBackendAdapter
from .gp import (
    GreenplumAdapter,
    format_gp_information_schema_type,
    split_gp_table_name,
)
from .registry import BACKEND_ADAPTERS, get_backend_adapter
from .trino import TrinoAdapter, split_trino_table_name
from .utils import extract_row_count


__all__ = [
    "BACKEND_ADAPTERS",
    "BackendAdapter",
    "ClickHouseAdapter",
    "DbApiBackendAdapter",
    "GreenplumAdapter",
    "TrinoAdapter",
    "UNSUPPORTED_BACKEND_MESSAGE",
    "ch_cluster_clause",
    "extract_row_count",
    "format_ch_cluster_name",
    "format_gp_information_schema_type",
    "get_backend_adapter",
    "is_simple_identifier",
    "split_gp_table_name",
    "split_trino_table_name",
]
