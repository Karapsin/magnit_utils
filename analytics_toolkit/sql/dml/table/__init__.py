"""Shared table operations and validation helpers used by load and transfer flows."""

from .table_ops import (
    analyze_table,
    clear_target_table,
    drop_table,
    drop_table_with_retry,
    finalize_stage_table,
    get_trino_table_column_types,
    insert_from_table,
    table_exists,
)
from .table_validation import (
    normalize_key_columns,
    validate_key_columns_in_columns,
    validate_stage_target_key_overlap,
    validate_stage_uniqueness,
)

__all__ = [
    "analyze_table",
    "clear_target_table",
    "drop_table",
    "drop_table_with_retry",
    "finalize_stage_table",
    "get_trino_table_column_types",
    "insert_from_table",
    "normalize_key_columns",
    "table_exists",
    "validate_key_columns_in_columns",
    "validate_stage_target_key_overlap",
    "validate_stage_uniqueness",
]
