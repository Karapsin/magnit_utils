"""Shared table operations and validation helpers used by load and transfer flows."""

from .ch_create_table_as import ch_create_table_as
from .ch_full_table_move import ch_full_table_move
from .create_table_from_sql import create_table_from_sql
from .models import (
    ChCreateTableAsOptions,
    ChFullTableMoveOptions,
    CreateTableFromSqlOptions,
)
from .table_ops import (
    analyze_table,
    build_analyze_table_sql,
    build_clear_table_sqls,
    build_count_table_rows_sql,
    build_drop_ch_distributed_table_pair_sqls,
    build_drop_table_sql,
    clear_target_table,
    build_insert_from_query_sql,
    build_insert_from_table_sql,
    count_table_rows,
    drop_table,
    drop_table_with_retry,
    finalize_stage_table,
    get_table_column_types,
    get_trino_table_column_types,
    gp_vacuum,
    insert_from_query,
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
    "build_analyze_table_sql",
    "build_clear_table_sqls",
    "build_count_table_rows_sql",
    "build_drop_ch_distributed_table_pair_sqls",
    "build_drop_table_sql",
    "ch_create_table_as",
    "ChCreateTableAsOptions",
    "ch_full_table_move",
    "ChFullTableMoveOptions",
    "clear_target_table",
    "build_insert_from_query_sql",
    "build_insert_from_table_sql",
    "count_table_rows",
    "create_table_from_sql",
    "CreateTableFromSqlOptions",
    "drop_table",
    "drop_table_with_retry",
    "finalize_stage_table",
    "get_table_column_types",
    "get_trino_table_column_types",
    "gp_vacuum",
    "insert_from_query",
    "insert_from_table",
    "normalize_key_columns",
    "table_exists",
    "validate_key_columns_in_columns",
    "validate_stage_target_key_overlap",
    "validate_stage_uniqueness",
]
