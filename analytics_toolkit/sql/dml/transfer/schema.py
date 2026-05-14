from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ..table.table_ops import get_table_column_types


@dataclass(frozen=True)
class SourceColumn:
    name: str
    native_type: str | None = None
    precision: int | None = None
    scale: int | None = None


_GP_OID_TYPES = {
    16: "boolean",
    17: "bytea",
    20: "bigint",
    21: "smallint",
    23: "integer",
    25: "text",
    700: "real",
    701: "double precision",
    1042: "character",
    1043: "character varying",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamp with time zone",
    1700: "numeric",
    2950: "uuid",
    3802: "jsonb",
}

_GP_MAX_NUMERIC_PRECISION = 1000
_TRINO_MAX_DECIMAL_PRECISION = 38
_CLICKHOUSE_MAX_DECIMAL_PRECISION = 76


def inspect_source_query_schema(
    connection_backend: str,
    connection: Any,
    query: str,
) -> list[SourceColumn]:
    if connection_backend in {"gp", "trino"}:
        return _inspect_dbapi_source_schema(connection_backend, connection, query)
    if connection_backend == "ch":
        return _inspect_ch_source_schema(connection, query)
    raise ValueError(f"Unsupported source backend: {connection_backend!r}.")


def map_source_schema_to_target(
    source_schema: list[SourceColumn],
    target_backend: str,
) -> dict[str, str]:
    return {
        column.name: map_source_type_to_target(column, target_backend)
        for column in source_schema
    }


def get_existing_target_insert_types(
    connection_backend: str,
    connection: Any,
    target_table: str,
    stage_column_types: dict[str, str],
    connection_key: str,
) -> dict[str, str]:
    target_column_types = get_table_column_types(
        connection_backend,
        connection,
        target_table,
        connection_key=connection_key,
    )
    missing_columns = [
        column_name
        for column_name in stage_column_types
        if column_name not in target_column_types
    ]
    if missing_columns:
        raise ValueError(
            "Target table is missing staged column(s): "
            + ", ".join(missing_columns)
        )
    return {
        column_name: target_column_types[column_name]
        for column_name in stage_column_types
    }


def map_source_type_to_target(column: SourceColumn, target_backend: str) -> str:
    source_type = _normalize_type_name(column.native_type)
    precision, scale = _type_precision_scale(column, source_type)
    kind = _classify_source_type(source_type)

    if target_backend == "gp":
        return _map_to_gp_type(kind, source_type, precision, scale)
    if target_backend == "trino":
        return _map_to_trino_type(kind, source_type, precision, scale)
    if target_backend == "ch":
        return _nullable_ch_type(_map_to_ch_base_type(kind, source_type, precision, scale))
    raise ValueError(f"Unsupported target backend: {target_backend!r}.")


def _inspect_dbapi_source_schema(
    connection_backend: str,
    connection: Any,
    query: str,
) -> list[SourceColumn]:
    cursor = connection.cursor()
    try:
        cursor.execute(_zero_row_query(query))
        return [
            _source_column_from_description(connection_backend, column)
            for column in cursor.description or []
        ]
    finally:
        cursor.close()


def _inspect_ch_source_schema(connection: Any, query: str) -> list[SourceColumn]:
    result = connection.query(f"DESCRIBE TABLE ({_strip_query_semicolon(query)})")
    rows = getattr(result, "result_rows", None) or []
    return [
        SourceColumn(name=str(row[0]), native_type=str(row[1]) if len(row) > 1 else None)
        for row in rows
    ]


def _source_column_from_description(
    connection_backend: str,
    column: Any,
) -> SourceColumn:
    name = _description_value(column, "name", 0)
    type_code = _description_value(column, "type_code", 1)
    precision = _optional_int(_description_value(column, "precision", 4))
    scale = _optional_int(_description_value(column, "scale", 5))
    native_type = _type_code_name(connection_backend, type_code, precision, scale)
    return SourceColumn(
        name=str(name),
        native_type=native_type,
        precision=precision,
        scale=scale,
    )


def _description_value(column: Any, attribute: str, index: int) -> Any:
    if hasattr(column, attribute):
        return getattr(column, attribute)
    try:
        return column[index]
    except (IndexError, TypeError):
        return None


def _type_code_name(
    connection_backend: str,
    type_code: Any,
    precision: int | None,
    scale: int | None,
) -> str | None:
    if type_code is None:
        return None
    if connection_backend == "gp" and isinstance(type_code, int):
        base_type = _GP_OID_TYPES.get(type_code, str(type_code))
        if base_type == "numeric" and precision is not None and scale is not None:
            return f"numeric({precision},{scale})"
        return base_type
    for attribute in ("name", "type_name", "typename"):
        value = getattr(type_code, attribute, None)
        if value:
            return str(value)
    return str(type_code)


def _zero_row_query(query: str) -> str:
    return f"SELECT * FROM ({_strip_query_semicolon(query)}) AS source_schema_probe WHERE 1 = 0"


def _strip_query_semicolon(query: str) -> str:
    return query.strip().removesuffix(";").strip()


def _normalize_type_name(source_type: str | None) -> str:
    if not source_type:
        return ""
    normalized = source_type.strip().lower()
    while True:
        unwrapped = _unwrap_type(normalized, "nullable")
        unwrapped = _unwrap_type(unwrapped, "lowcardinality")
        if unwrapped == normalized:
            return normalized
        normalized = unwrapped


def _unwrap_type(value: str, wrapper: str) -> str:
    prefix = f"{wrapper}("
    if value.startswith(prefix) and value.endswith(")"):
        return value[len(prefix) : -1].strip()
    return value


def _classify_source_type(source_type: str) -> str:
    if not source_type:
        return "string"
    if source_type in {"boolean", "bool"}:
        return "boolean"
    if source_type in {"date", "date32"}:
        return "date"
    if "timestamp" in source_type or source_type.startswith("datetime"):
        return "timestamp"
    if source_type.startswith(("decimal", "numeric", "number")):
        return "decimal"
    if source_type.startswith(("float", "double", "real")):
        return "float"
    if "int" in source_type and not source_type.startswith("interval"):
        return "integer"
    if any(
        token in source_type
        for token in (
            "char",
            "text",
            "string",
            "uuid",
            "json",
            "enum",
            "ip",
        )
    ):
        return "string"
    return "string"


def _type_precision_scale(
    column: SourceColumn,
    source_type: str,
) -> tuple[int | None, int | None]:
    match = re.search(r"\((\d+)\s*,\s*(\d+)\)", source_type)
    if match:
        return int(match.group(1)), int(match.group(2))
    return column.precision, column.scale


def _map_to_gp_type(
    kind: str,
    source_type: str,
    precision: int | None,
    scale: int | None,
) -> str:
    if kind == "boolean":
        return "BOOLEAN"
    if kind == "integer":
        if "small" in source_type or source_type in {"int16", "uint8"}:
            return "SMALLINT"
        if source_type in {"integer", "int", "int4", "int32", "uint16"}:
            return "INTEGER"
        if source_type in {"uint32"}:
            return "BIGINT"
        if source_type in {"uint64"}:
            return "NUMERIC(20, 0)"
        return "BIGINT"
    if kind == "float":
        if source_type in {"real", "float4", "float32"}:
            return "REAL"
        return "DOUBLE PRECISION"
    if kind == "decimal":
        return _decimal_type(
            "NUMERIC",
            precision,
            scale,
            fallback="NUMERIC",
            max_precision=_GP_MAX_NUMERIC_PRECISION,
        )
    if kind == "date":
        return "DATE"
    if kind == "timestamp":
        if "with time zone" in source_type or "timestamptz" in source_type:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"
    return "TEXT"


def _map_to_trino_type(
    kind: str,
    source_type: str,
    precision: int | None,
    scale: int | None,
) -> str:
    if kind == "boolean":
        return "BOOLEAN"
    if kind == "integer":
        if "tiny" in source_type or source_type in {"int8", "uint8"}:
            return "TINYINT"
        if "small" in source_type or source_type in {"int16", "uint16"}:
            return "SMALLINT"
        if source_type in {"integer", "int", "int4", "int32", "uint32"}:
            return "INTEGER" if source_type != "uint32" else "BIGINT"
        if source_type == "uint64":
            return "DECIMAL(20, 0)"
        return "BIGINT"
    if kind == "float":
        if source_type in {"real", "float4", "float32"}:
            return "REAL"
        return "DOUBLE"
    if kind == "decimal":
        return _decimal_type(
            "DECIMAL",
            precision,
            scale,
            fallback="DECIMAL(38, 10)",
            max_precision=_TRINO_MAX_DECIMAL_PRECISION,
        )
    if kind == "date":
        return "DATE"
    if kind == "timestamp":
        if "with time zone" in source_type or "timestamptz" in source_type:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"
    return "VARCHAR"


def _map_to_ch_base_type(
    kind: str,
    source_type: str,
    precision: int | None,
    scale: int | None,
) -> str:
    if kind == "boolean":
        return "Bool"
    if kind == "integer":
        if source_type.startswith("u"):
            if "8" in source_type:
                return "UInt8"
            if "16" in source_type:
                return "UInt16"
            if "32" in source_type:
                return "UInt32"
            return "UInt64"
        if "8" in source_type and "64" not in source_type:
            return "Int8"
        if "16" in source_type or "small" in source_type:
            return "Int16"
        if "32" in source_type or source_type in {"integer", "int", "int4"}:
            return "Int32"
        return "Int64"
    if kind == "float":
        if source_type in {"real", "float4", "float32"}:
            return "Float32"
        return "Float64"
    if kind == "decimal":
        return _decimal_type(
            "Decimal",
            precision,
            scale,
            fallback="Decimal(38, 10)",
            max_precision=_CLICKHOUSE_MAX_DECIMAL_PRECISION,
        )
    if kind == "date":
        return "Date"
    if kind == "timestamp":
        return "DateTime64(6)"
    return "String"


def _decimal_type(
    name: str,
    precision: int | None,
    scale: int | None,
    fallback: str,
    max_precision: int,
) -> str:
    if (
        precision is None
        or scale is None
        or precision < 1
        or precision > max_precision
        or scale < 0
        or scale > precision
    ):
        return fallback
    return f"{name}({precision}, {scale})"


def _nullable_ch_type(base_type: str) -> str:
    if base_type.startswith("Nullable("):
        return base_type
    return f"Nullable({base_type})"


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
