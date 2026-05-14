from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..labels import apply_query_label
from .dbapi import DbApiBackendAdapter


class GreenplumAdapter(DbApiBackendAdapter):
    def __init__(self) -> None:
        super().__init__(backend="gp", commit_commands=True)

    def table_exists(
        self,
        connection: Any,
        table_name: str,
        *,
        connection_key: str,
    ) -> bool:
        del connection_key
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT to_regclass(%s)", (table_name,))
            row = cursor.fetchone()
            return bool(row and row[0])
        finally:
            cursor.close()

    def clear_table_sqls(
        self,
        table_name: str,
        *,
        query_label: str | None = None,
    ) -> list[str]:
        return [apply_query_label(f"TRUNCATE TABLE {table_name}", query_label)]

    def build_dataframe_batch_insert_sql(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        row_count: int,
        query_label: str | None = None,
    ) -> str:
        del row_count
        return apply_query_label(
            f"INSERT INTO {table_name} ({self.column_list_sql(columns)}) VALUES %s",
            query_label,
        )

    def get_table_column_types(
        self,
        connection: Any,
        table_name: str,
        *,
        connection_key: str,
    ) -> dict[str, str]:
        del connection_key
        schema_name, relation_name = split_gp_table_name(table_name)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """.strip(),
                (schema_name, relation_name),
            )
            return {
                str(column_name): format_gp_information_schema_type(
                    str(data_type),
                    udt_name,
                    numeric_precision,
                    numeric_scale,
                )
                for (
                    column_name,
                    data_type,
                    udt_name,
                    numeric_precision,
                    numeric_scale,
                ) in cursor.fetchall()
            }
        finally:
            cursor.close()


def split_gp_table_name(table_name: str) -> tuple[str, str]:
    parts = [part.strip().strip('"') for part in table_name.split(".") if part.strip()]
    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid Greenplum table name: {table_name}")


def format_gp_information_schema_type(
    data_type: str,
    udt_name: Any,
    numeric_precision: Any,
    numeric_scale: Any,
) -> str:
    normalized = data_type.lower()
    if normalized == "numeric" and numeric_precision is not None:
        if numeric_scale is None:
            return f"NUMERIC({numeric_precision})"
        return f"NUMERIC({numeric_precision}, {numeric_scale})"
    if normalized == "character varying":
        return "VARCHAR"
    if normalized == "timestamp without time zone":
        return "TIMESTAMP"
    if normalized == "timestamp with time zone":
        return "TIMESTAMP WITH TIME ZONE"
    if normalized == "integer":
        return "INTEGER"
    if normalized == "bigint":
        return "BIGINT"
    if normalized == "smallint":
        return "SMALLINT"
    if normalized == "boolean":
        return "BOOLEAN"
    if normalized == "date":
        return "DATE"
    if normalized == "text":
        return "TEXT"
    return str(udt_name or data_type).upper()
