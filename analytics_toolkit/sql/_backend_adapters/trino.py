from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..connection.config import TrinoConfig, get_connection_config
from ..labels import apply_query_label
from .dbapi import DbApiBackendAdapter


class TrinoAdapter(DbApiBackendAdapter):
    def __init__(self) -> None:
        super().__init__(backend="trino", commit_commands=False)

    def table_exists(
        self,
        connection: Any,
        table_name: str,
        *,
        connection_key: str,
    ) -> bool:
        catalog, schema_name, relation_name = split_trino_table_name(
            table_name,
            connection_key=connection_key,
        )
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT 1
                FROM {catalog}.information_schema.tables
                WHERE table_schema = ?
                  AND table_name = ?
                """.strip(),
                (schema_name, relation_name),
            )
            return cursor.fetchone() is not None
        finally:
            cursor.close()

    def clear_table_sqls(
        self,
        table_name: str,
        *,
        query_label: str | None = None,
    ) -> list[str]:
        return [apply_query_label(f"DELETE FROM {table_name}", query_label)]

    def build_dataframe_batch_insert_sql(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        row_count: int,
        query_label: str | None = None,
    ) -> str:
        if row_count <= 0:
            raise ValueError("row_count must be a positive integer.")

        row_placeholders = f"({', '.join('?' for _ in columns)})"
        values_sql = ", ".join(row_placeholders for _ in range(row_count))
        return apply_query_label(
            f"INSERT INTO {table_name} ({self.column_list_sql(columns)}) "
            f"VALUES {values_sql}",
            query_label,
        )

    def get_table_column_types(
        self,
        connection: Any,
        table_name: str,
        *,
        connection_key: str,
    ) -> dict[str, str]:
        catalog, schema_name, relation_name = split_trino_table_name(
            table_name,
            connection_key=connection_key,
        )
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT column_name, data_type
                FROM {catalog}.information_schema.columns
                WHERE table_schema = ?
                  AND table_name = ?
                ORDER BY ordinal_position
                """.strip(),
                (schema_name, relation_name),
            )
            return {
                str(column_name): str(data_type)
                for column_name, data_type in cursor.fetchall()
            }
        finally:
            cursor.close()


def split_trino_table_name(
    table_name: str,
    connection_key: str = "trino",
) -> tuple[str, str, str]:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]

    config = get_connection_config(connection_key)
    if not isinstance(config, TrinoConfig):
        raise ValueError("Invalid Trino configuration.")

    if len(parts) == 2:
        if not config.catalog:
            raise ValueError(
                f"Trino table operations for schema-qualified names require "
                f".connections['{config.connection_key}'].catalog."
            )
        return config.catalog, parts[0], parts[1]
    if len(parts) == 1:
        if not config.catalog or not config.schema:
            raise ValueError(
                f"Trino table operations for unqualified names require "
                f".connections['{config.connection_key}'].catalog and schema."
            )
        return config.catalog, config.schema, parts[0]
    raise ValueError(f"Invalid table name: {table_name}")
