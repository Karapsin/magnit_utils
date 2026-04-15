from __future__ import annotations

from typing import Any

import pandas as pd

from ...connection.config import TrinoConfig, get_connection_config
from ...ddl.create_sql_table import create_sql_table
from ...connection.errors import UnsupportedConnectionTypeError
from ...general.logging import time_print


def table_exists(connection_type: str, connection: Any, table_name: str) -> bool:
    if connection_type == "gp":
        return _gp_table_exists(connection, table_name)
    if connection_type == "trino":
        return _trino_table_exists(connection, table_name)
    if connection_type == "ch":
        return _ch_table_exists(connection, table_name)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def clear_target_table(connection_type: str, connection: Any, table_name: str) -> None:
    time_print(f"Clearing target table {table_name} on {connection_type}")

    if connection_type == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if connection_type == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(f"DELETE FROM {table_name}")
            return
        finally:
            cursor.close()

    if connection_type == "ch":
        connection.command(f"TRUNCATE TABLE {table_name}")
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def finalize_stage_table(
    connection_type: str,
    connection: Any,
    stage_table: str,
    target_table: str,
    replace_target_table: bool,
    target_exists: bool,
    sample_batch: pd.DataFrame,
    gp_distributed_by_key: list[str] | None = None,
) -> None:
    time_print(
        f"Finalizing staged transfer from {stage_table} into {target_table} on {connection_type}"
    )

    if not target_exists:
        create_sql_table(
            connection_type,
            connection,
            target_table,
            sample_batch,
            gp_distributed_by_key=gp_distributed_by_key,
        )
    elif replace_target_table:
        clear_target_table(connection_type, connection, target_table)

    insert_from_table(connection_type, connection, target_table, stage_table)


def analyze_table(
    connection_type: str,
    connection: Any,
    table_name: str,
) -> None:
    if connection_type == "ch":
        return

    time_print(f"Analyzing target table {table_name} on {connection_type}")
    sql = f"ANALYZE {table_name}"

    if connection_type == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if connection_type == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def drop_table_with_retry(
    connection_type: str,
    connection_ref: dict[str, Any],
    table_name: str,
    retry_fn: Any,
    retry_cnt: int,
    timeout_increment: int | float,
    rollback_fn: Any,
    replace_connection_fn: Any,
) -> None:
    def operation(attempt: int) -> None:
        connection = connection_ref["connection"]
        try:
            drop_table(connection_type, connection, table_name)
            return None
        except Exception:
            if connection_type == "gp":
                rollback_fn(connection)
            replace_connection_fn(connection_type, connection_ref)
            raise

    retry_fn(
        operation_name=f"dropping stage table {table_name} on {connection_type}",
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        operation=operation,
    )


def drop_table(connection_type: str, connection: Any, table_name: str) -> None:
    sql = f"DROP TABLE IF EXISTS {table_name}"

    if connection_type == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if connection_type == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    if connection_type == "ch":
        connection.command(sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def get_trino_table_column_types(connection: Any, table_name: str) -> dict[str, str]:
    catalog, schema_name, relation_name = split_trino_table_name(table_name)

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


def insert_from_table(
    connection_type: str,
    connection: Any,
    target_table: str,
    source_table: str,
) -> None:
    sql = f"INSERT INTO {target_table} SELECT * FROM {source_table}"

    if connection_type == "gp":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    if connection_type == "trino":
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return
        finally:
            cursor.close()

    if connection_type == "ch":
        connection.command(sql)
        return

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def split_trino_table_name(table_name: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    config = get_connection_config("trino")
    if not isinstance(config, TrinoConfig):
        raise ValueError("Invalid Trino configuration.")

    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        if not config.catalog:
            raise ValueError(
                "Trino table operations for schema-qualified names require TRINO_CATALOG."
            )
        return config.catalog, parts[0], parts[1]
    if len(parts) == 1:
        if not config.catalog or not config.schema:
            raise ValueError(
                "Trino table operations for unqualified names require TRINO_CATALOG and TRINO_SCHEMA."
            )
        return config.catalog, config.schema, parts[0]
    raise ValueError(f"Invalid table name: {table_name}")


def _gp_table_exists(connection: Any, table_name: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT to_regclass(%s)", (table_name,))
        row = cursor.fetchone()
        return bool(row and row[0])
    finally:
        cursor.close()


def _trino_table_exists(connection: Any, table_name: str) -> bool:
    catalog, schema_name, relation_name = split_trino_table_name(table_name)
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


def _ch_table_exists(client: Any, table_name: str) -> bool:
    result = client.query(f"EXISTS TABLE {table_name}")
    return bool(result.result_rows and result.result_rows[0][0])
