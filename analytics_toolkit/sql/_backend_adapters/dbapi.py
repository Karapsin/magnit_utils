from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..connection.config import BackendName
from ..labels import apply_query_label
from .base import BackendAdapter
from .utils import extract_row_count


@dataclass(frozen=True)
class DbApiBackendAdapter(BackendAdapter):
    backend: BackendName
    commit_commands: bool

    def execute_command(self, connection: Any, sql: str) -> Any:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            if self.commit_commands:
                connection.commit()
            return cursor
        except Exception:
            if self.commit_commands:
                connection.rollback()
            raise
        finally:
            cursor.close()

    def execute_commands(self, connection: Any, sqls: list[str]) -> None:
        cursor = connection.cursor()
        try:
            for sql in sqls:
                cursor.execute(sql)
            if self.commit_commands:
                connection.commit()
        except Exception:
            if self.commit_commands:
                connection.rollback()
            raise
        finally:
            cursor.close()

    def insert_from_query(
        self,
        connection: Any,
        target_table: str,
        source_sql: str,
        column_types: Mapping[str, str],
        *,
        query_label: str | None = None,
    ) -> int:
        sql = apply_query_label(
            self.build_insert_from_query_sql(target_table, source_sql, column_types),
            query_label,
        )
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            row_count = extract_row_count(cursor)
            if self.commit_commands:
                connection.commit()
            return row_count
        except Exception:
            if self.commit_commands:
                connection.rollback()
            raise
        finally:
            cursor.close()
