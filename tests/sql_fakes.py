from __future__ import annotations

from typing import Any

import pandas as pd


class FakeDbapiCursor:
    def __init__(
        self,
        connection: FakeDbapiConnection,
        rows: list[tuple[Any, ...]] | None = None,
        description: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.connection = connection
        self._rows = rows or []
        self.description = description or []
        self.rowcount = -1
        self.close_calls = 0

    def __enter__(self) -> FakeDbapiCursor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.connection.executed.append(sql)
        self.connection.executed_params.append(params)
        if sql.startswith("INSERT INTO "):
            self.rowcount = self.connection.insert_rowcount

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self._rows:
            return None
        return self._rows[0]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch = self._rows[:size]
        self._rows = self._rows[size:]
        return batch

    def close(self) -> None:
        self.close_calls += 1


class FakeDbapiConnection:
    def __init__(
        self,
        rows: list[tuple[Any, ...]] | None = None,
        description: list[tuple[Any, ...]] | None = None,
        insert_rowcount: int = 0,
    ) -> None:
        self.rows = rows or []
        self.description = description or []
        self.insert_rowcount = insert_rowcount
        self.executed: list[str] = []
        self.executed_params: list[tuple[Any, ...] | None] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self) -> FakeDbapiCursor:
        return FakeDbapiCursor(
            self,
            rows=self.rows.copy(),
            description=self.description.copy(),
        )

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class FakeClickHouseResult:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.result_rows = rows or []


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.queries: list[str] = []
        self.inserts: list[dict[str, object]] = []
        self.close_calls = 0

    def command(
        self,
        sql: str,
        settings: dict[str, object] | None = None,
    ) -> dict[str, int] | None:
        del settings
        self.commands.append(sql)
        if sql.startswith("INSERT INTO "):
            return {"written_rows": 1}
        return None

    def query(self, sql: str) -> FakeClickHouseResult:
        self.queries.append(sql)
        if "count()" in sql:
            return FakeClickHouseResult([(1,)])
        if sql.startswith("EXISTS TABLE "):
            return FakeClickHouseResult([(1,)])
        return FakeClickHouseResult([])

    def query_df(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        return pd.DataFrame({"value": [1]})

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        column_names: list[str],
    ) -> None:
        self.inserts.append(
            {
                "table": table,
                "df": df.copy(),
                "column_names": list(column_names),
            }
        )

    def insert(
        self,
        table: str,
        data: list[tuple[Any, ...]],
        column_names: list[str],
        column_type_names: list[str] | None = None,
    ) -> None:
        self.inserts.append(
            {
                "table": table,
                "data": list(data),
                "column_names": list(column_names),
                "column_type_names": (
                    list(column_type_names)
                    if column_type_names is not None
                    else None
                ),
            }
        )

    def close(self) -> None:
        self.close_calls += 1
