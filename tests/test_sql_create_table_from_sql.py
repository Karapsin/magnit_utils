from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

create_module = importlib.import_module(
    "analytics_toolkit.sql.dml.table.create_table_from_sql"
)
sql_module = importlib.import_module("analytics_toolkit.sql")


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class FakeDbapiCursor:
    def __init__(self, connection: FakeDbapiConnection) -> None:
        self.connection = connection
        self.description = connection.description
        self.rowcount = -1
        self.close_calls = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.connection.executed.append(sql)
        self.connection.executed_params.append(params)
        if sql.startswith("INSERT INTO "):
            self.rowcount = self.connection.insert_rowcount

    def fetchone(self) -> tuple[Any, ...] | None:
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []

    def close(self) -> None:
        self.close_calls += 1


class FakeDbapiConnection:
    def __init__(
        self,
        description: list[tuple[Any, ...]] | None = None,
        insert_rowcount: int = 0,
    ) -> None:
        self.description = description or []
        self.insert_rowcount = insert_rowcount
        self.executed: list[str] = []
        self.executed_params: list[tuple[Any, ...] | None] = []
        self.cursors: list[FakeDbapiCursor] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self) -> FakeDbapiCursor:
        cursor = FakeDbapiCursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class FakeClickHouseClient:
    def __init__(self, insert_rowcount: int = 0) -> None:
        self.insert_rowcount = insert_rowcount
        self.commands: list[str] = []
        self.command_settings: list[dict[str, object] | None] = []
        self.queries: list[str] = []
        self.created_tables: set[str] = set()
        self.close_calls = 0

    def command(
        self,
        sql: str,
        settings: dict[str, object] | None = None,
    ) -> dict[str, int] | None:
        self.commands.append(sql)
        self.command_settings.append(settings)
        self._track_table_ddl(sql)
        if sql.startswith("INSERT INTO "):
            return {"written_rows": self.insert_rowcount}
        return None

    def query(self, sql: str) -> FakeResult:
        self.queries.append(sql)
        if sql.startswith("EXISTS TABLE "):
            table_name = sql.removeprefix("EXISTS TABLE ").strip()
            return FakeResult([(int(table_name in self.created_tables),)])
        return FakeResult([])

    def close(self) -> None:
        self.close_calls += 1

    def _track_table_ddl(self, sql: str) -> None:
        if sql.startswith("CREATE TABLE IF NOT EXISTS "):
            table_name = sql.removeprefix("CREATE TABLE IF NOT EXISTS ").split()[0]
            self.created_tables.add(table_name)
            return
        if sql.startswith("DROP TABLE IF EXISTS "):
            table_name = sql.removeprefix("DROP TABLE IF EXISTS ").split()[0]
            self.created_tables.discard(table_name)


SOURCE_DESCRIPTION = [
    ("id", 23, None, None, None, None),
    ("amount", 1700, None, None, 12, 2),
]


def test_create_table_from_sql_is_exported() -> None:
    assert sql_module.create_table_from_sql is create_module.create_table_from_sql


def test_schema_only_creation_uses_native_metadata_types(monkeypatch) -> None:
    connection = FakeDbapiConnection(description=SOURCE_DESCRIPTION)
    monkeypatch.setattr(
        create_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    result = create_module.create_table_from_sql(
        "gp",
        "sandbox.target_table",
        "select id, amount from source_table;",
    )

    assert result is None
    assert connection.executed[0] == (
        "SELECT * FROM (select id, amount from source_table) "
        "AS source_schema_probe WHERE 1 = 0"
    )
    assert any(
        sql.startswith("CREATE TABLE sandbox.target_table")
        and '"id" INTEGER' in sql
        and '"amount" NUMERIC(12, 2)' in sql
        for sql in connection.executed
    )
    assert not any(sql.startswith("DROP TABLE") for sql in connection.executed)
    assert connection.close_calls == 1


def test_schema_only_creation_falls_back_for_gp_unbounded_numeric(
    monkeypatch,
) -> None:
    connection = FakeDbapiConnection(
        description=[
            ("quantity", 1700, None, None, 65535, 0),
        ]
    )
    monkeypatch.setattr(
        create_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    create_module.create_table_from_sql(
        "gp",
        "sandbox.target_table",
        "select quantity from source_table;",
    )

    assert any(
        sql.startswith("CREATE TABLE sandbox.target_table")
        and '"quantity" NUMERIC' in sql
        and '"quantity" NUMERIC(' not in sql
        for sql in connection.executed
    )


def test_schema_only_creation_preserves_gp_bytea_columns(monkeypatch) -> None:
    connection = FakeDbapiConnection(
        description=[
            ("cheque_pk", 17, None, None, None, None),
            ("quantity", 1700, None, None, 12, 3),
        ]
    )
    monkeypatch.setattr(
        create_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    create_module.create_table_from_sql(
        "gp",
        "sandbox.target_table",
        "select cheque_pk, quantity from source_table;",
    )

    assert any(
        sql.startswith("CREATE TABLE sandbox.target_table")
        and '"cheque_pk" BYTEA' in sql
        and '"quantity" NUMERIC(12, 3)' in sql
        for sql in connection.executed
    )


def test_cross_backend_creation_maps_types_to_clickhouse_and_creates_pair(
    monkeypatch,
) -> None:
    source = FakeDbapiConnection(
        description=[
            ("id", 23, None, None, None, None),
            ("dt", 1082, None, None, None, None),
            ("amount", 1700, None, None, 12, 2),
        ]
    )
    target = FakeClickHouseClient()

    def fake_get_sql_connection(connection_key: str) -> object:
        if connection_key == "gp":
            return source
        if connection_key == "ch":
            return target
        raise AssertionError(f"Unexpected connection key: {connection_key}")

    monkeypatch.setattr(create_module, "get_sql_connection", fake_get_sql_connection)

    create_module.create_table_from_sql(
        "gp",
        "analytics.events",
        "select id, dt, amount from source_table",
        table_db="ch",
        ch_partition_by=["dt"],
        ch_order_by=["dt", "id"],
        sharding_key="cityHash64(id)",
    )

    assert not any(command.startswith("DROP TABLE") for command in target.commands)
    assert len(target.commands) == 3
    shard_sql, distributed_sql, local_distributed_sql = target.commands
    assert shard_sql.startswith("CREATE TABLE IF NOT EXISTS analytics.events_shard")
    assert "ON CLUSTER '{cluster}'" in shard_sql
    assert "`id` Nullable(Int32)" in shard_sql
    assert "`dt` Nullable(Date)" in shard_sql
    assert "`amount` Nullable(Decimal(12, 2))" in shard_sql
    assert "PARTITION BY `dt`" in shard_sql
    assert "ORDER BY (`dt`, `id`)" in shard_sql
    assert distributed_sql.startswith("CREATE TABLE IF NOT EXISTS analytics.events")
    assert "ENGINE = Distributed(" in distributed_sql
    assert "    'events_shard'," in distributed_sql
    assert "    cityHash64(id)" in distributed_sql
    assert "ON CLUSTER" not in local_distributed_sql
    assert target.queries[-1] == "EXISTS TABLE analytics.events"
    assert source.close_calls == 1
    assert target.close_calls == 1


def test_drop_target_if_exists_drops_trino_target_before_create(monkeypatch) -> None:
    source = FakeDbapiConnection(
        description=[
            ("id", 23, None, None, None, None),
            ("name", 25, None, None, None, None),
        ]
    )
    target = FakeDbapiConnection()

    def fake_get_sql_connection(connection_key: str) -> object:
        if connection_key == "gp":
            return source
        if connection_key == "trino":
            return target
        raise AssertionError(f"Unexpected connection key: {connection_key}")

    monkeypatch.setattr(create_module, "get_sql_connection", fake_get_sql_connection)

    create_module.create_table_from_sql(
        "gp",
        "sandbox.created_table",
        "select id, name from source_table",
        table_db="trino",
        drop_target_if_exists=True,
    )

    assert target.executed[0] == "DROP TABLE IF EXISTS sandbox.created_table"
    assert any(
        sql.startswith("CREATE TABLE sandbox.created_table")
        and '"id" INTEGER' in sql
        and '"name" VARCHAR' in sql
        for sql in target.executed
    )


def test_drop_target_if_exists_drops_clickhouse_distributed_pair(monkeypatch) -> None:
    source = FakeDbapiConnection(description=SOURCE_DESCRIPTION)
    target = FakeClickHouseClient()

    def fake_get_sql_connection(connection_key: str) -> object:
        if connection_key == "gp":
            return source
        if connection_key == "ch":
            return target
        raise AssertionError(f"Unexpected connection key: {connection_key}")

    monkeypatch.setattr(create_module, "get_sql_connection", fake_get_sql_connection)

    create_module.create_table_from_sql(
        "gp",
        "analytics.events",
        "select id, amount from source_table",
        table_db="ch",
        drop_target_if_exists=True,
    )

    assert target.commands[:4] == [
        "DROP TABLE IF EXISTS analytics.events",
        "DROP TABLE IF EXISTS analytics.events_shard",
        "DROP TABLE IF EXISTS analytics.events ON CLUSTER '{cluster}'",
        "DROP TABLE IF EXISTS analytics.events_shard ON CLUSTER '{cluster}'",
    ]


def test_insert_data_same_backend_emits_typed_insert_and_returns_rowcount(
    monkeypatch,
) -> None:
    connection = FakeDbapiConnection(
        description=SOURCE_DESCRIPTION,
        insert_rowcount=7,
    )
    monkeypatch.setattr(
        create_module,
        "get_sql_connection",
        lambda connection_key: connection,
    )

    inserted_rows = create_module.create_table_from_sql(
        "gp",
        "sandbox.target_table",
        "select id, amount from source_table",
        insert_data=True,
    )

    assert inserted_rows == 7
    assert connection.executed[-1] == (
        'INSERT INTO sandbox.target_table ("id", "amount") '
        'SELECT CAST("id" AS INTEGER) AS "id", '
        'CAST("amount" AS NUMERIC(12, 2)) AS "amount" '
        "FROM (select id, amount from source_table) AS source_query"
    )


def test_insert_data_cross_backend_delegates_to_transfer_after_creation(
    monkeypatch,
) -> None:
    source = FakeDbapiConnection(description=SOURCE_DESCRIPTION)
    target = FakeClickHouseClient()
    transfer_calls: list[dict[str, object]] = []

    def fake_get_sql_connection(connection_key: str) -> object:
        if connection_key == "gp":
            return source
        if connection_key == "ch":
            return target
        raise AssertionError(f"Unexpected connection key: {connection_key}")

    def fake_transfer_table(**kwargs: object) -> int:
        transfer_calls.append(kwargs)
        assert source.close_calls == 1
        assert target.close_calls == 1
        return 11

    monkeypatch.setattr(create_module, "get_sql_connection", fake_get_sql_connection)
    monkeypatch.setattr(create_module, "transfer_table", fake_transfer_table)

    inserted_rows = create_module.create_table_from_sql(
        "gp",
        "analytics.events",
        "select id, amount from source_table;",
        table_db="ch",
        insert_data=True,
        ch_order_by=["id"],
        trino_insert_chunk_size=500,
    )

    assert inserted_rows == 11
    assert transfer_calls == [
        {
            "from_db": "gp",
            "to_db": "ch",
            "from_sql": "select id, amount from source_table",
            "to_table": "analytics.events",
            "replace_target_table": False,
            "gp_distributed_by_key": None,
            "trino_insert_chunk_size": 500,
            "ch_partition_by": None,
            "ch_order_by": ["id"],
            "ch_engine": "ReplicatedMergeTree",
            "ch_cluster": "{cluster}",
            "sharding_key": "rand()",
        }
    ]
    assert any(
        command.startswith("CREATE TABLE IF NOT EXISTS analytics.events_shard")
        for command in target.commands
    )


def test_create_table_from_sql_validates_empty_inputs(monkeypatch) -> None:
    monkeypatch.setattr(
        create_module,
        "get_sql_connection",
        lambda connection_key: pytest.fail("connection should not be opened"),
    )

    with pytest.raises(create_module.InvalidSqlInputError, match="table_name"):
        create_module.create_table_from_sql("gp", " ", "select 1")

    with pytest.raises(create_module.InvalidSqlInputError, match="sql"):
        create_module.create_table_from_sql("gp", "target", " ")

    with pytest.raises(create_module.InvalidSqlInputError, match="exactly one"):
        create_module.create_table_from_sql("gp", "target", "select 1; select 2")
