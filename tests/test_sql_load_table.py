from __future__ import annotations

import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.sql_fakes import FakeDbapiConnection

CURRENT_DT = date.today().strftime("%Y%m%d")
TEST_CH_TABLE = f"test_table_{CURRENT_DT}"
TEST_CH_SHARD_TABLE = f"test_table_{CURRENT_DT}_shard"
TEST_CH_STAGE_TABLE = f"test_table_{CURRENT_DT}__stage__abcd1234"
TEST_CH_SHARD_RELATION = f"test_table_{CURRENT_DT}_shard"

create_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.ddl.create_sql_table"
)
load_sql_table_module = importlib.import_module(
    "analytics_toolkit.sql.dml.load.load_sql_table"
)
load_df_module = importlib.import_module("analytics_toolkit.sql.dml.load.load_df")
table_ops_module = importlib.import_module("analytics_toolkit.sql.dml.table.table_ops")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.commands: list[str] = []
        self.queries: list[str] = []
        self.close_calls = 0

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str) -> object:
        self.queries.append(sql)
        return type("FakeResult", (), {"result_rows": [(1,)]})()

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        column_names: list[str],
    ) -> None:
        self.calls.append(
            {
                "table": table,
                "df": df.copy(),
                "column_names": list(column_names),
            }
        )

    def insert(
        self,
        table: str,
        data: list[tuple[object, ...]],
        column_names: list[str],
        column_type_names: list[str] | None = None,
    ) -> None:
        self.calls.append(
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


def test_insert_table_batch_normalizes_decimal_for_clickhouse() -> None:
    client = FakeClickHouseClient()
    connection_ref = {"connection": client}
    batch = pd.DataFrame(
        {
            "amount": [Decimal("1.20"), None],
            "label": ["ok", None],
            "count": [1, 2],
        }
    )

    inserted_rows = load_sql_table_module.insert_table_batch(
        connection_type="ch",
        connection_ref=connection_ref,
        table_name="schema.stage_table",
        batch=batch,
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
    )

    assert inserted_rows == 2
    assert len(client.calls) == 1

    inserted_df = client.calls[0]["df"]
    assert isinstance(inserted_df, pd.DataFrame)
    assert inserted_df["amount"].tolist() == [1.2, None]
    assert inserted_df["label"].tolist() == ["ok", None]
    assert inserted_df["count"].tolist() == [1, 2]


def test_insert_rows_batch_gp_uses_row_tuples_and_normalizes_nulls(monkeypatch) -> None:
    connection = FakeDbapiConnection()
    captured: dict[str, object] = {}

    def fake_execute_values(cursor, sql, rows, page_size):
        captured["sql"] = sql
        captured["rows"] = list(rows)
        captured["page_size"] = page_size

    monkeypatch.setattr(load_sql_table_module, "execute_values", fake_execute_values)

    inserted_rows = load_sql_table_module.insert_rows_batch(
        connection_type="gp",
        connection_ref={"connection": connection},
        table_name="schema.stage_table",
        columns=["id", "value"],
        rows=[(1, pd.NA), (2, float("nan"))],
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
    )

    assert inserted_rows == 2
    assert captured["sql"] == 'INSERT INTO schema.stage_table ("id", "value") VALUES %s'
    assert captured["rows"] == [(1, None), (2, None)]
    assert captured["page_size"] == 2
    assert connection.commit_calls == 1


def test_insert_rows_batch_gp_honors_insert_chunk_size(monkeypatch) -> None:
    connection = FakeDbapiConnection()
    captured: dict[str, object] = {}

    def fake_execute_values(cursor, sql, rows, page_size):
        del cursor, sql
        captured["rows"] = list(rows)
        captured["page_size"] = page_size

    monkeypatch.setattr(load_sql_table_module, "execute_values", fake_execute_values)

    inserted_rows = load_sql_table_module.insert_rows_batch(
        connection_type="gp",
        connection_ref={"connection": connection},
        table_name="schema.stage_table",
        columns=["id"],
        rows=[(1,), (2,), (3,)],
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
        gp_insert_chunk_size=2,
    )

    assert inserted_rows == 3
    assert captured["rows"] == [(1,), (2,), (3,)]
    assert captured["page_size"] == 2
    assert connection.commit_calls == 1


def test_insert_rows_batch_trino_normalizes_values_and_splits_chunks() -> None:
    connection = FakeDbapiConnection()

    inserted_rows = load_sql_table_module.insert_rows_batch(
        connection_type="trino",
        connection_ref={"connection": connection},
        table_name="schema.stage_table",
        columns=["id", "label"],
        rows=[(1.9, "a"), (2, pd.NA), (3, "c")],
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
        target_column_types={"id": "bigint", "label": "varchar"},
        trino_insert_chunk_size=2,
    )

    assert inserted_rows == 3
    assert connection.executed == [
        'INSERT INTO schema.stage_table ("id", "label") VALUES (?, ?), (?, ?)',
        'INSERT INTO schema.stage_table ("id", "label") VALUES (?, ?)',
    ]
    assert connection.executed_params == [
        [1, "a", 2, None],
        [3, "c"],
    ]


def test_insert_rows_batch_clickhouse_uses_rows_and_column_type_names() -> None:
    client = FakeClickHouseClient()

    inserted_rows = load_sql_table_module.insert_rows_batch(
        connection_type="ch",
        connection_ref={"connection": client},
        table_name="schema.stage_table",
        columns=["amount", "label"],
        rows=[(Decimal("1.20"), "ok"), (None, pd.NA)],
        retry_fn=lambda **kwargs: kwargs["operation"](1),
        retry_cnt=1,
        timeout_increment=0,
        target_column_types={
            "amount": "Nullable(Decimal(10, 2))",
            "label": "Nullable(String)",
        },
    )

    assert inserted_rows == 2
    assert client.calls == [
        {
            "table": "schema.stage_table",
            "data": [(1.2, "ok"), (None, None)],
            "column_names": ["amount", "label"],
            "column_type_names": [
                "Nullable(Decimal(10, 2))",
                "Nullable(String)",
            ],
        }
    ]


def test_batch_insert_sql_builders_preserve_backend_shapes() -> None:
    assert load_sql_table_module.build_gp_batch_insert_sql(
        "schema.stage_table",
        ["id", "value"],
        query_label="load-stage",
    ) == (
        "/* analytics_toolkit query_label=load-stage */\n"
        'INSERT INTO schema.stage_table ("id", "value") VALUES %s'
    )

    assert load_sql_table_module.build_trino_batch_insert_sql(
        "schema.stage_table",
        ["id", "value"],
        row_count=2,
    ) == (
        'INSERT INTO schema.stage_table ("id", "value") '
        "VALUES (?, ?), (?, ?)"
    )


def test_build_create_table_sql_uses_float64_for_decimal_clickhouse_columns() -> None:
    batch = pd.DataFrame(
        {
            "amount": [Decimal("1.20"), Decimal("2.50"), None],
            "label": ["ok", "still ok", None],
        }
    )

    sql = create_sql_table_module.build_create_table_sql(
        connection_type="ch",
        table_name="schema.stage_table",
        batch=batch,
    )

    assert "`amount` Nullable(Float64)" in sql
    assert "`label` Nullable(String)" in sql


def test_build_create_table_sql_uses_explicit_column_types() -> None:
    batch = pd.DataFrame(
        {
            "amount": ["1.20"],
            "created_at": ["2024-01-01"],
        }
    )

    sql = create_sql_table_module.build_create_table_sql(
        connection_type="gp",
        table_name="schema.stage_table",
        batch=batch,
        column_types={
            "amount": "NUMERIC(12, 2)",
            "created_at": "TIMESTAMP",
        },
    )

    assert '"amount" NUMERIC(12, 2)' in sql
    assert '"created_at" TIMESTAMP' in sql
    assert "appendonly=true" in sql
    assert "blocksize=32768" in sql
    assert "compresstype=zstd" in sql
    assert "compresslevel=4" in sql
    assert "orientation=column" in sql


def test_build_create_table_sqls_creates_clickhouse_distributed_pair() -> None:
    batch = pd.DataFrame(
        {
            "min_month_use": [date(2024, 1, 1)],
            "month_date": [date(2024, 2, 1)],
            "users": [10],
        }
    )

    sqls = create_sql_table_module.build_create_table_sqls(
        connection_type="ch",
        table_name=TEST_CH_TABLE,
        batch=batch,
        ch_distributed_table=True,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        ch_sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert len(sqls) == 3
    shard_sql, distributed_sql, local_distributed_sql = sqls
    assert "SETTINGS index_granularity" not in "\n".join(sqls)
    assert shard_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}"
    )
    assert "ON CLUSTER '{cluster}'" in shard_sql
    assert "ENGINE = ReplicatedMergeTree" in shard_sql
    assert "PARTITION BY `month_date`" in shard_sql
    assert "ORDER BY (`month_date`, `min_month_use`)" in shard_sql
    assert distributed_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}"
    )
    assert f"AS {TEST_CH_SHARD_TABLE}" not in distributed_sql
    assert "`min_month_use` Date" in distributed_sql
    assert "`month_date` Date" in distributed_sql
    assert "ENGINE = Distributed(" in distributed_sql
    assert "    '{cluster}'," in distributed_sql
    assert "    currentDatabase()," in distributed_sql
    assert f"    '{TEST_CH_SHARD_RELATION}'," in distributed_sql
    assert "    cityHash64(month_date, min_month_use)" in distributed_sql
    assert local_distributed_sql.startswith(
        f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}"
    )
    assert "ON CLUSTER" not in local_distributed_sql
    assert "ENGINE = Distributed(" in local_distributed_sql


def test_load_df_clickhouse_creates_pair_and_loads_distributed_table(monkeypatch) -> None:
    client = FakeClickHouseClient()
    batch = pd.DataFrame(
        {
            "month_date": [date(2024, 2, 1)],
            "min_month_use": [date(2024, 1, 1)],
            "users": [10],
        }
    )

    monkeypatch.setattr(
        load_df_module,
        "get_sql_connection",
        lambda connection_type: client,
    )
    monkeypatch.setattr(load_df_module, "table_exists", lambda *args, **kwargs: False)

    inserted_rows = load_df_module.load_df(
        "ch",
        TEST_CH_TABLE,
        batch,
        retry_cnt=1,
        timeout_increment=0,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert inserted_rows == 1
    assert f"DROP TABLE IF EXISTS {TEST_CH_TABLE}" in client.commands
    assert f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE}" in client.commands
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_TABLE} ON CLUSTER '{{cluster}}'"
        in client.commands
    )
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE} ON CLUSTER '{{cluster}}'"
        in client.commands
    )
    assert "SETTINGS index_granularity" not in "\n".join(client.commands)
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}")
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER '{cluster}'" in command
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER" not in command
        for command in client.commands
    )
    assert client.calls[0]["table"] == TEST_CH_TABLE
    assert client.close_calls == 1


def test_finalize_stage_table_clickhouse_recreates_pair_and_inserts_target() -> None:
    client = FakeClickHouseClient()
    batch = pd.DataFrame(
        {
            "month_date": [date(2024, 2, 1)],
            "min_month_use": [date(2024, 1, 1)],
            "users": [10],
        }
    )

    table_ops_module.finalize_stage_table(
        connection_type="ch",
        connection=client,
        stage_table=TEST_CH_STAGE_TABLE,
        target_table=TEST_CH_TABLE,
        replace_target_table=True,
        target_exists=True,
        sample_batch=batch,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date", "min_month_use"],
        ch_sharding_key="cityHash64(month_date, min_month_use)",
    )

    assert f"DROP TABLE IF EXISTS {TEST_CH_TABLE}" in client.commands
    assert f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE}" in client.commands
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_TABLE} ON CLUSTER '{{cluster}}'"
        in client.commands
    )
    assert (
        f"DROP TABLE IF EXISTS {TEST_CH_SHARD_TABLE} ON CLUSTER '{{cluster}}'"
        in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_SHARD_TABLE}")
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER '{cluster}'" in command
        for command in client.commands
    )
    assert any(
        command.startswith(f"CREATE TABLE IF NOT EXISTS {TEST_CH_TABLE}")
        and "ON CLUSTER" not in command
        for command in client.commands
    )
    assert client.commands[-1] == (
        f"INSERT INTO {TEST_CH_TABLE} "
        f"SELECT * FROM {TEST_CH_STAGE_TABLE}"
    )


def test_finalize_stage_table_clickhouse_uses_explicit_types_and_casts_insert() -> None:
    client = FakeClickHouseClient()
    batch = pd.DataFrame(
        {
            "month_date": ["2024-02-01"],
            "users": ["10"],
        }
    )
    column_types = {
        "month_date": "Nullable(Date)",
        "users": "Nullable(Int64)",
    }

    table_ops_module.finalize_stage_table(
        connection_type="ch",
        connection=client,
        stage_table=TEST_CH_STAGE_TABLE,
        target_table=TEST_CH_TABLE,
        replace_target_table=True,
        target_exists=True,
        sample_batch=batch,
        target_column_types=column_types,
        insert_column_types=column_types,
        ch_partition_by=["month_date"],
        ch_order_by=["month_date"],
    )

    create_sql = "\n".join(client.commands)
    assert "`month_date` Nullable(Date)" in create_sql
    assert "`users` Nullable(Int64)" in create_sql
    assert client.commands[-1] == (
        f"INSERT INTO {TEST_CH_TABLE} (`month_date`, `users`) "
        f"SELECT CAST(`month_date` AS Nullable(Date)) AS `month_date`, "
        f"CAST(`users` AS Nullable(Int64)) AS `users` "
        f"FROM {TEST_CH_STAGE_TABLE}"
    )
