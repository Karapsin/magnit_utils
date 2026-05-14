from __future__ import annotations

import importlib
import inspect

from analytics_toolkit.sql.backend_adapters import BACKEND_ADAPTERS, get_backend_adapter
from tests.sql_fakes import FakeClickHouseResult, FakeDbapiConnection


sql_module = importlib.import_module("analytics_toolkit.sql")
table_ops_module = importlib.import_module("analytics_toolkit.sql.dml.table.table_ops")
ch_lifecycle_module = importlib.import_module("analytics_toolkit.sql.ch_lifecycle")


class RecordingClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict[str, object] | None]] = []
        self.queries: list[str] = []

    def command(
        self,
        sql: str,
        settings: dict[str, object] | None = None,
    ) -> dict[str, int] | None:
        self.commands.append((sql, settings))
        if sql.startswith("INSERT INTO "):
            return {"written_rows": 3}
        return None

    def query(self, sql: str) -> FakeClickHouseResult:
        self.queries.append(sql)
        if sql.startswith("EXISTS TABLE "):
            return FakeClickHouseResult([(1,)])
        if sql.startswith("SELECT count()"):
            return FakeClickHouseResult([(9,)])
        if sql.startswith("DESCRIBE TABLE "):
            return FakeClickHouseResult([("id", "Nullable(Int64)")])
        return FakeClickHouseResult([])


def test_sql_public_api_exports_are_stable() -> None:
    public_names = {
        "async_sql",
        "ch_create_table_as",
        "ch_full_table_move",
        "create_sql_table",
        "create_table_from_sql",
        "execute_read",
        "execute_sql",
        "get_sql_connection",
        "load_df",
        "read_sql",
        "transfer_table",
    }

    for name in public_names:
        assert name in sql_module.__all__
        assert callable(getattr(sql_module, name))

    assert list(inspect.signature(sql_module.load_df).parameters)[:3] == [
        "connection_type",
        "destination_table",
        "df",
    ]
    assert list(inspect.signature(sql_module.transfer_table).parameters)[:4] == [
        "from_db",
        "to_db",
        "from_sql",
        "to_table",
    ]


def test_table_ops_compatibility_helpers_remain_importable() -> None:
    helper_names = {
        "build_analyze_table_sql",
        "build_clear_table_sqls",
        "build_count_table_rows_sql",
        "build_drop_ch_distributed_table_pair_sqls",
        "build_drop_table_sql",
        "build_insert_from_query_sql",
        "build_insert_from_table_sql",
        "clear_target_table",
        "count_table_rows",
        "drop_table",
        "finalize_stage_table",
        "get_table_column_types",
        "get_trino_table_column_types",
        "insert_from_query",
        "insert_from_table",
        "table_exists",
        "_build_typed_insert_select_sql",
        "_ch_cluster_clause",
        "_execute_ch_command",
        "_gp_table_exists",
        "_trino_table_exists",
    }

    for name in helper_names:
        assert callable(getattr(table_ops_module, name))


def test_backend_adapter_registry_renders_existing_sql_shapes() -> None:
    assert set(BACKEND_ADAPTERS) == {"gp", "trino", "ch"}

    assert get_backend_adapter("gp").clear_table_sqls("schema.target") == [
        "TRUNCATE TABLE schema.target"
    ]
    assert get_backend_adapter("trino").clear_table_sqls("schema.target") == [
        "DELETE FROM schema.target"
    ]
    assert get_backend_adapter("ch").clear_table_sqls("db.target") == [
        "TRUNCATE TABLE IF EXISTS db.target"
    ]
    assert (
        get_backend_adapter("ch").drop_table_sql(
            "db.target",
            ch_cluster="{cluster}",
        )
        == "DROP TABLE IF EXISTS db.target ON CLUSTER '{cluster}'"
    )
    assert (
        get_backend_adapter("gp").build_insert_from_table_sql(
            "schema.target",
            "schema.stage",
            {"id": "BIGINT", "amount": "NUMERIC(12, 2)"},
        )
        == 'INSERT INTO schema.target ("id", "amount") '
        'SELECT CAST("id" AS BIGINT) AS "id", '
        'CAST("amount" AS NUMERIC(12, 2)) AS "amount" FROM schema.stage'
    )
    assert (
        get_backend_adapter("ch").count_table_rows_sql("db.target")
        == "SELECT count() FROM db.target"
    )
    assert get_backend_adapter("trino").build_dataframe_batch_insert_sql(
        "schema.stage",
        ["id", "name"],
        row_count=2,
    ) == (
        'INSERT INTO schema.stage ("id", "name") VALUES (?, ?), (?, ?)'
    )
    assert get_backend_adapter("gp").build_stage_duplicate_keys_sql(
        "schema.stage",
        ["id", "dt"],
    ) == (
        'SELECT 1 FROM schema.stage GROUP BY "id", "dt" '
        "HAVING COUNT(*) > 1 LIMIT 1"
    )
    assert get_backend_adapter("ch").build_stage_target_key_overlap_sql(
        "db.stage",
        "db.target",
        ["id"],
    ) == (
        "SELECT 1 FROM db.stage AS stage_src "
        "INNER JOIN db.target AS target_dst ON "
        "(stage_src.`id` = target_dst.`id` "
        "OR (stage_src.`id` IS NULL AND target_dst.`id` IS NULL)) "
        "LIMIT 1"
    )


def test_backend_adapters_execute_operations_like_existing_table_ops() -> None:
    gp_connection = FakeDbapiConnection(rows=[(5,)])
    get_backend_adapter("gp").clear_table(gp_connection, "schema.target")
    assert gp_connection.executed == ["TRUNCATE TABLE schema.target"]
    assert gp_connection.commit_calls == 1

    trino_connection = FakeDbapiConnection(rows=[(7,)])
    assert get_backend_adapter("trino").count_table_rows(
        trino_connection,
        "schema.target",
    ) == 7
    assert trino_connection.executed == ["SELECT COUNT(*) FROM schema.target"]
    assert trino_connection.commit_calls == 0

    ch_client = RecordingClickHouseClient()
    get_backend_adapter("ch").drop_table(
        ch_client,
        "db.target",
        ch_cluster="{cluster}",
    )
    assert ch_client.commands == [
        (
            "DROP TABLE IF EXISTS db.target ON CLUSTER '{cluster}'",
            {
                "distributed_ddl_task_timeout": 300,
                "distributed_ddl_output_mode": "none",
            },
        )
    ]
    assert get_backend_adapter("ch").count_table_rows(ch_client, "db.target") == 9
    assert ch_client.queries[-1] == "SELECT count() FROM db.target"


def test_clickhouse_lifecycle_builds_distributed_pair_sql_in_order() -> None:
    assert ch_lifecycle_module.build_drop_ch_distributed_table_pair_sqls(
        "db.target",
        ch_cluster="{cluster}",
    ) == [
        "DROP TABLE IF EXISTS db.target",
        "DROP TABLE IF EXISTS db.target_shard",
        "DROP TABLE IF EXISTS db.target ON CLUSTER '{cluster}'",
        "DROP TABLE IF EXISTS db.target_shard ON CLUSTER '{cluster}'",
    ]

    assert ch_lifecycle_module.build_truncate_ch_distributed_table_pair_sqls(
        "db.target",
        ch_cluster="analytics",
    ) == [
        "TRUNCATE TABLE IF EXISTS db.target_shard ON CLUSTER analytics",
        "TRUNCATE TABLE IF EXISTS db.target",
    ]

    create_sqls = ch_lifecycle_module.build_create_ch_distributed_table_pair_sqls(
        table_name="db.target",
        joined_columns="`id` UInt64",
        ch_partition_by=["id"],
        ch_order_by=["id"],
        ch_cluster="{cluster}",
        ch_sharding_key="cityHash64(id)",
    )
    assert len(create_sqls) == 3
    assert create_sqls[0].startswith("CREATE TABLE IF NOT EXISTS db.target_shard")
    assert create_sqls[1].startswith("CREATE TABLE IF NOT EXISTS db.target")
    assert "ON CLUSTER '{cluster}'" in create_sqls[1]
    assert create_sqls[2].startswith("CREATE TABLE IF NOT EXISTS db.target")
    assert "ON CLUSTER" not in create_sqls[2]


def test_clickhouse_lifecycle_executes_on_cluster_settings() -> None:
    client = RecordingClickHouseClient()

    ch_lifecycle_module.drop_ch_distributed_table_pair(
        client,
        "db.target",
        ch_cluster="{cluster}",
    )

    assert client.commands[2] == (
        "DROP TABLE IF EXISTS db.target ON CLUSTER '{cluster}'",
        {
            "distributed_ddl_task_timeout": 300,
            "distributed_ddl_output_mode": "none",
        },
    )


def test_target_lifecycle_helper_preserves_non_ch_replace_modes() -> None:
    drop_connection = FakeDbapiConnection()
    target_exists = table_ops_module.apply_target_write_mode(
        "gp",
        drop_connection,
        "schema.target",
        write_mode="replace",
        target_exists=True,
        replace_existing_non_ch="drop",
    )
    assert target_exists is False
    assert drop_connection.executed == ["DROP TABLE IF EXISTS schema.target"]

    clear_connection = FakeDbapiConnection()
    target_exists = table_ops_module.apply_target_write_mode(
        "gp",
        clear_connection,
        "schema.target",
        write_mode="replace",
        target_exists=True,
        replace_existing_non_ch="clear",
    )
    assert target_exists is True
    assert clear_connection.executed == ["TRUNCATE TABLE schema.target"]


def test_target_lifecycle_can_preserve_load_df_ch_truncate_missing_target() -> None:
    client = RecordingClickHouseClient()

    target_exists = table_ops_module.apply_target_write_mode(
        "ch",
        client,
        "db.target",
        write_mode="truncate_insert",
        target_exists=False,
        replace_existing_non_ch="drop",
        drop_missing_ch_truncate_target=False,
    )

    assert target_exists is False
    assert client.commands == []


def test_backend_adapters_execute_validation_queries_per_backend() -> None:
    gp_connection = FakeDbapiConnection(rows=[(1,)])
    assert get_backend_adapter("gp").stage_has_duplicate_keys(
        gp_connection,
        "schema.stage",
        ["id"],
    )
    assert gp_connection.executed == [
        'SELECT 1 FROM schema.stage GROUP BY "id" HAVING COUNT(*) > 1 LIMIT 1'
    ]

    ch_client = RecordingClickHouseClient()
    assert get_backend_adapter("ch").stage_keys_overlap_target(
        ch_client,
        "db.stage",
        "db.target",
        ["id"],
    ) is False
    assert ch_client.queries[-1] == (
        "SELECT 1 FROM db.stage AS stage_src "
        "INNER JOIN db.target AS target_dst ON "
        "(stage_src.`id` = target_dst.`id` "
        "OR (stage_src.`id` IS NULL AND target_dst.`id` IS NULL)) "
        "LIMIT 1"
    )


def test_dbapi_backend_adapter_rolls_back_failed_committed_commands() -> None:
    class FailingCursor:
        def __init__(self, connection: FailingConnection) -> None:
            self.connection = connection

        def execute(self, sql: str) -> None:
            self.connection.executed.append(sql)
            raise RuntimeError("boom")

        def close(self) -> None:
            self.connection.cursor_closed = True

    class FailingConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.commit_calls = 0
            self.rollback_calls = 0
            self.cursor_closed = False

        def cursor(self) -> FailingCursor:
            return FailingCursor(self)

        def commit(self) -> None:
            self.commit_calls += 1

        def rollback(self) -> None:
            self.rollback_calls += 1

    connection = FailingConnection()

    try:
        get_backend_adapter("gp").execute_command(connection, "DROP TABLE target")
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected failing execute to raise.")

    assert connection.executed == ["DROP TABLE target"]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1
    assert connection.cursor_closed is True


def test_backend_adapter_insert_from_query_returns_backend_row_counts() -> None:
    class RowCountCursorConnection(FakeDbapiConnection):
        def __init__(self) -> None:
            super().__init__()
            self.insert_rowcount = 4

    gp_connection = RowCountCursorConnection()
    assert get_backend_adapter("gp").insert_from_query(
        gp_connection,
        "schema.target",
        "select id from source",
        {"id": "BIGINT"},
    ) == 4
    assert gp_connection.commit_calls == 1

    ch_client = RecordingClickHouseClient()
    assert get_backend_adapter("ch").insert_from_query(
        ch_client,
        "db.target",
        "select id from source",
        {"id": "Nullable(Int64)"},
    ) == 3
    assert ch_client.commands[-1][0] == (
        "INSERT INTO db.target (`id`) "
        "SELECT CAST(`id` AS Nullable(Int64)) AS `id` "
        "FROM (select id from source) AS source_query"
    )
