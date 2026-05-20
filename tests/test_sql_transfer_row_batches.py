from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

attempt_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.attempt"
)
estimate_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.estimate"
)
transfer_api_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.flow.api"
)
models_module = importlib.import_module(
    "analytics_toolkit.sql.dml.transfer.runtime.models"
)


class RecordingSourceCursor:
    def __init__(self, rows: list[tuple[int]]) -> None:
        self._rows = rows
        self.description = [("id", 23, None, None, None, None)]
        self.fetch_sizes: list[int] = []
        self.executed: list[str] = []
        self.close_calls = 0

    def execute(self, query: str) -> None:
        self.executed.append(query)

    def fetchmany(self, size: int) -> list[tuple[int]]:
        self.fetch_sizes.append(size)
        batch = self._rows[:size]
        self._rows = self._rows[size:]
        return batch

    def close(self) -> None:
        self.close_calls += 1


class RecordingSourceConnection:
    def __init__(self, rows: list[tuple[int]]) -> None:
        self.cursor_obj = RecordingSourceCursor(rows)

    def cursor(self) -> RecordingSourceCursor:
        return self.cursor_obj


class StaticDbapiCursor:
    def __init__(
        self,
        connection: StaticDbapiConnection,
        rows: list[tuple[Any, ...]],
    ) -> None:
        self.connection = connection
        self._rows = rows
        self.close_calls = 0

    def execute(self, query: str) -> None:
        self.connection.executed.append(query)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        self.close_calls += 1


class StaticDbapiConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[str] = []
        self.rollback_calls = 0

    def cursor(self) -> StaticDbapiCursor:
        return StaticDbapiCursor(self, self.rows)

    def rollback(self) -> None:
        self.rollback_calls += 1


class StaticClickHouseResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class StaticClickHouseClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def query(self, query: str) -> StaticClickHouseResult:
        self.queries.append(query)
        return StaticClickHouseResult(self.rows)


class RenderingFakeTqdm:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.total = kwargs["total"]
        self.n = 0
        self.rendered: list[str] = []

    @property
    def format_dict(self) -> dict[str, Any]:
        desc = self.kwargs["desc"]
        return {
            "n": self.n,
            "total": self.total,
            "desc": desc,
            "unit": self.kwargs["unit"],
            "elapsed": "00:00",
            "remaining": "00:02",
            "rate_fmt": "14087.46row/s",
            "postfix": "",
            "l_bar": f"{desc}:  86%|",
            "bar": "########",
        }

    def update(self, value: int) -> None:
        self.n += value
        if not self.kwargs["disable"]:
            self.rendered.append(self.kwargs["bar_format"].format(**self.format_dict))


def capture_rendering_progress_bars(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    progress_bars: list[Any] = []

    class CapturingTqdm(RenderingFakeTqdm):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            progress_bars.append(self)

    monkeypatch.setattr(attempt_module, "tqdm", CapturingTqdm)
    return progress_bars


def make_progress_options(**overrides: Any) -> Any:
    values = {
        "from_db_key": "gp",
        "from_db_backend": "gp",
        "to_db_key": "gp_sandbox",
        "to_db_backend": "gp",
        "source_sql": "select id from source_table",
        "target_table": "sandbox.target",
        "batch_size": 2,
    }
    values.update(overrides)
    return models_module.TransferOptions(**values)


def test_adaptive_batch_sizer_grows_shrinks_caps_floors_and_can_disable() -> None:
    sizer = models_module.AdaptiveBatchSizer(
        enabled=True,
        current_size=1_000,
        min_size=500,
        max_size=2_000,
        target_seconds=10.0,
    )

    sizer.update(4.9)
    assert sizer.current_size == 1_500
    sizer.update(4.9)
    assert sizer.current_size == 2_000
    sizer.update(10.0)
    assert sizer.current_size == 2_000
    sizer.update(21.0)
    assert sizer.current_size == 1_000
    sizer.update(21.0)
    assert sizer.current_size == 500
    sizer.update(21.0)
    assert sizer.current_size == 500

    disabled = models_module.AdaptiveBatchSizer(
        enabled=False,
        current_size=1_000,
        min_size=500,
        max_size=2_000,
        target_seconds=10.0,
    )
    disabled.update(1.0)
    assert disabled.current_size == 1_000


def test_transfer_options_resolve_adaptive_bounds_and_validate() -> None:
    options = transfer_api_module.build_transfer_options(
        from_db="gp",
        to_db="trino",
        from_sql="select id from source_table",
        to_table="sandbox.target",
        batch_size=100,
    )

    assert options.min_batch_size == 100
    assert options.max_batch_size == 400

    with pytest.raises(ValueError, match="min_batch_size"):
        transfer_api_module.build_transfer_options(
            from_db="gp",
            to_db="trino",
            from_sql="select id from source_table",
            to_table="sandbox.target",
            batch_size=100,
            min_batch_size=101,
        )

    with pytest.raises(ValueError, match="max_batch_size"):
        transfer_api_module.build_transfer_options(
            from_db="gp",
            to_db="trino",
            from_sql="select id from source_table",
            to_table="sandbox.target",
            batch_size=100,
            max_batch_size=99,
        )


def test_load_stage_batches_fetches_row_batches_with_adaptive_sizes(monkeypatch) -> None:
    source = RecordingSourceConnection(rows=[(row_id,) for row_id in range(10)])
    connection_refs = models_module.TransferConnectionRefs(
        source={"connection": source},
        target={"connection": object()},
    )
    stage_state = models_module.TransferStageState(
        target_exists=False,
        stage_column_types={"id": "INTEGER"},
    )
    options = models_module.TransferOptions(
        from_db_key="gp",
        from_db_backend="gp",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        batch_size=2,
        adaptive_batch_size=True,
        min_batch_size=1,
        max_batch_size=4,
        target_batch_seconds=10.0,
    )
    inserted_batch_sizes: list[int] = []
    insert_durations = iter([1.0, 1.0, 30.0, 30.0])

    def fake_initialize_stage_for_first_batch(
        options: object,
        connection_refs: object,
        stage_state: object,
        batch: object,
    ) -> None:
        del options, connection_refs
        stage_state.first_non_empty_batch = batch.to_dataframe()
        stage_state.stage_table = "sandbox.target__stage__abcd1234"

    def fake_insert_rows_batch(
        connection_type: str,
        connection_ref: dict[str, Any],
        table_name: str,
        columns: list[str],
        rows: list[tuple[int]],
        **kwargs: Any,
    ) -> int:
        del connection_type, connection_ref, table_name
        assert columns == ["id"]
        assert not isinstance(rows, pd.DataFrame)
        inserted_batch_sizes.append(len(rows))
        kwargs["on_success"](next(insert_durations))
        return len(rows)

    monkeypatch.setattr(
        attempt_module,
        "initialize_stage_for_first_batch",
        fake_initialize_stage_for_first_batch,
    )
    monkeypatch.setattr(attempt_module, "insert_rows_batch", fake_insert_rows_batch)

    total_rows = attempt_module.load_stage_batches(
        options=options,
        connection_refs=connection_refs,
        stage_state=stage_state,
        read_retry_cnt=1,
        insert_retry_cnt=1,
    )

    assert total_rows == 10
    assert inserted_batch_sizes == [2, 3, 4, 1]
    assert source.cursor_obj.fetch_sizes == [2, 3, 4, 2, 1]


def test_load_stage_batches_updates_progress_bar(monkeypatch) -> None:
    source = RecordingSourceConnection(rows=[(row_id,) for row_id in range(3)])
    connection_refs = models_module.TransferConnectionRefs(
        source={"connection": source},
        target={"connection": object()},
    )
    stage_state = models_module.TransferStageState(
        target_exists=False,
        stage_column_types={"id": "INTEGER"},
    )
    options = models_module.TransferOptions(
        from_db_key="gp",
        from_db_backend="gp",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        batch_size=2,
        adaptive_batch_size=False,
        min_batch_size=1,
        max_batch_size=4,
        target_batch_seconds=10.0,
    )
    progress_bars: list[Any] = []

    class FakeTqdm:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.updates: list[int] = []
            self.closed = False
            progress_bars.append(self)

        def update(self, value: int) -> None:
            self.updates.append(value)

        def close(self) -> None:
            self.closed = True

    def fake_initialize_stage_for_first_batch(
        options: object,
        connection_refs: object,
        stage_state: object,
        batch: object,
    ) -> None:
        del options, connection_refs
        stage_state.first_non_empty_batch = batch.to_dataframe()
        stage_state.stage_table = "sandbox.target__stage__abcd1234"

    def fake_insert_rows_batch(
        connection_type: str,
        connection_ref: dict[str, Any],
        table_name: str,
        columns: list[str],
        rows: list[tuple[int]],
        **kwargs: Any,
    ) -> int:
        del connection_type, connection_ref, table_name, columns
        kwargs["on_progress"](len(rows))
        kwargs["on_success"](1.0)
        return len(rows)

    monkeypatch.setattr(attempt_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(
        attempt_module,
        "initialize_stage_for_first_batch",
        fake_initialize_stage_for_first_batch,
    )
    monkeypatch.setattr(attempt_module, "insert_rows_batch", fake_insert_rows_batch)

    total_rows = attempt_module.load_stage_batches(
        options=options,
        connection_refs=connection_refs,
        stage_state=stage_state,
        read_retry_cnt=1,
        insert_retry_cnt=1,
    )

    assert total_rows == 3
    assert len(progress_bars) == 1
    assert progress_bars[0].kwargs == {
        "total": None,
        "desc": "transfer_table gp_sandbox.sandbox.target",
        "unit": "row",
        "disable": False,
        "bar_format": attempt_module._TRANSFER_PROGRESS_UNKNOWN_TOTAL_FORMAT,
    }
    assert progress_bars[0].updates == [2, 1]
    assert progress_bars[0].closed is True


def test_load_stage_batches_estimated_total_sets_progress_bar_total(
    monkeypatch,
) -> None:
    source = RecordingSourceConnection(rows=[(row_id,) for row_id in range(3)])
    connection_refs = models_module.TransferConnectionRefs(
        source={"connection": source},
        target={"connection": object()},
    )
    stage_state = models_module.TransferStageState(
        target_exists=False,
        stage_column_types={"id": "INTEGER"},
    )
    options = models_module.TransferOptions(
        from_db_key="gp",
        from_db_backend="gp",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        batch_size=2,
        adaptive_batch_size=False,
        min_batch_size=1,
        max_batch_size=4,
        target_batch_seconds=10.0,
        estimate_total_rows=True,
    )
    progress_bars: list[Any] = []

    class FakeTqdm:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.updates: list[int] = []
            self.closed = False
            progress_bars.append(self)

        def update(self, value: int) -> None:
            self.updates.append(value)

        def close(self) -> None:
            self.closed = True

    def fake_initialize_stage_for_first_batch(
        options: object,
        connection_refs: object,
        stage_state: object,
        batch: object,
    ) -> None:
        del options, connection_refs
        stage_state.first_non_empty_batch = batch.to_dataframe()
        stage_state.stage_table = "sandbox.target__stage__abcd1234"

    monkeypatch.setattr(attempt_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(attempt_module, "estimate_source_rows", lambda *_args: 3)
    monkeypatch.setattr(
        attempt_module,
        "initialize_stage_for_first_batch",
        fake_initialize_stage_for_first_batch,
    )
    monkeypatch.setattr(
        attempt_module,
        "insert_rows_batch",
        lambda *args, **kwargs: len(args[4]),
    )

    total_rows = attempt_module.load_stage_batches(
        options=options,
        connection_refs=connection_refs,
        stage_state=stage_state,
        read_retry_cnt=1,
        insert_retry_cnt=1,
    )

    assert total_rows == 3
    assert progress_bars[0].kwargs["total"] == 3
    assert (
        progress_bars[0].kwargs["bar_format"]
        == attempt_module._TRANSFER_PROGRESS_TOTAL_FORMAT
    )
    assert progress_bars[0].updates == [2, 1]
    assert progress_bars[0].closed is True


def test_load_stage_batches_estimator_failure_keeps_unknown_total(
    monkeypatch,
) -> None:
    source = RecordingSourceConnection(rows=[(1,), (2,)])
    connection_refs = models_module.TransferConnectionRefs(
        source={"connection": source},
        target={"connection": object()},
    )
    stage_state = models_module.TransferStageState(
        target_exists=False,
        stage_column_types={"id": "INTEGER"},
    )
    options = models_module.TransferOptions(
        from_db_key="gp",
        from_db_backend="gp",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        batch_size=2,
        adaptive_batch_size=False,
        min_batch_size=1,
        max_batch_size=4,
        target_batch_seconds=10.0,
        estimate_total_rows=True,
    )
    progress_bars: list[Any] = []

    class FakeTqdm:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.updates: list[int] = []
            self.closed = False
            progress_bars.append(self)

        def update(self, value: int) -> None:
            self.updates.append(value)

        def close(self) -> None:
            self.closed = True

    def fake_initialize_stage_for_first_batch(
        options: object,
        connection_refs: object,
        stage_state: object,
        batch: object,
    ) -> None:
        del options, connection_refs
        stage_state.first_non_empty_batch = batch.to_dataframe()
        stage_state.stage_table = "sandbox.target__stage__abcd1234"

    monkeypatch.setattr(attempt_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(
        attempt_module,
        "initialize_stage_for_first_batch",
        fake_initialize_stage_for_first_batch,
    )
    monkeypatch.setattr(
        attempt_module,
        "insert_rows_batch",
        lambda *args, **kwargs: len(args[4]),
    )

    total_rows = attempt_module.load_stage_batches(
        options=options,
        connection_refs=connection_refs,
        stage_state=stage_state,
        read_retry_cnt=1,
        insert_retry_cnt=1,
    )

    assert total_rows == 2
    assert progress_bars[0].kwargs["total"] is None
    assert progress_bars[0].updates == [2]
    assert source.cursor_obj.executed[0].startswith("EXPLAIN (FORMAT JSON)")
    assert source.cursor_obj.executed[-1] == "select id from source_table"


def test_load_stage_batches_progress_false_disables_bar(monkeypatch) -> None:
    source = RecordingSourceConnection(rows=[(1,), (2,)])
    connection_refs = models_module.TransferConnectionRefs(
        source={"connection": source},
        target={"connection": object()},
    )
    stage_state = models_module.TransferStageState(
        target_exists=False,
        stage_column_types={"id": "INTEGER"},
    )
    options = models_module.TransferOptions(
        from_db_key="gp",
        from_db_backend="gp",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        batch_size=2,
        adaptive_batch_size=False,
        min_batch_size=1,
        max_batch_size=4,
        target_batch_seconds=10.0,
        progress=False,
        estimate_total_rows=True,
    )
    progress_bars: list[Any] = []

    class FakeTqdm:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.updates: list[int] = []
            self.closed = False
            progress_bars.append(self)

        def update(self, value: int) -> None:
            self.updates.append(value)

        def close(self) -> None:
            self.closed = True

    def fake_initialize_stage_for_first_batch(
        options: object,
        connection_refs: object,
        stage_state: object,
        batch: object,
    ) -> None:
        del options, connection_refs
        stage_state.first_non_empty_batch = batch.to_dataframe()
        stage_state.stage_table = "sandbox.target__stage__abcd1234"

    def unexpected_estimate(*_args: object) -> int:
        raise AssertionError("unexpected estimate")

    monkeypatch.setattr(attempt_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(attempt_module, "estimate_source_rows", unexpected_estimate)
    monkeypatch.setattr(
        attempt_module,
        "initialize_stage_for_first_batch",
        fake_initialize_stage_for_first_batch,
    )
    monkeypatch.setattr(
        attempt_module,
        "insert_rows_batch",
        lambda *args, **kwargs: len(args[4]),
    )

    total_rows = attempt_module.load_stage_batches(
        options=options,
        connection_refs=connection_refs,
        stage_state=stage_state,
        read_retry_cnt=1,
        insert_retry_cnt=1,
    )

    assert total_rows == 2
    assert len(progress_bars) == 1
    assert progress_bars[0].kwargs["disable"] is True
    assert progress_bars[0].updates == [2]
    assert progress_bars[0].closed is True


def test_transfer_progress_bar_formats_unknown_total_counts(monkeypatch) -> None:
    progress_bars = capture_rendering_progress_bars(monkeypatch)

    options = make_progress_options()
    progress_bar = attempt_module._make_transfer_progress_bar(options, total=None)
    progress_bar.update(1_722_355)

    assert progress_bars[0].rendered == [
        "transfer_table gp_sandbox.sandbox.target: "
        "1_722_355row [00:00, 14087.46row/s]"
    ]


def test_transfer_progress_bar_formats_estimated_total_counts(monkeypatch) -> None:
    progress_bars = capture_rendering_progress_bars(monkeypatch)

    options = make_progress_options()
    progress_bar = attempt_module._make_transfer_progress_bar(
        options,
        total=2_000_000,
    )
    progress_bar.update(1_722_355)

    assert progress_bars[0].rendered == [
        "transfer_table gp_sandbox.sandbox.target:  86%|########| "
        "1_722_355/2_000_000 [00:00<00:02, 14087.46row/s]"
    ]


def test_transfer_progress_bar_progress_false_disables_output(monkeypatch) -> None:
    progress_bars = capture_rendering_progress_bars(monkeypatch)

    options = make_progress_options(progress=False)
    progress_bar = attempt_module._make_transfer_progress_bar(options, total=None)
    progress_bar.update(1_722_355)

    assert progress_bars[0].kwargs["disable"] is True
    assert progress_bars[0].rendered == []


@pytest.mark.parametrize("progress", [None, 0, 1, "yes"])
def test_transfer_table_validates_progress(progress: Any) -> None:
    with pytest.raises(ValueError, match="progress"):
        transfer_api_module.transfer_table(
            from_db="gp",
            to_db="trino",
            from_sql="select id from source_table",
            to_table="sandbox.target",
            dry_run=True,
            progress=progress,
        )


@pytest.mark.parametrize("estimate_total_rows", [None, 0, 1, "yes"])
def test_transfer_table_validates_estimate_total_rows(
    estimate_total_rows: Any,
) -> None:
    with pytest.raises(ValueError, match="estimate_total_rows"):
        transfer_api_module.transfer_table(
            from_db="gp",
            to_db="trino",
            from_sql="select id from source_table",
            to_table="sandbox.target",
            dry_run=True,
            estimate_total_rows=estimate_total_rows,
        )


def test_transfer_dry_run_includes_estimate_total_rows_option() -> None:
    plan = transfer_api_module.transfer_table(
        from_db="gp",
        to_db="trino",
        from_sql="select id from source_table",
        to_table="sandbox.target",
        dry_run=True,
        estimate_total_rows=True,
    )

    assert plan.options["estimate_total_rows"] is True


@pytest.mark.parametrize(
    ("backend", "connection", "expected_total", "expected_sql_prefix"),
    [
        (
            "gp",
            StaticDbapiConnection([('[{"Plan": {"Plan Rows": 123}}]',)]),
            123,
            "EXPLAIN (FORMAT JSON)",
        ),
        (
            "trino",
            StaticDbapiConnection([('{"outputRowCount": 456}',)]),
            456,
            "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)",
        ),
        (
            "ch",
            StaticClickHouseClient([("default", "source_table", 1, 789, 1)]),
            789,
            "EXPLAIN ESTIMATE",
        ),
    ],
)
def test_estimate_source_rows_uses_backend_planner_estimates(
    backend: str,
    connection: Any,
    expected_total: int,
    expected_sql_prefix: str,
) -> None:
    options = models_module.TransferOptions(
        from_db_key=backend,
        from_db_backend=backend,
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table",
        target_table="sandbox.target",
        estimate_total_rows=True,
    )

    estimated_total = estimate_module.estimate_source_rows(options, connection)

    assert estimated_total == expected_total
    executed = getattr(connection, "executed", getattr(connection, "queries", []))
    assert executed[0].startswith(expected_sql_prefix)


def test_clickhouse_estimator_skips_non_simple_select() -> None:
    connection = StaticClickHouseClient([("default", "source_table", 1, 789, 1)])
    options = models_module.TransferOptions(
        from_db_key="ch",
        from_db_backend="ch",
        to_db_key="gp_sandbox",
        to_db_backend="gp",
        source_sql="select id from source_table where id > 10",
        target_table="sandbox.target",
        estimate_total_rows=True,
    )

    assert estimate_module.estimate_source_rows(options, connection) is None
    assert connection.queries == []
