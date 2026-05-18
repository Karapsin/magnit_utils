from __future__ import annotations

import asyncio
import importlib
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

async_module = importlib.import_module("analytics_toolkit.sql.async_api")
sql_module = importlib.import_module("analytics_toolkit.sql")


def named_tasks(tasks: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"name": name, **spec} for name, spec in tasks.items()]


def test_async_sql_is_exported() -> None:
    assert sql_module.async_sql is async_module.async_sql


def test_async_sql_dispatches_supported_task_types_and_preserves_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    read_result = pd.DataFrame({"value": [1]})
    execute_read_result = pd.DataFrame({"value": [2]})
    load_result = 3
    transfer_result = 4
    df = pd.DataFrame({"id": [1]})

    def record(task_type: str, result: Any):
        def fake_operation(**kwargs: Any) -> Any:
            calls.append((task_type, kwargs))
            return result

        return fake_operation

    monkeypatch.setattr(async_module, "read_sql", record("read", read_result))
    monkeypatch.setattr(async_module, "execute_sql", record("execute", None))
    monkeypatch.setattr(
        async_module,
        "execute_read",
        record("execute_read", execute_read_result),
    )
    monkeypatch.setattr(async_module, "load_df", record("load_df", load_result))
    monkeypatch.setattr(
        async_module,
        "transfer_table",
        record("transfer", transfer_result),
    )

    tasks = named_tasks(
        {
            "read_users": {
                "type": "read",
                "connection_type": "gp",
                "query": "select * from users",
                "print_queries": False,
            },
            "refresh_table": {
                "type": "execute",
                "connection_type": "gp",
                "query": "truncate table sandbox.target",
                "gp_break_query": True,
                "gp_commit_each_statement": True,
            },
            "prepare_and_read": {
                "type": "execute_read",
                "connection_type": "trino",
                "query": "create table tmp as select 1; select * from tmp",
                "random_sleep_seconds": None,
            },
            "load_batch": {
                "type": "load_df",
                "connection_type": "ch",
                "destination_table": "sandbox.batch",
                "df": df,
                "append": True,
                "ch_order_by": ["id"],
            },
            "copy_table": {
                "type": "transfer",
                "from_db": "gp",
                "to_db": "trino",
                "from_sql": "select * from source",
                "to_table": "sandbox.copy",
                "batch_size": 10,
                "estimate_total_rows": True,
            },
        }
    )

    result = async_module.async_sql(tasks, concurrency=3)

    assert list(result) == [
        "read_users",
        "refresh_table",
        "prepare_and_read",
        "load_batch",
        "copy_table",
    ]
    pd.testing.assert_frame_equal(result["read_users"], read_result)
    assert result["refresh_table"] == "success"
    pd.testing.assert_frame_equal(result["prepare_and_read"], execute_read_result)
    assert result["load_batch"] == load_result
    assert result["copy_table"] == transfer_result

    calls_by_type = {task_type: kwargs for task_type, kwargs in calls}
    assert calls_by_type["read"] == {
        "connection_type": "gp",
        "query": "select * from users",
        "print_queries": False,
    }
    assert calls_by_type["execute"] == {
        "connection_type": "gp",
        "query": "truncate table sandbox.target",
        "gp_break_query": True,
        "gp_commit_each_statement": True,
    }
    assert calls_by_type["execute_read"] == {
        "connection_type": "trino",
        "query": "create table tmp as select 1; select * from tmp",
        "random_sleep_seconds": None,
    }
    load_kwargs = calls_by_type["load_df"]
    assert load_kwargs["df"] is df
    assert {key: value for key, value in load_kwargs.items() if key != "df"} == {
        "connection_type": "ch",
        "destination_table": "sandbox.batch",
        "append": True,
        "ch_order_by": ["id"],
    }
    assert calls_by_type["transfer"] == {
        "from_db": "gp",
        "to_db": "trino",
        "from_sql": "select * from source",
        "to_table": "sandbox.copy",
        "batch_size": 10,
        "estimate_total_rows": True,
    }


def test_async_sql_uses_generated_names_for_unnamed_task_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_execute_sql(**kwargs: Any) -> str:
        calls.append(kwargs)
        return kwargs["query"]

    monkeypatch.setattr(async_module, "execute_sql", fake_execute_sql)

    result = async_module.async_sql(
        [
            {
                "type": "execute",
                "connection_type": "gp",
                "query": "insert into target select 1",
            },
            {
                "type": "execute",
                "connection_type": "gp",
                "query": "insert into target select 2",
            },
        ],
        concurrency=1,
    )

    assert result == {
        "task_0": "insert into target select 1",
        "task_1": "insert into target select 2",
    }
    assert calls == [
        {
            "connection_type": "gp",
            "query": "insert into target select 1",
        },
        {
            "connection_type": "gp",
            "query": "insert into target select 2",
        },
    ]


def test_async_sql_runs_from_inside_existing_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_execute_sql(**kwargs: Any) -> None:
        return None

    monkeypatch.setattr(async_module, "execute_sql", fake_execute_sql)

    async def call_sync_api() -> dict[str, Any]:
        return async_module.async_sql(
            [
                {
                    "type": "execute",
                    "connection_type": "gp",
                    "query": "insert into target select 1",
                }
            ],
            concurrency=1,
        )

    assert asyncio.run(call_sync_api()) == {"task_0": "success"}


def test_async_sql_updates_progress_bar(monkeypatch: pytest.MonkeyPatch) -> None:
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

    def fake_execute_sql(**kwargs: Any) -> None:
        return None

    monkeypatch.setattr(async_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(async_module, "execute_sql", fake_execute_sql)

    result = async_module.async_sql(
        [
            {
                "type": "execute",
                "connection_type": "gp",
                "query": "insert into target select 1",
            },
            {
                "type": "execute",
                "connection_type": "gp",
                "query": "insert into target select 2",
            },
        ],
        concurrency=1,
    )

    assert result == {
        "task_0": "success",
        "task_1": "success",
    }
    assert len(progress_bars) == 1
    progress_bar = progress_bars[0]
    assert progress_bar.kwargs == {
        "total": 2,
        "desc": "async_sql tasks",
        "unit": "task",
        "disable": False,
    }
    assert progress_bar.updates == [1, 1]
    assert progress_bar.closed


def test_async_sql_concurrency_limits_active_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = threading.Lock()
    active_tasks = 0
    max_active_tasks = 0

    def fake_read_sql(**kwargs: Any) -> str:
        nonlocal active_tasks, max_active_tasks
        with lock:
            active_tasks += 1
            max_active_tasks = max(max_active_tasks, active_tasks)
        time.sleep(0.1)
        with lock:
            active_tasks -= 1
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    tasks = named_tasks(
        {
            f"read_{index}": {
                "type": "read",
                "connection_type": "gp",
                "query": f"select {index}",
            }
            for index in range(6)
        }
    )

    result = async_module.async_sql(tasks, concurrency=2)

    assert list(result) == [f"read_{index}" for index in range(6)]
    assert max_active_tasks == 2


def test_async_sql_pipeline_runs_steps_sequentially_and_returns_last_result() -> None:
    observations: list[tuple[str, int, list[Any], Any]] = []

    def first_step(context: Any) -> str:
        observations.append(
            (
                context.task_name,
                context.step_index,
                list(context.results),
                context.last_result,
            )
        )
        return "first"

    async def second_step(context: Any) -> str:
        await asyncio.sleep(0)
        observations.append(
            (
                context.task_name,
                context.step_index,
                list(context.results),
                context.last_result,
            )
        )
        return f"{context.last_result}:second"

    result = async_module.async_sql(
        [
            {
                "name": "pipeline",
                "type": "custom_sql_pipeline",
                "steps": [first_step, second_step],
            }
        ]
    )

    assert result["pipeline"] == "first:second"
    assert observations == [
        ("pipeline", 0, [], None),
        ("pipeline", 1, ["first"], "first"),
    ]


def test_async_sql_pipeline_can_run_nested_sync_async_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read_sql(**kwargs: Any) -> str:
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    def nested_batch(context: Any) -> dict[str, Any]:
        return async_module.async_sql(
            named_tasks(
                {
                    "a": {
                        "type": "read",
                        "connection_type": "gp",
                        "query": f"{context.task_name}:a",
                    },
                    "b": {
                        "type": "read",
                        "connection_type": "gp",
                        "query": f"{context.task_name}:b",
                    },
                }
            ),
            concurrency=2,
        )

    result = async_module.async_sql(
        [
            {
                "name": "pipeline",
                "type": "custom_sql_pipeline",
                "steps": [nested_batch],
            }
        ]
    )

    assert result["pipeline"] == {"a": "pipeline:a", "b": "pipeline:b"}


def test_async_sql_soft_cap_limits_top_level_worker_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = threading.Lock()
    active_workers = 0
    max_active_workers = 0

    def fake_read_sql(**kwargs: Any) -> str:
        nonlocal active_workers, max_active_workers
        with lock:
            active_workers += 1
            max_active_workers = max(max_active_workers, active_workers)
        time.sleep(0.1)
        with lock:
            active_workers -= 1
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    tasks = named_tasks(
        {
            f"read_{index}": {
                "type": "read",
                "connection_type": "gp",
                "query": f"select {index}",
            }
            for index in range(6)
        }
    )

    result = async_module.async_sql(tasks, concurrency=6, soft_concurrency_cap=2)

    assert list(result) == [f"read_{index}" for index in range(6)]
    assert max_active_workers == 2


def test_async_sql_hard_cap_rejects_unthrottled_effective_concurrency() -> None:
    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(11)
    ]

    with pytest.raises(
        ValueError,
        match=(
            "effective concurrency exceeds hard_concurrency_cap.*"
            "soft_concurrency_cap"
        ),
    ):
        async_module.async_sql(tasks, concurrency=11)


def test_async_sql_lower_soft_cap_avoids_hard_cap_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = threading.Lock()
    active_workers = 0
    max_active_workers = 0

    def fake_read_sql(**kwargs: Any) -> str:
        nonlocal active_workers, max_active_workers
        with lock:
            active_workers += 1
            max_active_workers = max(max_active_workers, active_workers)
        time.sleep(0.1)
        with lock:
            active_workers -= 1
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(11)
    ]

    result = async_module.async_sql(
        tasks,
        concurrency=11,
        soft_concurrency_cap=5,
        hard_concurrency_cap=10,
    )

    assert list(result) == [f"task_{index}" for index in range(11)]
    assert max_active_workers == 5


def test_async_sql_pipeline_stops_on_first_step_exception() -> None:
    error = RuntimeError("pipeline failed")
    calls: list[str] = []

    def broken_step(context: Any) -> None:
        calls.append("broken")
        raise error

    def skipped_step(context: Any) -> None:
        calls.append("skipped")

    with pytest.raises(RuntimeError) as exc_info:
        async_module.async_sql(
            [
                {
                    "name": "pipeline",
                    "type": "custom_sql_pipeline",
                    "steps": [broken_step, skipped_step],
                }
            ]
        )

    assert exc_info.value is error
    assert calls == ["broken"]


def test_async_sql_fail_fast_false_returns_pipeline_exception() -> None:
    error = RuntimeError("pipeline failed")

    def broken_step(context: Any) -> None:
        raise error

    result = async_module.async_sql(
        [
            {
                "name": "pipeline",
                "type": "custom_sql_pipeline",
                "steps": [broken_step],
            }
        ],
        fail_fast=False,
    )

    assert result["pipeline"] == str(error)


def test_async_sql_fail_fast_raises_first_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("read failed")

    def fake_read_sql(**kwargs: Any) -> str:
        raise error

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    tasks = named_tasks(
        {
            "broken": {
                "type": "read",
                "connection_type": "gp",
                "query": "select broken",
            },
            "also_broken": {
                "type": "read",
                "connection_type": "gp",
                "query": "select also_broken",
            },
        }
    )

    with pytest.raises(RuntimeError) as exc_info:
        async_module.async_sql(tasks, concurrency=1, fail_fast=True)

    assert exc_info.value is error


def test_async_sql_fail_fast_false_returns_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("read failed")

    def fake_read_sql(**kwargs: Any) -> str:
        if kwargs["query"] == "select broken":
            raise error
        return kwargs["query"]

    def fake_execute_sql(**kwargs: Any) -> None:
        return None

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)
    monkeypatch.setattr(async_module, "execute_sql", fake_execute_sql)

    result = async_module.async_sql(
        named_tasks(
            {
                "ok": {
                    "type": "read",
                    "connection_type": "gp",
                    "query": "select ok",
                },
                "broken": {
                    "type": "read",
                    "connection_type": "gp",
                    "query": "select broken",
                },
                "write_ok": {
                    "type": "execute",
                    "connection_type": "gp",
                    "query": "truncate table sandbox.target",
                },
            }
        ),
        fail_fast=False,
    )

    assert result["ok"] == "select ok"
    assert result["broken"] == str(error)
    assert result["write_ok"] == "success"


@pytest.mark.parametrize(
    ("tasks", "expected_exception"),
    [
        ([], ValueError),
        ({}, TypeError),
        ([{"name": "", "type": "read"}], ValueError),
        ([{"type": "read"}, "read"], TypeError),
        ([{"connection_type": "gp"}], ValueError),
        ([{"type": "unknown"}], ValueError),
        ([{"type": ["read"]}], ValueError),
    ],
)
def test_async_sql_validates_task_input(
    tasks: Any,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        async_module.async_sql(tasks)


@pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
def test_async_sql_validates_concurrency(concurrency: Any) -> None:
    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    ]

    with pytest.raises(ValueError, match="concurrency"):
        async_module.async_sql(tasks, concurrency=concurrency)


@pytest.mark.parametrize("soft_concurrency_cap", [0, -1, True, 1.5])
def test_async_sql_validates_soft_concurrency_cap(
    soft_concurrency_cap: Any,
) -> None:
    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    ]

    with pytest.raises(ValueError, match="soft_concurrency_cap"):
        async_module.async_sql(
            tasks,
            soft_concurrency_cap=soft_concurrency_cap,
        )


@pytest.mark.parametrize("hard_concurrency_cap", [0, -1, True, 1.5])
def test_async_sql_validates_hard_concurrency_cap(
    hard_concurrency_cap: Any,
) -> None:
    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    ]

    with pytest.raises(ValueError, match="hard_concurrency_cap"):
        async_module.async_sql(
            tasks,
            hard_concurrency_cap=hard_concurrency_cap,
        )


@pytest.mark.parametrize("progress", [None, 0, 1, "yes"])
def test_async_sql_validates_progress(progress: Any) -> None:
    tasks = [
        {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    ]

    with pytest.raises(ValueError, match="progress"):
        async_module.async_sql(tasks, progress=progress)


@pytest.mark.parametrize(
    ("spec", "expected_exception"),
    [
        ({"type": "custom_sql_pipeline"}, ValueError),
        ({"type": "custom_sql_pipeline", "steps": []}, ValueError),
        ({"type": "custom_sql_pipeline", "steps": "not steps"}, TypeError),
        ({"type": "custom_sql_pipeline", "steps": b"not steps"}, TypeError),
        ({"type": "custom_sql_pipeline", "steps": object()}, TypeError),
        (
            {"type": "custom_sql_pipeline", "steps": [lambda context: None, 1]},
            TypeError,
        ),
    ],
)
def test_async_sql_validates_pipeline_steps(
    spec: dict[str, Any],
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception, match="steps|step"):
        async_module.async_sql([{"name": "pipeline", **spec}])


def test_async_sql_validates_pipeline_extra_fields() -> None:
    with pytest.raises(ValueError, match="unsupported custom_sql_pipeline field"):
        async_module.async_sql(
            [
                {
                    "name": "pipeline",
                    "type": "custom_sql_pipeline",
                    "steps": [lambda context: None],
                    "connection_type": "gp",
                }
            ]
        )
