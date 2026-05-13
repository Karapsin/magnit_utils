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


def test_async_sql_is_exported() -> None:
    assert sql_module.async_sql is async_module.async_sql


def test_async_sql_dispatches_supported_task_types_and_preserves_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    read_result = pd.DataFrame({"value": [1]})
    execute_result = object()
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
    monkeypatch.setattr(async_module, "execute_sql", record("execute", execute_result))
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

    tasks = {
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
        },
    }

    result = asyncio.run(async_module.async_sql(tasks, concurrency=3))

    assert list(result) == list(tasks)
    pd.testing.assert_frame_equal(result["read_users"], read_result)
    assert result["refresh_table"] is execute_result
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
    }


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

    tasks = {
        f"read_{index}": {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(6)
    }

    result = asyncio.run(async_module.async_sql(tasks, concurrency=2))

    assert list(result) == list(tasks)
    assert max_active_tasks == 2


def test_async_sql_pipeline_runs_sync_steps_sequentially_and_returns_last_result() -> None:
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

    def second_step(context: Any) -> str:
        observations.append(
            (
                context.task_name,
                context.step_index,
                list(context.results),
                context.last_result,
            )
        )
        return f"{context.last_result}:second"

    result = asyncio.run(
        async_module.async_sql(
            {
                "pipeline": {
                    "type": "custom_sql_pipeline",
                    "steps": [first_step, second_step],
                }
            }
        )
    )

    assert result["pipeline"] == "first:second"
    assert observations == [
        ("pipeline", 0, [], None),
        ("pipeline", 1, ["first"], "first"),
    ]


def test_async_sql_pipeline_awaits_async_steps_and_mixes_sync_steps() -> None:
    async def async_step(context: Any) -> str:
        await asyncio.sleep(0)
        return f"{context.task_name}:async"

    def sync_step(context: Any) -> str:
        return f"{context.last_result}:sync"

    result = asyncio.run(
        async_module.async_sql(
            {
                "pipeline": {
                    "type": "custom_sql_pipeline",
                    "steps": [async_step, sync_step],
                }
            }
        )
    )

    assert result["pipeline"] == "pipeline:async:sync"


def test_async_sql_pipeline_supports_nested_async_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read_sql(**kwargs: Any) -> str:
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    async def nested_batch(context: Any) -> dict[str, Any]:
        return await async_module.async_sql(
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
            },
            concurrency=2,
        )

    result = asyncio.run(
        async_module.async_sql(
            {
                "pipeline": {
                    "type": "custom_sql_pipeline",
                    "steps": [nested_batch],
                }
            }
        )
    )

    assert result["pipeline"] == {"a": "pipeline:a", "b": "pipeline:b"}


def test_async_sql_multiple_pipelines_respect_outer_concurrency() -> None:
    active_pipelines = 0
    max_active_pipelines = 0
    lock = asyncio.Lock()

    async def step(context: Any) -> str:
        nonlocal active_pipelines, max_active_pipelines
        async with lock:
            active_pipelines += 1
            max_active_pipelines = max(max_active_pipelines, active_pipelines)
        await asyncio.sleep(0.05)
        async with lock:
            active_pipelines -= 1
        return context.task_name

    tasks = {
        f"pipeline_{index}": {
            "type": "custom_sql_pipeline",
            "steps": [step],
        }
        for index in range(5)
    }

    result = asyncio.run(async_module.async_sql(tasks, concurrency=2))

    assert list(result) == list(tasks)
    assert max_active_pipelines == 2


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

    tasks = {
        f"read_{index}": {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(6)
    }

    result = asyncio.run(
        async_module.async_sql(tasks, concurrency=6, soft_concurrency_cap=2)
    )

    assert list(result) == list(tasks)
    assert max_active_workers == 2


def test_async_sql_default_soft_cap_equals_concurrency(
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

    tasks = {
        f"read_{index}": {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(6)
    }

    result = asyncio.run(async_module.async_sql(tasks, concurrency=6))

    assert list(result) == list(tasks)
    assert max_active_workers == 6


def test_async_sql_hard_cap_rejects_unthrottled_effective_concurrency() -> None:
    tasks = {
        f"read_{index}": {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(11)
    }

    with pytest.raises(
        ValueError,
        match=(
            "effective concurrency exceeds hard_concurrency_cap.*"
            "soft_concurrency_cap"
        ),
    ):
        asyncio.run(async_module.async_sql(tasks, concurrency=11))


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

    tasks = {
        f"read_{index}": {
            "type": "read",
            "connection_type": "gp",
            "query": f"select {index}",
        }
        for index in range(11)
    }

    result = asyncio.run(
        async_module.async_sql(
            tasks,
            concurrency=11,
            soft_concurrency_cap=5,
            hard_concurrency_cap=10,
        )
    )

    assert list(result) == list(tasks)
    assert max_active_workers == 5


def test_async_sql_nested_batches_inherit_soft_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = threading.Lock()
    started_cap_workers = threading.Event()
    release_workers = threading.Event()
    active_workers = 0
    max_active_workers = 0

    def fake_read_sql(**kwargs: Any) -> str:
        nonlocal active_workers, max_active_workers
        with lock:
            active_workers += 1
            max_active_workers = max(max_active_workers, active_workers)
            if active_workers == 2:
                started_cap_workers.set()
        try:
            release_workers.wait(timeout=2)
            return kwargs["query"]
        finally:
            with lock:
                active_workers -= 1

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    async def nested_batch(context: Any) -> dict[str, Any]:
        nested_tasks = {
            f"{context.task_name}_read_{index}": {
                "type": "read",
                "connection_type": "gp",
                "query": f"{context.task_name}:{index}",
            }
            for index in range(6)
        }
        return await async_module.async_sql(nested_tasks, concurrency=6)

    async def run_with_release() -> dict[str, Any]:
        run_task = asyncio.create_task(
            async_module.async_sql(
                {
                    "pipeline_a": {
                        "type": "custom_sql_pipeline",
                        "steps": [nested_batch],
                    },
                    "pipeline_b": {
                        "type": "custom_sql_pipeline",
                        "steps": [nested_batch],
                    },
                },
                concurrency=2,
            )
        )
        try:
            cap_reached = await asyncio.to_thread(started_cap_workers.wait, 2)
            assert cap_reached
            with lock:
                assert active_workers == 2
            release_workers.set()
            return await run_task
        finally:
            release_workers.set()

    result = asyncio.run(run_with_release())

    assert list(result) == ["pipeline_a", "pipeline_b"]
    assert max_active_workers == 2


def test_async_sql_nested_lower_soft_cap_tightens_subtree(
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

    async def nested_batch(context: Any) -> dict[str, Any]:
        nested_tasks = {
            f"read_{index}": {
                "type": "read",
                "connection_type": "gp",
                "query": f"select {index}",
            }
            for index in range(6)
        }
        return await async_module.async_sql(
            nested_tasks,
            concurrency=6,
            soft_concurrency_cap=2,
        )

    result = asyncio.run(
        async_module.async_sql(
            {
                "pipeline": {
                    "type": "custom_sql_pipeline",
                    "steps": [nested_batch],
                }
            },
            concurrency=5,
        )
    )

    assert list(result["pipeline"]) == [f"read_{index}" for index in range(6)]
    assert max_active_workers == 2


def test_async_sql_pipeline_stops_on_first_step_exception() -> None:
    error = RuntimeError("pipeline failed")
    calls: list[str] = []

    def broken_step(context: Any) -> None:
        calls.append("broken")
        raise error

    def skipped_step(context: Any) -> None:
        calls.append("skipped")

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(
            async_module.async_sql(
                {
                    "pipeline": {
                        "type": "custom_sql_pipeline",
                        "steps": [broken_step, skipped_step],
                    }
                }
            )
        )

    assert exc_info.value is error
    assert calls == ["broken"]


def test_async_sql_fail_fast_false_returns_pipeline_exception() -> None:
    error = RuntimeError("pipeline failed")

    def broken_step(context: Any) -> None:
        raise error

    result = asyncio.run(
        async_module.async_sql(
            {
                "pipeline": {
                    "type": "custom_sql_pipeline",
                    "steps": [broken_step],
                }
            },
            fail_fast=False,
        )
    )

    assert result["pipeline"] is error


def test_async_sql_fail_fast_raises_first_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("read failed")

    def fake_read_sql(**kwargs: Any) -> str:
        raise error

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    tasks = {
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

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(async_module.async_sql(tasks, concurrency=1, fail_fast=True))

    assert exc_info.value is error


def test_async_sql_fail_fast_false_returns_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("read failed")

    def fake_read_sql(**kwargs: Any) -> str:
        if kwargs["query"] == "select broken":
            raise error
        return kwargs["query"]

    monkeypatch.setattr(async_module, "read_sql", fake_read_sql)

    result = asyncio.run(
        async_module.async_sql(
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
            },
            fail_fast=False,
        )
    )

    assert result["ok"] == "select ok"
    assert result["broken"] is error


@pytest.mark.parametrize(
    ("tasks", "expected_exception"),
    [
        ([], TypeError),
        ({}, ValueError),
        ({"": {"type": "read"}}, ValueError),
        ({"task": "read"}, TypeError),
        ({"task": {"connection_type": "gp"}}, ValueError),
        ({"task": {"type": "unknown"}}, ValueError),
        ({"task": {"type": ["read"]}}, ValueError),
    ],
)
def test_async_sql_validates_task_input(
    tasks: Any,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        asyncio.run(async_module.async_sql(tasks))


@pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
def test_async_sql_validates_concurrency(concurrency: Any) -> None:
    tasks = {
        "read": {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    }

    with pytest.raises(ValueError, match="concurrency"):
        asyncio.run(async_module.async_sql(tasks, concurrency=concurrency))


@pytest.mark.parametrize("soft_concurrency_cap", [0, -1, True, 1.5])
def test_async_sql_validates_soft_concurrency_cap(
    soft_concurrency_cap: Any,
) -> None:
    tasks = {
        "read": {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    }

    with pytest.raises(ValueError, match="soft_concurrency_cap"):
        asyncio.run(
            async_module.async_sql(
                tasks,
                soft_concurrency_cap=soft_concurrency_cap,
            )
        )


@pytest.mark.parametrize("hard_concurrency_cap", [0, -1, True, 1.5])
def test_async_sql_validates_hard_concurrency_cap(
    hard_concurrency_cap: Any,
) -> None:
    tasks = {
        "read": {
            "type": "read",
            "connection_type": "gp",
            "query": "select 1",
        }
    }

    with pytest.raises(ValueError, match="hard_concurrency_cap"):
        asyncio.run(
            async_module.async_sql(
                tasks,
                hard_concurrency_cap=hard_concurrency_cap,
            )
        )


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
        asyncio.run(async_module.async_sql({"pipeline": spec}))


def test_async_sql_validates_pipeline_extra_fields() -> None:
    with pytest.raises(ValueError, match="unsupported custom_sql_pipeline field"):
        asyncio.run(
            async_module.async_sql(
                {
                    "pipeline": {
                        "type": "custom_sql_pipeline",
                        "steps": [lambda context: None],
                        "connection_type": "gp",
                    }
                }
            )
        )
