from __future__ import annotations

import importlib
import threading
import time
from typing import Any

import pandas as pd
import pytest

ab_utils_module = importlib.import_module("analytics_toolkit.ab_utils")
metrics_module = importlib.import_module("analytics_toolkit.ab_utils.metrics")
parallel_module = importlib.import_module("analytics_toolkit.ab_utils.parallel")


def test_parallel_compute_metrics_is_exported() -> None:
    assert ab_utils_module.parallel_compute_metrics is parallel_module.parallel_compute_metrics
    assert metrics_module.parallel_compute_metrics is parallel_module.parallel_compute_metrics


def test_parallel_compute_metrics_from_sql_is_exported() -> None:
    assert (
        ab_utils_module.parallel_compute_metrics_from_sql
        is parallel_module.parallel_compute_metrics_from_sql
    )
    assert (
        metrics_module.parallel_compute_metrics_from_sql
        is parallel_module.parallel_compute_metrics_from_sql
    )


def test_parallel_compute_metrics_runs_tasks_and_preserves_input_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        time.sleep(kwargs["delay"])
        return pd.DataFrame({"metric_name": [kwargs["metric"]]})

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    result = parallel_module.parallel_compute_metrics(
        {
            "slow": {"df": pd.DataFrame(), "metric": "first", "delay": 0.05},
            "fast": {"df": pd.DataFrame(), "metric": "second", "delay": 0.0},
        },
        concurrency=2,
        progress=False,
    )

    assert list(result) == ["slow", "fast"]
    pd.testing.assert_frame_equal(
        result["slow"],
        pd.DataFrame({"metric_name": ["first"]}),
    )
    pd.testing.assert_frame_equal(
        result["fast"],
        pd.DataFrame({"metric_name": ["second"]}),
    )


def test_parallel_compute_metrics_maps_pre_exp_df_and_honors_task_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    pre_df = pd.DataFrame({"user_id": [1]})

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        calls.append(kwargs)
        return pd.DataFrame({"metric_name": [kwargs["metric_name"]]})

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    parallel_module.parallel_compute_metrics(
        {
            "with_pre": {
                "df": pd.DataFrame({"user_id": [1]}),
                "pre_exp_df": pre_df,
                "metric_name": "orders",
                "test_vs_test": False,
                "bootstrap_progress": False,
            }
        },
        progress=False,
    )

    assert len(calls) == 1
    call = calls[0]
    pd.testing.assert_frame_equal(call.pop("df"), pd.DataFrame({"user_id": [1]}))
    assert call.pop("pre_exp_metrics_df") is pre_df
    assert "pre_exp_df" not in call
    assert call == {
        "metric_name": "orders",
        "test_vs_test": False,
        "bootstrap_progress": False,
    }


def test_parallel_compute_metrics_inserts_labels_as_leading_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_result = pd.DataFrame({"metric_name": ["orders", "gmv"], "p-value": [0.1, 0.2]})

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        return raw_result

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    result = parallel_module.parallel_compute_metrics(
        {
            "segment_1": {
                "df": pd.DataFrame(),
                "labels": {"segment": "segment1", "country": "RU"},
            }
        },
        progress=False,
    )

    expected = pd.DataFrame(
        {
            "segment": ["segment1", "segment1"],
            "country": ["RU", "RU"],
            "metric_name": ["orders", "gmv"],
            "p-value": [0.1, 0.2],
        }
    )
    pd.testing.assert_frame_equal(result["segment_1"], expected)
    assert list(raw_result.columns) == ["metric_name", "p-value"]


def test_parallel_compute_metrics_limits_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = threading.Lock()
    active_tasks = 0
    max_active_tasks = 0

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        nonlocal active_tasks, max_active_tasks
        with lock:
            active_tasks += 1
            max_active_tasks = max(max_active_tasks, active_tasks)
        time.sleep(0.05)
        with lock:
            active_tasks -= 1
        return pd.DataFrame({"metric_name": [kwargs["metric_name"]]})

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    tasks = {
        f"task_{index}": {"df": pd.DataFrame(), "metric_name": f"metric_{index}"}
        for index in range(6)
    }

    result = parallel_module.parallel_compute_metrics(
        tasks,
        concurrency=2,
        progress=False,
    )

    assert list(result) == [f"task_{index}" for index in range(6)]
    assert max_active_tasks == 2


def test_parallel_compute_metrics_updates_progress_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame({"metric_name": [kwargs["metric_name"]]})

    monkeypatch.setattr(parallel_module, "tqdm", FakeTqdm)
    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    parallel_module.parallel_compute_metrics(
        {
            "first": {"df": pd.DataFrame(), "metric_name": "first"},
            "second": {"df": pd.DataFrame(), "metric_name": "second"},
        }
    )

    assert len(progress_bars) == 1
    progress_bar = progress_bars[0]
    assert progress_bar.kwargs == {
        "total": 2,
        "desc": "parallel_compute_metrics tasks",
        "unit": "task",
        "disable": False,
    }
    assert progress_bar.updates == [1, 1]
    assert progress_bar.closed


def test_parallel_compute_metrics_fail_fast_raises_original_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("metric failed")

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        raise error

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    with pytest.raises(RuntimeError) as exc_info:
        parallel_module.parallel_compute_metrics(
            {
                "broken": {"df": pd.DataFrame()},
                "also_broken": {"df": pd.DataFrame()},
            },
            concurrency=1,
            fail_fast=True,
            progress=False,
        )

    assert exc_info.value is error


def test_parallel_compute_metrics_fail_fast_false_returns_exception_strings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = RuntimeError("metric failed")

    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        if kwargs["metric_name"] == "broken":
            raise error
        return pd.DataFrame({"metric_name": [kwargs["metric_name"]]})

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    result = parallel_module.parallel_compute_metrics(
        {
            "ok": {"df": pd.DataFrame(), "metric_name": "ok"},
            "broken": {"df": pd.DataFrame(), "metric_name": "broken"},
            "ok_2": {"df": pd.DataFrame(), "metric_name": "ok_2"},
        },
        fail_fast=False,
        progress=False,
    )

    pd.testing.assert_frame_equal(result["ok"], pd.DataFrame({"metric_name": ["ok"]}))
    assert result["broken"] == str(error)
    pd.testing.assert_frame_equal(
        result["ok_2"],
        pd.DataFrame({"metric_name": ["ok_2"]}),
    )


@pytest.mark.parametrize(
    ("tasks", "expected_exception"),
    [
        ({}, ValueError),
        ([], TypeError),
        ({1: {"df": pd.DataFrame()}}, ValueError),
        ({"": {"df": pd.DataFrame()}}, ValueError),
        ({"task": "not a mapping"}, TypeError),
        ({"task": {}}, ValueError),
    ],
)
def test_parallel_compute_metrics_validates_task_input(
    tasks: Any,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        parallel_module.parallel_compute_metrics(tasks, progress=False)


@pytest.mark.parametrize(
    "labels",
    [
        ["not", "a", "mapping"],
        {"": "segment1"},
        {1: "segment1"},
        {"segment": ["segment1"]},
        {"segment": {"name": "segment1"}},
    ],
)
def test_parallel_compute_metrics_validates_labels(labels: Any) -> None:
    with pytest.raises((TypeError, ValueError), match="labels|label"):
        parallel_module.parallel_compute_metrics(
            {"task": {"df": pd.DataFrame(), "labels": labels}},
            progress=False,
        )


def test_parallel_compute_metrics_rejects_label_result_column_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_compute_test_metrics(**kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame({"metric_name": ["orders"]})

    monkeypatch.setattr(
        parallel_module,
        "compute_test_metrics",
        fake_compute_test_metrics,
    )

    with pytest.raises(ValueError, match="conflict"):
        parallel_module.parallel_compute_metrics(
            {
                "task": {
                    "df": pd.DataFrame(),
                    "labels": {"metric_name": "orders"},
                }
            },
            progress=False,
        )


@pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
def test_parallel_compute_metrics_validates_concurrency(concurrency: Any) -> None:
    with pytest.raises(ValueError, match="concurrency"):
        parallel_module.parallel_compute_metrics(
            {"task": {"df": pd.DataFrame()}},
            concurrency=concurrency,
            progress=False,
        )


@pytest.mark.parametrize("progress", [None, 0, 1, "yes"])
def test_parallel_compute_metrics_validates_progress(progress: Any) -> None:
    with pytest.raises(ValueError, match="progress"):
        parallel_module.parallel_compute_metrics(
            {"task": {"df": pd.DataFrame()}},
            progress=progress,
        )


def test_parallel_compute_metrics_rejects_ambiguous_pre_exp_aliases() -> None:
    with pytest.raises(ValueError, match="pre_exp_df"):
        parallel_module.parallel_compute_metrics(
            {
                "task": {
                    "df": pd.DataFrame(),
                    "pre_exp_df": pd.DataFrame(),
                    "pre_exp_metrics_df": pd.DataFrame(),
                }
            },
            progress=False,
        )


def test_parallel_compute_metrics_from_sql_loads_sql_and_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment_df = pd.DataFrame({"user_id": [1]})
    pre_exp_df = pd.DataFrame({"user_id": [1], "orders": [3]})
    second_df = pd.DataFrame({"user_id": [2]})
    async_calls: list[tuple[list[dict[str, Any]], dict[str, Any]]] = []
    compute_calls: list[tuple[dict[str, dict[str, Any]], dict[str, Any]]] = []

    def fake_async_sql(
        tasks: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, pd.DataFrame]:
        async_calls.append((tasks, kwargs))
        return {
            "with_pre:sql": experiment_df,
            "with_pre:pre_exp_sql": pre_exp_df,
            "without_pre:sql": second_df,
        }

    def fake_parallel_compute_metrics(
        tasks: dict[str, dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, pd.DataFrame]:
        compute_calls.append((tasks, kwargs))
        return {
            "with_pre": pd.DataFrame({"task": ["with_pre"]}),
            "without_pre": pd.DataFrame({"task": ["without_pre"]}),
        }

    monkeypatch.setattr(parallel_module, "async_sql", fake_async_sql)
    monkeypatch.setattr(
        parallel_module,
        "parallel_compute_metrics",
        fake_parallel_compute_metrics,
    )

    result = parallel_module.parallel_compute_metrics_from_sql(
        {
            "with_pre": {
                "sql": "select * from experiment_1",
                "pre_exp_sql": "select * from pre_experiment_1",
                "labels": {"segment": "segment1"},
                "test_vs_test": False,
                "bootstrap_progress": False,
            },
            "without_pre": {
                "sql": "select * from experiment_2",
                "labels": {"segment": "segment2"},
                "multiple_comparisons_adjustment": True,
            },
        },
        db="analytics_prod",
        concurrency=2,
        fail_fast=False,
        progress=False,
    )

    assert list(result) == ["with_pre", "without_pre"]
    pd.testing.assert_frame_equal(
        result["with_pre"],
        pd.DataFrame({"task": ["with_pre"]}),
    )
    pd.testing.assert_frame_equal(
        result["without_pre"],
        pd.DataFrame({"task": ["without_pre"]}),
    )

    assert len(async_calls) == 1
    sql_tasks, sql_kwargs = async_calls[0]
    assert sql_kwargs == {"concurrency": 2, "fail_fast": False, "progress": False}
    assert sql_tasks == [
        {
            "name": "with_pre:sql",
            "type": "read",
            "connection_type": "analytics_prod",
            "query": "select * from experiment_1",
        },
        {
            "name": "with_pre:pre_exp_sql",
            "type": "read",
            "connection_type": "analytics_prod",
            "query": "select * from pre_experiment_1",
        },
        {
            "name": "without_pre:sql",
            "type": "read",
            "connection_type": "analytics_prod",
            "query": "select * from experiment_2",
        },
    ]

    assert len(compute_calls) == 1
    metric_tasks, metric_kwargs = compute_calls[0]
    assert metric_kwargs == {"concurrency": 2, "fail_fast": False, "progress": False}
    assert list(metric_tasks) == ["with_pre", "without_pre"]

    with_pre = dict(metric_tasks["with_pre"])
    assert with_pre.pop("df") is experiment_df
    assert with_pre.pop("pre_exp_df") is pre_exp_df
    assert with_pre == {
        "labels": {"segment": "segment1"},
        "test_vs_test": False,
        "bootstrap_progress": False,
    }

    without_pre = dict(metric_tasks["without_pre"])
    assert without_pre.pop("df") is second_df
    assert without_pre == {
        "labels": {"segment": "segment2"},
        "multiple_comparisons_adjustment": True,
    }


def test_parallel_compute_metrics_from_sql_fail_fast_false_returns_sql_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ok_df = pd.DataFrame({"user_id": [1]})
    skipped_df = pd.DataFrame({"user_id": [2]})
    computed = pd.DataFrame({"metric_name": ["orders"]})
    compute_calls: list[dict[str, dict[str, Any]]] = []

    def fake_async_sql(
        tasks: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, pd.DataFrame | str]:
        assert kwargs["fail_fast"] is False
        return {
            "ok:sql": ok_df,
            "broken:sql": "database failed",
            "pre_broken:sql": skipped_df,
            "pre_broken:pre_exp_sql": "pre query failed",
        }

    def fake_parallel_compute_metrics(
        tasks: dict[str, dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, pd.DataFrame]:
        compute_calls.append(tasks)
        return {"ok": computed}

    monkeypatch.setattr(parallel_module, "async_sql", fake_async_sql)
    monkeypatch.setattr(
        parallel_module,
        "parallel_compute_metrics",
        fake_parallel_compute_metrics,
    )

    result = parallel_module.parallel_compute_metrics_from_sql(
        {
            "ok": {"sql": "select * from ok"},
            "broken": {"sql": "select * from broken"},
            "pre_broken": {
                "sql": "select * from pre_broken",
                "pre_exp_sql": "select * from pre_exp",
            },
        },
        db="analytics_prod",
        fail_fast=False,
        progress=False,
    )

    assert list(result) == ["ok", "broken", "pre_broken"]
    pd.testing.assert_frame_equal(result["ok"], computed)
    assert result["broken"] == "database failed"
    assert result["pre_broken"] == "pre query failed"
    assert len(compute_calls) == 1
    assert list(compute_calls[0]) == ["ok"]
    assert compute_calls[0]["ok"]["df"] is ok_df


@pytest.mark.parametrize(
    ("tasks", "expected_exception"),
    [
        ({}, ValueError),
        ([], TypeError),
        ({1: {"sql": "select 1"}}, ValueError),
        ({"": {"sql": "select 1"}}, ValueError),
        ({"task": "not a mapping"}, TypeError),
        ({"task": {}}, ValueError),
        ({"task": {"sql": ""}}, ValueError),
        ({"task": {"sql": "select 1", "pre_exp_sql": ""}}, ValueError),
    ],
)
def test_parallel_compute_metrics_from_sql_validates_task_input(
    tasks: Any,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        parallel_module.parallel_compute_metrics_from_sql(
            tasks,
            db="analytics_prod",
            progress=False,
        )


@pytest.mark.parametrize("field", ["df", "pre_exp_df", "pre_exp_metrics_df"])
def test_parallel_compute_metrics_from_sql_rejects_dataframe_inputs(field: str) -> None:
    with pytest.raises(ValueError, match="SQL-backed"):
        parallel_module.parallel_compute_metrics_from_sql(
            {
                "task": {
                    "sql": "select 1",
                    field: pd.DataFrame(),
                }
            },
            db="analytics_prod",
            progress=False,
        )
