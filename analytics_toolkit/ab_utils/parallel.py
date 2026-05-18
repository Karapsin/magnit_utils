from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
from pandas.api.types import is_scalar
from tqdm import tqdm

from analytics_toolkit.sql import async_sql

from .api import compute_test_metrics

_SQL_DATAFRAME_FIELDS = frozenset({"df", "pre_exp_df", "pre_exp_metrics_df"})


def parallel_compute_metrics(
    tasks: Mapping[str, Mapping[str, Any]],
    *,
    concurrency: int = 5,
    fail_fast: bool = True,
    progress: bool = True,
) -> dict[str, pd.DataFrame | str]:
    """Run independent ``compute_test_metrics`` tasks concurrently."""
    task_defs = _validate_tasks(tasks)
    _validate_concurrency(concurrency)
    _validate_progress(progress)

    results_by_index: dict[int, pd.DataFrame | str] = {}
    executor = ThreadPoolExecutor(max_workers=concurrency)
    shutdown_called = False
    progress_bar = _make_progress_bar(total=len(task_defs), progress=progress)

    try:
        future_to_index: dict[Future[pd.DataFrame], int] = {
            executor.submit(_run_task, kwargs, labels): index
            for index, (_name, kwargs, labels) in enumerate(task_defs)
        }

        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results_by_index[index] = future.result()
            except BaseException as exc:
                if fail_fast:
                    for pending in future_to_index:
                        if pending is not future:
                            pending.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    shutdown_called = True
                    raise
                results_by_index[index] = str(exc)
            finally:
                progress_bar.update(1)

        return {
            name: results_by_index[index]
            for index, (name, _kwargs, _labels) in enumerate(task_defs)
        }
    finally:
        progress_bar.close()
        if not shutdown_called:
            executor.shutdown(wait=True, cancel_futures=True)


def parallel_compute_metrics_from_sql(
    tasks: Mapping[str, Mapping[str, Any]],
    db: str,
    *,
    concurrency: int = 5,
    fail_fast: bool = True,
    progress: bool = True,
) -> dict[str, pd.DataFrame | str]:
    """Load SQL-backed task dataframes, then run ``parallel_compute_metrics``."""
    task_defs = _validate_sql_tasks(tasks)
    _validate_concurrency(concurrency)
    _validate_progress(progress)

    sql_tasks: list[dict[str, Any]] = []
    for name, kwargs, sql, pre_exp_sql in task_defs:
        sql_tasks.append(
            {
                "name": _sql_read_task_name(name, "sql"),
                "type": "read",
                "connection_type": db,
                "query": sql,
            }
        )
        if pre_exp_sql is not None:
            sql_tasks.append(
                {
                    "name": _sql_read_task_name(name, "pre_exp_sql"),
                    "type": "read",
                    "connection_type": db,
                    "query": pre_exp_sql,
                }
            )

    sql_results = async_sql(
        sql_tasks,
        concurrency=concurrency,
        fail_fast=fail_fast,
        progress=progress,
    )

    metric_tasks: dict[str, dict[str, Any]] = {}
    sql_failures: dict[str, str] = {}
    for name, kwargs, _sql, pre_exp_sql in task_defs:
        df_result = sql_results[_sql_read_task_name(name, "sql")]
        if isinstance(df_result, str):
            sql_failures[name] = df_result
            continue

        metric_kwargs = dict(kwargs)
        metric_kwargs["df"] = df_result
        if pre_exp_sql is not None:
            pre_exp_df_result = sql_results[_sql_read_task_name(name, "pre_exp_sql")]
            if isinstance(pre_exp_df_result, str):
                sql_failures[name] = pre_exp_df_result
                continue
            metric_kwargs["pre_exp_df"] = pre_exp_df_result
        metric_tasks[name] = metric_kwargs

    metric_results = (
        parallel_compute_metrics(
            metric_tasks,
            concurrency=concurrency,
            fail_fast=fail_fast,
            progress=progress,
        )
        if metric_tasks
        else {}
    )

    return {
        name: sql_failures[name] if name in sql_failures else metric_results[name]
        for name, _kwargs, _sql, _pre_exp_sql in task_defs
    }


def _make_progress_bar(*, total: int, progress: bool) -> Any:
    return tqdm(
        total=total,
        desc="parallel_compute_metrics tasks",
        unit="task",
        disable=not progress,
    )


def _run_task(kwargs: dict[str, Any], labels: dict[str, Any]) -> pd.DataFrame:
    result = compute_test_metrics(**kwargs)
    if not labels:
        return result

    labeled_result = result.copy()
    conflicts = [column for column in labels if column in labeled_result.columns]
    if conflicts:
        fields = ", ".join(conflicts)
        raise ValueError(f"Label column(s) conflict with result columns: {fields}.")

    for index, (column, value) in enumerate(labels.items()):
        labeled_result.insert(index, column, value)
    return labeled_result


def _validate_tasks(
    tasks: Mapping[str, Mapping[str, Any]],
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    if not isinstance(tasks, Mapping):
        raise TypeError("tasks must be a non-empty mapping of task names to task mappings.")
    if not tasks:
        raise ValueError("tasks must be a non-empty mapping.")

    task_defs: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for name, spec in tasks.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Task names must be non-empty strings.")
        if not isinstance(spec, Mapping):
            raise TypeError(f"Task {name!r} must be a mapping.")
        task_defs.append(_validate_task_spec(name, spec))
    return task_defs


def _validate_task_spec(
    name: str,
    spec: Mapping[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    kwargs = dict(spec)
    if "df" not in kwargs:
        raise ValueError(f"Task {name!r} must define df.")

    labels = _validate_labels(name, kwargs.pop("labels", None))
    pre_exp_df = kwargs.pop("pre_exp_df", None)
    if pre_exp_df is not None:
        if "pre_exp_metrics_df" in kwargs:
            raise ValueError(
                f"Task {name!r} cannot define both pre_exp_df and pre_exp_metrics_df."
            )
        kwargs["pre_exp_metrics_df"] = pre_exp_df

    return name, kwargs, labels


def _validate_sql_tasks(
    tasks: Mapping[str, Mapping[str, Any]],
) -> list[tuple[str, dict[str, Any], str, str | None]]:
    if not isinstance(tasks, Mapping):
        raise TypeError("tasks must be a non-empty mapping of task names to task mappings.")
    if not tasks:
        raise ValueError("tasks must be a non-empty mapping.")

    task_defs: list[tuple[str, dict[str, Any], str, str | None]] = []
    for name, spec in tasks.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Task names must be non-empty strings.")
        if not isinstance(spec, Mapping):
            raise TypeError(f"Task {name!r} must be a mapping.")
        task_defs.append(_validate_sql_task_spec(name, spec))
    return task_defs


def _validate_sql_task_spec(
    name: str,
    spec: Mapping[str, Any],
) -> tuple[str, dict[str, Any], str, str | None]:
    kwargs = dict(spec)
    ambiguous_fields = sorted(_SQL_DATAFRAME_FIELDS.intersection(kwargs))
    if ambiguous_fields:
        fields = ", ".join(ambiguous_fields)
        raise ValueError(
            f"Task {name!r} cannot define dataframe field(s) for SQL-backed "
            f"inputs: {fields}."
        )

    if "sql" not in kwargs:
        raise ValueError(f"Task {name!r} must define sql.")
    sql = kwargs.pop("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError(f"Task {name!r} sql must be a non-empty string.")

    pre_exp_sql = kwargs.pop("pre_exp_sql", None)
    if pre_exp_sql is not None and (
        not isinstance(pre_exp_sql, str) or not pre_exp_sql.strip()
    ):
        raise ValueError(f"Task {name!r} pre_exp_sql must be a non-empty string.")

    _validate_labels(name, kwargs.get("labels"))
    return name, kwargs, sql, pre_exp_sql


def _sql_read_task_name(name: str, field: str) -> str:
    return f"{name}:{field}"


def _validate_labels(name: str, labels: Any) -> dict[str, Any]:
    if labels is None:
        return {}
    if not isinstance(labels, Mapping):
        raise TypeError(f"Task {name!r} labels must be a mapping.")

    labels_dict = dict(labels)
    for column, value in labels_dict.items():
        if not isinstance(column, str) or not column.strip():
            raise ValueError(f"Task {name!r} label columns must be non-empty strings.")
        if not is_scalar(value):
            raise ValueError(f"Task {name!r} label {column!r} must be a scalar value.")
    return labels_dict


def _validate_concurrency(concurrency: int) -> None:
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 1
    ):
        raise ValueError("concurrency must be an integer >= 1.")


def _validate_progress(progress: bool) -> None:
    if not isinstance(progress, bool):
        raise ValueError("progress must be a boolean.")
