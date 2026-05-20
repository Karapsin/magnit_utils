from __future__ import annotations

import importlib
from typing import Any

import pandas as pd
import pytest


gp_cancel_module = importlib.import_module("analytics_toolkit.sql.dml.io.gp_cancel")
dml_io_module = importlib.import_module("analytics_toolkit.sql.dml.io")
dml_module = importlib.import_module("analytics_toolkit.sql.dml")
sql_module = importlib.import_module("analytics_toolkit.sql")


PID_QUERY = """select pid
from pg_stat_activity
where usename = current_user
  and pid <> pg_backend_pid()"""


def test_gp_cancel_all_running_queries_is_exported() -> None:
    assert (
        sql_module.gp_cancel_all_running_queries
        is gp_cancel_module.gp_cancel_all_running_queries
    )
    assert (
        dml_module.gp_cancel_all_running_queries
        is gp_cancel_module.gp_cancel_all_running_queries
    )
    assert (
        dml_io_module.gp_cancel_all_running_queries
        is gp_cancel_module.gp_cancel_all_running_queries
    )


def test_gp_cancel_reads_pids_and_cancels_sequentially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_read_sql(
        connection_key: str,
        query: str,
        print_queries: bool = True,
        retry_cnt: int = 5,
        timeout_increment: int | float = 5,
        query_label: str | None = None,
    ) -> pd.DataFrame:
        calls.append(
            {
                "connection_key": connection_key,
                "query": query,
                "print_queries": print_queries,
                "retry_cnt": retry_cnt,
                "timeout_increment": timeout_increment,
                "query_label": query_label,
            }
        )
        if query == PID_QUERY:
            return pd.DataFrame({"pid": [42, 7]})
        if query == "select pg_cancel_backend(42) as cancelled":
            return pd.DataFrame({"cancelled": [True]})
        if query == "select pg_cancel_backend(7) as cancelled":
            return pd.DataFrame({"cancelled": [False]})
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(gp_cancel_module, "read_sql", fake_read_sql)

    result = gp_cancel_module.gp_cancel_all_running_queries(
        "gp",
        concurrency=1,
        print_queries=False,
        retry_cnt=2,
        timeout_increment=0.5,
        query_label="cancel-tests",
    )

    assert [call["query"] for call in calls] == [
        PID_QUERY,
        "select pg_cancel_backend(42) as cancelled",
        "select pg_cancel_backend(7) as cancelled",
    ]
    assert all(call["connection_key"] == "gp" for call in calls)
    assert all(call["print_queries"] is False for call in calls)
    assert all(call["retry_cnt"] == 2 for call in calls)
    assert all(call["timeout_increment"] == 0.5 for call in calls)
    assert all(call["query_label"] == "cancel-tests" for call in calls)
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "pid": [42, 7],
                "cancel_query": [
                    "select pg_cancel_backend(42) as cancelled",
                    "select pg_cancel_backend(7) as cancelled",
                ],
                "cancelled": [True, False],
            }
        ),
    )


def test_gp_cancel_concurrent_path_preserves_result_pid_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class RecordingExecutor:
        max_workers_seen: list[int] = []
        mapped_items: list[list[int]] = []

        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers
            self.max_workers_seen.append(max_workers)

        def __enter__(self) -> RecordingExecutor:
            return self

        def __exit__(
            self,
            exc_type: object,
            exc: object,
            traceback: object,
        ) -> None:
            return None

        def map(self, fn: Any, iterable: Any) -> list[dict[str, object]]:
            items = list(iterable)
            self.mapped_items.append(items)
            return [fn(item) for item in items]

    def fake_read_sql(
        connection_key: str,
        query: str,
        print_queries: bool = True,
        retry_cnt: int = 5,
        timeout_increment: int | float = 5,
        query_label: str | None = None,
    ) -> pd.DataFrame:
        del connection_key, print_queries, retry_cnt, timeout_increment, query_label
        calls.append(query)
        if query == PID_QUERY:
            return pd.DataFrame({"pid": [3, 1, 2]})
        if query.startswith("select pg_cancel_backend("):
            return pd.DataFrame({"cancelled": [query.endswith("(1) as cancelled")]})
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(gp_cancel_module, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr(gp_cancel_module, "read_sql", fake_read_sql)

    result = gp_cancel_module.gp_cancel_all_running_queries(
        "gp",
        concurrency=3,
        print_queries=False,
        retry_cnt=1,
        timeout_increment=0,
    )

    assert RecordingExecutor.max_workers_seen == [3]
    assert RecordingExecutor.mapped_items == [[3, 1, 2]]
    assert calls == [
        PID_QUERY,
        "select pg_cancel_backend(3) as cancelled",
        "select pg_cancel_backend(1) as cancelled",
        "select pg_cancel_backend(2) as cancelled",
    ]
    assert result["pid"].tolist() == [3, 1, 2]
    assert result["cancel_query"].tolist() == [
        "select pg_cancel_backend(3) as cancelled",
        "select pg_cancel_backend(1) as cancelled",
        "select pg_cancel_backend(2) as cancelled",
    ]
    assert result["cancelled"].tolist() == [False, True, False]


def test_gp_cancel_empty_pid_result_returns_expected_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    print_flags: list[bool] = []

    def fake_read_sql(
        connection_key: str,
        query: str,
        print_queries: bool = True,
        retry_cnt: int = 5,
        timeout_increment: int | float = 5,
        query_label: str | None = None,
    ) -> pd.DataFrame:
        del connection_key, retry_cnt, timeout_increment, query_label
        print_flags.append(print_queries)
        calls.append(query)
        return pd.DataFrame({"pid": []})

    monkeypatch.setattr(gp_cancel_module, "read_sql", fake_read_sql)

    result = gp_cancel_module.gp_cancel_all_running_queries("gp")

    assert calls == [PID_QUERY]
    assert print_flags == [False]
    assert result.empty
    assert result.columns.tolist() == ["pid", "cancel_query", "cancelled"]


@pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
def test_gp_cancel_rejects_invalid_concurrency(concurrency: Any) -> None:
    with pytest.raises(ValueError, match="concurrency"):
        gp_cancel_module.gp_cancel_all_running_queries(concurrency=concurrency)


def test_gp_cancel_rejects_non_gp_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gp_cancel_module,
        "read_sql",
        lambda *args, **kwargs: pytest.fail("read_sql should not be called"),
    )

    with pytest.raises(
        gp_cancel_module.UnsupportedConnectionTypeError,
        match="requires a gp connection",
    ):
        gp_cancel_module.gp_cancel_all_running_queries("trino")
