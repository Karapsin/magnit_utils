from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd

from ...connection.config import get_connection_config
from ...connection.errors import UnsupportedConnectionTypeError
from ...operation_runner import timed_public_sql_function
from .read_sql import read_sql


_GP_RUNNING_QUERY_PIDS_SQL = """select pid
from pg_stat_activity
where usename = current_user
  and pid <> pg_backend_pid()"""
_GP_CANCEL_RESULT_COLUMNS = ["pid", "cancel_query", "cancelled"]


@timed_public_sql_function
def gp_cancel_all_running_queries(
    connection_key: str = "gp",
    concurrency: int = 1,
    print_queries: bool = False,
    retry_cnt: int = 5,
    timeout_increment: int | float = 5,
    query_label: str | None = None,
) -> pd.DataFrame:
    _validate_concurrency(concurrency)
    config = get_connection_config(connection_key)
    if config.backend != "gp":
        raise UnsupportedConnectionTypeError(
            "gp_cancel_all_running_queries requires a gp connection, "
            f"got '{config.backend}'."
        )

    connection_key = config.connection_key
    pid_rows = read_sql(
        connection_key,
        _GP_RUNNING_QUERY_PIDS_SQL,
        print_queries=print_queries,
        retry_cnt=retry_cnt,
        timeout_increment=timeout_increment,
        query_label=query_label,
    )
    if pid_rows.empty:
        return pd.DataFrame(columns=_GP_CANCEL_RESULT_COLUMNS)

    pids = [int(pid) for pid in pid_rows["pid"].tolist()]

    def cancel_pid(pid: int) -> dict[str, Any]:
        cancel_query = f"select pg_cancel_backend({pid}) as cancelled"
        cancel_result = read_sql(
            connection_key,
            cancel_query,
            print_queries=print_queries,
            retry_cnt=retry_cnt,
            timeout_increment=timeout_increment,
            query_label=query_label,
        )
        return {
            "pid": pid,
            "cancel_query": cancel_query,
            "cancelled": cancel_result["cancelled"].iloc[0],
        }

    if concurrency == 1:
        results = [cancel_pid(pid) for pid in pids]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = list(executor.map(cancel_pid, pids))

    return pd.DataFrame(results, columns=_GP_CANCEL_RESULT_COLUMNS)


def _validate_concurrency(concurrency: int) -> None:
    if (
        isinstance(concurrency, bool)
        or not isinstance(concurrency, int)
        or concurrency < 1
    ):
        raise ValueError("concurrency must be an integer >= 1.")
