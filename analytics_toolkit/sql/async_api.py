from __future__ import annotations

import asyncio
import contextvars
import inspect
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from queue import Queue
from threading import Thread
from typing import Any

from .dml.io.execute_read import execute_read
from .dml.io.execute_sql import execute_sql
from .dml.io.read_sql import read_sql
from .dml.load.load_df import load_df
from .dml.transfer.flow.api import transfer_table

_SUPPORTED_TASK_TYPES = frozenset(
    {"read", "execute", "execute_read", "load_df", "transfer", "custom_sql_pipeline"}
)
_PIPELINE_TASK_TYPE = "custom_sql_pipeline"
_DEFAULT_HARD_CONCURRENCY_CAP = 10
_CONCURRENCY_STATE: contextvars.ContextVar["_ConcurrencyState | None"] = (
    contextvars.ContextVar("analytics_toolkit_async_sql_concurrency", default=None)
)


@dataclass(frozen=True)
class _ConcurrencyState:
    effective_concurrency: int
    hard_cap: int
    soft_cap: int
    semaphores: tuple[asyncio.Semaphore, ...]


@dataclass
class _PipelineContext:
    task_name: str
    step_index: int = 0
    results: list[Any] = field(default_factory=list)

    @property
    def last_result(self) -> Any:
        if not self.results:
            return None
        return self.results[-1]


def async_sql(
    tasks: Sequence[Mapping[str, Any]],
    *,
    concurrency: int = 5,
    fail_fast: bool = True,
    soft_concurrency_cap: int | None = None,
    hard_concurrency_cap: int = _DEFAULT_HARD_CONCURRENCY_CAP,
) -> dict[str, Any]:
    """Run independent SQL tasks concurrently and return a result dictionary."""
    return _run_coroutine_sync(
        lambda: _async_sql_impl(
            tasks,
            concurrency=concurrency,
            fail_fast=fail_fast,
            soft_concurrency_cap=soft_concurrency_cap,
            hard_concurrency_cap=hard_concurrency_cap,
        )
    )


async def _async_sql_impl(
    tasks: Sequence[Mapping[str, Any]],
    *,
    concurrency: int = 5,
    fail_fast: bool = True,
    soft_concurrency_cap: int | None = None,
    hard_concurrency_cap: int = _DEFAULT_HARD_CONCURRENCY_CAP,
) -> dict[str, Any]:
    task_defs = _validate_tasks(tasks)
    _validate_concurrency(concurrency)
    _validate_optional_soft_concurrency_cap(soft_concurrency_cap)
    _validate_hard_concurrency_cap(hard_concurrency_cap)
    state = _build_concurrency_state(
        concurrency=concurrency,
        soft_concurrency_cap=soft_concurrency_cap,
        hard_concurrency_cap=hard_concurrency_cap,
    )
    reset_token = _CONCURRENCY_STATE.set(state)

    semaphore = asyncio.Semaphore(concurrency)

    async def run_task(name: str, task_type: str, kwargs: dict[str, Any]) -> Any:
        async with semaphore:
            if task_type == _PIPELINE_TASK_TYPE:
                return await _run_pipeline(
                    name,
                    kwargs["steps"],
                    state.semaphores,
                )
            return await _run_blocking(
                state.semaphores,
                _run_sync_task,
                task_type,
                kwargs,
            )

    async def run_indexed(
        index: int,
        name: str,
        task_type: str,
        kwargs: dict[str, Any],
    ) -> tuple[int, Any]:
        return index, await run_task(name, task_type, kwargs)

    try:
        async_tasks = [
            asyncio.create_task(run_indexed(index, name, task_type, kwargs))
            for index, (name, task_type, kwargs) in enumerate(task_defs)
        ]

        if fail_fast:
            results_by_index: dict[int, Any] = {}
            try:
                for finished in asyncio.as_completed(async_tasks):
                    index, result = await finished
                    results_by_index[index] = result
            except BaseException:
                for task in async_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*async_tasks, return_exceptions=True)
                raise

            return {
                name: results_by_index[index]
                for index, (name, _task_type, _kwargs) in enumerate(task_defs)
            }

        indexed_results = await asyncio.gather(*async_tasks, return_exceptions=True)
        results_by_index: dict[int, Any] = {}
        for default_index, item in enumerate(indexed_results):
            if isinstance(item, BaseException):
                results_by_index[default_index] = item
            else:
                index, result = item
                results_by_index[index] = result

        return {
            name: results_by_index[index]
            for index, (name, _task_type, _kwargs) in enumerate(task_defs)
        }
    finally:
        _CONCURRENCY_STATE.reset(reset_token)


def _run_coroutine_sync(
    coroutine_factory: Callable[[], Coroutine[Any, Any, dict[str, Any]]],
) -> dict[str, Any]:
    if _is_event_loop_running():
        return _run_coroutine_sync_in_thread(coroutine_factory)
    return asyncio.run(coroutine_factory())


def _run_coroutine_sync_in_thread(
    coroutine_factory: Callable[[], Coroutine[Any, Any, dict[str, Any]]],
) -> dict[str, Any]:
    queue: Queue[tuple[bool, dict[str, Any] | BaseException, Any | None]] = Queue(
        maxsize=1
    )

    def run() -> None:
        try:
            queue.put((True, asyncio.run(coroutine_factory()), None))
        except BaseException as exc:
            queue.put((False, exc, exc.__traceback__))

    thread = Thread(target=run, daemon=True)
    thread.start()
    ok, value, traceback = queue.get()
    thread.join()

    if ok:
        return value  # type: ignore[return-value]
    raise value.with_traceback(traceback)  # type: ignore[union-attr]


def _is_event_loop_running() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _build_concurrency_state(
    *,
    concurrency: int,
    soft_concurrency_cap: int | None,
    hard_concurrency_cap: int,
) -> _ConcurrencyState:
    active_state = _CONCURRENCY_STATE.get()
    if active_state is None:
        hard_cap = hard_concurrency_cap
        soft_cap = concurrency if soft_concurrency_cap is None else soft_concurrency_cap
        semaphores = (asyncio.Semaphore(soft_cap),)
        effective_concurrency = concurrency
    else:
        hard_cap = active_state.hard_cap
        if (
            hard_concurrency_cap != _DEFAULT_HARD_CONCURRENCY_CAP
            and hard_concurrency_cap >= hard_cap
        ):
            hard_cap = hard_concurrency_cap

        soft_cap = active_state.soft_cap
        semaphores = active_state.semaphores
        if soft_concurrency_cap is not None and soft_concurrency_cap < soft_cap:
            soft_cap = soft_concurrency_cap
            semaphores = (*semaphores, asyncio.Semaphore(soft_cap))

        effective_concurrency = active_state.effective_concurrency * concurrency

    actual_worker_ceiling = min(effective_concurrency, soft_cap)
    if actual_worker_ceiling > hard_cap:
        raise ValueError(
            "effective concurrency exceeds hard_concurrency_cap "
            f"({actual_worker_ceiling} > {hard_cap}). Reduce concurrency, set "
            "soft_concurrency_cap at or below hard_concurrency_cap, or increase "
            "hard_concurrency_cap."
        )

    return _ConcurrencyState(
        effective_concurrency=effective_concurrency,
        hard_cap=hard_cap,
        soft_cap=soft_cap,
        semaphores=semaphores,
    )


async def _run_pipeline(
    task_name: str,
    steps: Sequence[Any],
    soft_semaphores: tuple[asyncio.Semaphore, ...],
) -> Any:
    context = _PipelineContext(task_name=task_name)
    for index, step in enumerate(steps):
        context.step_index = index
        if _is_async_callable(step):
            result = await step(context)
        else:
            result = await _run_blocking(soft_semaphores, step, context)
        context.results.append(result)

    return context.last_result


async def _run_blocking(
    soft_semaphores: tuple[asyncio.Semaphore, ...],
    func: Any,
    *args: Any,
) -> Any:
    async with AsyncExitStack() as stack:
        for semaphore in reversed(soft_semaphores):
            await stack.enter_async_context(semaphore)
        return await asyncio.to_thread(func, *args)


def _is_async_callable(func: Any) -> bool:
    if inspect.iscoroutinefunction(func):
        return True
    call = getattr(func, "__call__", None)
    return inspect.iscoroutinefunction(call)


def _validate_tasks(
    tasks: Sequence[Mapping[str, Any]],
) -> list[tuple[str, str, dict[str, Any]]]:
    if isinstance(tasks, Sequence) and not isinstance(
        tasks,
        (str, bytes, bytearray),
    ):
        return _validate_task_sequence(tasks)
    raise TypeError("tasks must be a non-empty sequence of task mappings.")


def _validate_task_sequence(
    tasks: Sequence[Mapping[str, Any]],
) -> list[tuple[str, str, dict[str, Any]]]:
    if not tasks:
        raise ValueError("tasks must be a non-empty sequence.")

    task_defs: list[tuple[str, str, dict[str, Any]]] = []
    for index, spec in enumerate(tasks):
        if not isinstance(spec, Mapping):
            raise TypeError(f"Task at index {index} must be a mapping.")
        spec_dict = dict(spec)
        task_name = spec_dict.pop("name", f"task_{index}")
        if not isinstance(task_name, str) or not task_name.strip():
            raise ValueError(
                f"Task at index {index} has invalid name; expected a non-empty string."
            )
        task_defs.append(_validate_task_spec(task_name, spec_dict))

    return task_defs


def _validate_task_spec(
    name: str,
    spec: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    kwargs = dict(spec)
    task_type = kwargs.pop("type", None)
    if task_type is None:
        raise ValueError(f"Task {name!r} must define a type.")
    if not isinstance(task_type, str) or task_type not in _SUPPORTED_TASK_TYPES:
        supported = ", ".join(sorted(_SUPPORTED_TASK_TYPES))
        raise ValueError(
            f"Task {name!r} has unsupported type {task_type!r}. "
            f"Expected one of: {supported}."
        )
    if task_type == _PIPELINE_TASK_TYPE:
        kwargs = _validate_pipeline_task(name, kwargs)
    return name, task_type, kwargs


def _validate_pipeline_task(name: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    extra_fields = sorted(set(kwargs) - {"steps"})
    if extra_fields:
        fields = ", ".join(extra_fields)
        raise ValueError(
            f"Task {name!r} has unsupported custom_sql_pipeline field(s): {fields}."
        )

    if "steps" not in kwargs:
        raise ValueError(f"Task {name!r} must define steps.")

    steps = kwargs["steps"]
    if isinstance(steps, (str, bytes)) or not isinstance(steps, Sequence):
        raise TypeError(f"Task {name!r} steps must be a non-empty sequence.")
    if not steps:
        raise ValueError(f"Task {name!r} steps must be a non-empty sequence.")

    steps_list = list(steps)
    for index, step in enumerate(steps_list):
        if not callable(step):
            raise TypeError(f"Task {name!r} step {index} must be callable.")

    return {"steps": steps_list}


def _validate_concurrency(concurrency: int) -> None:
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 1
    ):
        raise ValueError("concurrency must be an integer >= 1.")


def _validate_optional_soft_concurrency_cap(
    soft_concurrency_cap: int | None,
) -> None:
    if soft_concurrency_cap is None:
        return
    if (
        not isinstance(soft_concurrency_cap, int)
        or isinstance(soft_concurrency_cap, bool)
        or soft_concurrency_cap < 1
    ):
        raise ValueError("soft_concurrency_cap must be an integer >= 1.")


def _validate_hard_concurrency_cap(hard_concurrency_cap: int) -> None:
    if (
        not isinstance(hard_concurrency_cap, int)
        or isinstance(hard_concurrency_cap, bool)
        or hard_concurrency_cap < 1
    ):
        raise ValueError("hard_concurrency_cap must be an integer >= 1.")


def _run_sync_task(task_type: str, kwargs: dict[str, Any]) -> Any:
    if task_type == "read":
        return read_sql(**kwargs)
    if task_type == "execute":
        return execute_sql(**kwargs)
    if task_type == "execute_read":
        return execute_read(**kwargs)
    if task_type == "load_df":
        return load_df(**kwargs)
    if task_type == "transfer":
        return transfer_table(**kwargs)
    raise ValueError(f"Unsupported task type: {task_type!r}.")
