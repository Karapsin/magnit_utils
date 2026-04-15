from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from typing import Literal
from warnings import warn

from dateutil.relativedelta import relativedelta

DateInput = str | date | datetime
OutputPeriod = Literal["week", "month"]
OutputInterval = Literal["days", "weeks", "months"]


def gen_dates_list(
    start_dt: DateInput,
    end_dt: DateInput,
    interval: str = "days",
    output_string: bool = True,
) -> list[str] | list[datetime]:
    start_date = _to_date(start_dt)
    end_date = _to_date(end_dt)
    normalized_interval = _normalize_interval(interval)

    if normalized_interval == "weeks":
        truncated_start = _period_start(start_date, "week")
        truncated_end = _period_start(end_date, "week")
        _warn_if_truncated(start_date, truncated_start, "week", "start_dt")
        _warn_if_truncated(end_date, truncated_end, "week", "end_dt")
        start_date = truncated_start
        end_date = truncated_end
    elif normalized_interval == "months":
        truncated_start = _period_start(start_date, "month")
        truncated_end = _period_start(end_date, "month")
        _warn_if_truncated(start_date, truncated_start, "month", "start_dt")
        _warn_if_truncated(end_date, truncated_end, "month", "end_dt")
        start_date = truncated_start
        end_date = truncated_end

    if end_date < start_date:
        return []

    result: list[str] | list[datetime] = []
    current = start_date
    while current <= end_date:
        result.append(_format_output(current, output_string))
        current = _add_interval(current, normalized_interval)

    return result


def first_day(
    dt: DateInput,
    period: str = "month",
    output_string: bool = True,
) -> str | datetime:
    normalized_period = _normalize_period(period)
    result = _period_start(_to_date(dt), normalized_period)
    return _format_output(result, output_string)


def last_day(
    dt: DateInput,
    period: str = "month",
    output_string: bool = True,
) -> str | datetime:
    normalized_period = _normalize_period(period)
    result = _period_end(_to_date(dt), normalized_period)
    return _format_output(result, output_string)


def add_days(
    dt: DateInput,
    n: int,
    output_string: bool = True,
) -> str | datetime:
    result = _to_date(dt) + timedelta(days=n)
    return _format_output(result, output_string)


def add_weeks(
    dt: DateInput,
    n: int,
    output_string: bool = True,
) -> str | datetime:
    result = _period_start(_to_date(dt), "week") + timedelta(weeks=n)
    return _format_output(result, output_string)


def add_months(
    dt: DateInput,
    n: int,
    output_string: bool = True,
) -> str | datetime:
    result = _period_start(_to_date(dt), "month") + relativedelta(months=n)
    return _format_output(result, output_string)


def get_today(output_string: bool = True) -> str | datetime:
    return _format_output(date.today(), output_string)


def get_random_day(
    start_dt: DateInput,
    end_dt: DateInput,
    output_string: bool = True,
) -> str | datetime:
    start_date = _to_date(start_dt)
    end_date = _to_date(end_dt)

    if end_date < start_date:
        raise ValueError("end_dt must be greater than or equal to start_dt.")

    random_days = random.randint(0, (end_date - start_date).days)
    result = start_date + timedelta(days=random_days)
    return _format_output(result, output_string)


def _to_date(value: DateInput) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    raise TypeError("Date value must be a string, date, or datetime.")


def _format_output(value: date, output_string: bool) -> str | datetime:
    if output_string:
        return value.isoformat()
    return datetime.combine(value, time.min)


def _normalize_interval(interval: str) -> OutputInterval:
    normalized = interval.strip().lower()
    if normalized in {"day", "days"}:
        return "days"
    if normalized in {"week", "weeks"}:
        return "weeks"
    if normalized in {"month", "months"}:
        return "months"
    raise ValueError("interval must be one of: 'days', 'weeks', 'months'.")


def _normalize_period(period: str) -> OutputPeriod:
    normalized = period.strip().lower()
    if normalized == "week":
        return "week"
    if normalized == "month":
        return "month"
    raise ValueError("period must be one of: 'week', 'month'.")


def _period_start(value: date, period: OutputPeriod) -> date:
    if period == "week":
        return value - timedelta(days=value.weekday())
    return value.replace(day=1)


def _period_end(value: date, period: OutputPeriod) -> date:
    if period == "week":
        return _period_start(value, "week") + timedelta(days=6)
    return _period_start(value, "month") + relativedelta(months=1) - timedelta(days=1)


def _add_interval(value: date, interval: OutputInterval) -> date:
    if interval == "days":
        return value + timedelta(days=1)
    if interval == "weeks":
        return value + timedelta(weeks=1)
    return value + relativedelta(months=1)


def _warn_if_truncated(
    original: date,
    truncated: date,
    period: OutputPeriod,
    argument_name: str,
) -> None:
    if original != truncated:
        warn(
            f"{argument_name} was truncated to the start of the {period}: "
            f"{original.isoformat()} -> {truncated.isoformat()}",
            stacklevel=3,
        )
