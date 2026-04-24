from __future__ import annotations

from itertools import combinations
import math

import pandas as pd

from .constants import DEFAULT_ALPHA, DEFAULT_POWER
from .ratio import (
    _build_ratio_valid_mask,
    _compute_agg_ratio_diff_standard_error,
    _compute_agg_ratio_group_stats,
)
from .stats import (
    _both_present,
    _compute_mde_abs,
    _compute_mde_from_standard_error,
    _compute_normal_p_value,
    _compute_studentized_statistic,
    _compute_ttest_stat_and_p_value,
    _get_numeric_metric_series,
    _safe_mean,
    _safe_relative,
)


def _build_comparisons(
    group_names: list[str],
    control: str,
    test_vs_test: bool = True,
) -> list[tuple[str, str]]:
    test_groups = sorted(
        (group_name for group_name in group_names if group_name != control),
        key=lambda value: str(value),
    )
    if not test_groups:
        raise ValueError("At least one non-control group is required.")
    if len(group_names) == 2:
        return [(test_groups[0], control)]

    comparisons = [(test_group, control) for test_group in test_groups]
    if test_vs_test:
        comparisons.extend(combinations(test_groups, 2))
    return comparisons


def _build_metric_definitions(
    metric_columns: list[str],
    ratio_specs: list[dict[str, str]],
) -> list[dict[str, object]]:
    metric_definitions: list[dict[str, object]] = [
        {"kind": "mean", "metric_key": metric_name, "column": metric_name}
        for metric_name in metric_columns
    ]
    metric_definitions.extend(
        {
            "kind": "ratio",
            "metric_key": ratio_spec["name"],
            "ratio_spec": ratio_spec,
        }
        for ratio_spec in ratio_specs
    )
    return metric_definitions


def _build_metric_row(
    df: pd.DataFrame,
    group_column: str,
    baseline_group: str,
    test_group: str,
    metric_definition: dict[str, object],
    mde_alpha: float,
    mde_power: float,
) -> dict[str, object]:
    if metric_definition["kind"] == "mean":
        metric_name = str(metric_definition["metric_key"])
        metric_values = _get_numeric_metric_series(df, str(metric_definition["column"]))
        baseline_values = metric_values[df[group_column] == baseline_group].dropna()
        test_values = metric_values[df[group_column] == test_group].dropna()
        return _build_mean_metric_row(
            metric_name=metric_name,
            metric_key=metric_name,
            baseline_values=baseline_values,
            test_values=test_values,
            mde_alpha=mde_alpha,
            mde_power=mde_power,
        )

    return _build_ratio_metric_row(
        df=df,
        group_column=group_column,
        baseline_group=baseline_group,
        test_group=test_group,
        metric_key=str(metric_definition["metric_key"]),
        ratio_spec=dict(metric_definition["ratio_spec"]),
        mde_alpha=mde_alpha,
        mde_power=mde_power,
    )


def _build_mean_metric_row(
    metric_name: str,
    metric_key: str,
    baseline_values: pd.Series,
    test_values: pd.Series,
    mde_alpha: float,
    mde_power: float,
) -> dict[str, object]:
    baseline_mean = _safe_mean(baseline_values)
    test_mean = _safe_mean(test_values)
    delta_abs = test_mean - baseline_mean if _both_present(test_mean, baseline_mean) else math.nan
    t_stat, p_value = _compute_ttest_stat_and_p_value(baseline_values, test_values)

    row = {
        "metric_name": metric_name,
        "n0": int(baseline_values.shape[0]),
        "n1": int(test_values.shape[0]),
        "metric_control": baseline_mean,
        "metric_test": test_mean,
        "delta_abs": delta_abs,
        "delta_relative": _safe_relative(delta_abs, baseline_mean),
        "mde_abs": _compute_mde_abs(
            baseline_values,
            test_values,
            alpha=mde_alpha,
            power=mde_power,
        ),
        "mde_relative": math.nan,
        "p-value": p_value,
        "bootstrap_adj_p": math.nan,
        "_metric_key": metric_key,
        "_test_stat": t_stat,
    }
    row["mde_relative"] = _safe_relative(row["mde_abs"], baseline_mean)
    return row


def _build_ratio_metric_row(
    df: pd.DataFrame,
    group_column: str,
    baseline_group: str,
    test_group: str,
    metric_key: str,
    ratio_spec: dict[str, str],
    mde_alpha: float,
    mde_power: float,
) -> dict[str, object]:
    metric_name = metric_key
    numerator = _get_numeric_metric_series(df, ratio_spec["numerator"])
    denominator = _get_numeric_metric_series(df, ratio_spec["denominator"])

    valid_mask = _build_ratio_valid_mask(
        numerator=numerator,
        denominator=denominator,
        level=ratio_spec["level"],
    )
    baseline_mask = (df[group_column] == baseline_group) & valid_mask
    test_mask = (df[group_column] == test_group) & valid_mask

    if ratio_spec["level"] == "user":
        baseline_values = (numerator[baseline_mask] / denominator[baseline_mask]).dropna()
        test_values = (numerator[test_mask] / denominator[test_mask]).dropna()
        return _build_mean_metric_row(
            metric_name=metric_name,
            metric_key=metric_key,
            baseline_values=baseline_values,
            test_values=test_values,
            mde_alpha=mde_alpha,
            mde_power=mde_power,
        )

    baseline_frame = pd.DataFrame(
        {"numerator": numerator[baseline_mask], "denominator": denominator[baseline_mask]}
    )
    test_frame = pd.DataFrame(
        {"numerator": numerator[test_mask], "denominator": denominator[test_mask]}
    )
    baseline_stats = _compute_agg_ratio_group_stats(baseline_frame)
    test_stats = _compute_agg_ratio_group_stats(test_frame)

    delta_abs = math.nan
    if _both_present(test_stats["ratio"], baseline_stats["ratio"]):
        delta_abs = test_stats["ratio"] - baseline_stats["ratio"]

    se_diff = _compute_agg_ratio_diff_standard_error(
        baseline_frame=baseline_frame,
        baseline_ratio=baseline_stats["ratio"],
        test_frame=test_frame,
        test_ratio=test_stats["ratio"],
    )
    p_value = _compute_normal_p_value(delta_abs=delta_abs, standard_error=se_diff)
    mde_abs = _compute_mde_from_standard_error(
        standard_error=se_diff,
        alpha=mde_alpha,
        power=mde_power,
    )

    return {
        "metric_name": metric_name,
        "n0": int(baseline_stats["n"]),
        "n1": int(test_stats["n"]),
        "metric_control": baseline_stats["ratio"],
        "metric_test": test_stats["ratio"],
        "delta_abs": delta_abs,
        "delta_relative": _safe_relative(delta_abs, baseline_stats["ratio"]),
        "mde_abs": mde_abs,
        "mde_relative": _safe_relative(mde_abs, baseline_stats["ratio"]),
        "p-value": p_value,
        "bootstrap_adj_p": math.nan,
        "_metric_key": metric_key,
        "_test_stat": _compute_studentized_statistic(delta_abs, se_diff),
    }
