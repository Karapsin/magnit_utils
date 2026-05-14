from __future__ import annotations

from itertools import combinations
import math

import pandas as pd

from .constants import DEFAULT_ALPHA, DEFAULT_POWER
from .outliers import (
    _apply_outliers_to_agg_ratio_components,
    _apply_outliers_to_values,
    _count_outliers_by_group,
    _get_outlier_cutoff,
)
from .ratio import (
    _build_ratio_valid_mask,
    _compute_agg_ratio_group_stats,
    _compute_agg_ratio_variance,
)
from .stats import (
    _both_present,
    _compute_group_diff_standard_error,
    _compute_mde_abs,
    _compute_mde_from_standard_error,
    _compute_normal_p_value,
    _compute_sample_variance,
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
    outlier_context: dict[str, object] | None = None,
) -> dict[str, object]:
    if outlier_context is None:
        outlier_context = metric_definition.get("_outlier_context")
    if metric_definition["kind"] == "mean":
        metric_name = str(metric_definition["metric_key"])
        metric_values = _get_numeric_metric_series(df, str(metric_definition["column"]))
        metric_values, outlier_mask = _apply_outliers_to_values(
            metric_values,
            outlier_context,
        )
        outliers_n_control, outliers_n_test = _count_outliers_by_group(
            outlier_mask=outlier_mask,
            group_values=df[group_column],
            baseline_group=baseline_group,
            test_group=test_group,
        )
        baseline_values = metric_values[df[group_column] == baseline_group].dropna()
        test_values = metric_values[df[group_column] == test_group].dropna()
        return _build_mean_metric_row(
            metric_name=metric_name,
            metric_key=metric_name,
            baseline_values=baseline_values,
            test_values=test_values,
            mde_alpha=mde_alpha,
            mde_power=mde_power,
            outliers_cutoff=_get_outlier_cutoff(outlier_context),
            outliers_n_control=outliers_n_control,
            outliers_n_test=outliers_n_test,
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
        outlier_context=outlier_context,
    )


def _build_mean_metric_row(
    metric_name: str,
    metric_key: str,
    baseline_values: pd.Series,
    test_values: pd.Series,
    mde_alpha: float,
    mde_power: float,
    outliers_cutoff: float = math.nan,
    outliers_n_control: int = 0,
    outliers_n_test: int = 0,
) -> dict[str, object]:
    baseline_mean = _safe_mean(baseline_values)
    test_mean = _safe_mean(test_values)
    delta_abs = test_mean - baseline_mean if _both_present(test_mean, baseline_mean) else math.nan
    baseline_n = int(baseline_values.shape[0])
    test_n = int(test_values.shape[0])
    baseline_variance = _compute_sample_variance(baseline_values)
    test_variance = _compute_sample_variance(test_values)
    standard_error = _compute_group_diff_standard_error(
        baseline_variance=baseline_variance,
        baseline_n=baseline_n,
        test_variance=test_variance,
        test_n=test_n,
    )
    t_stat, p_value = _compute_ttest_stat_and_p_value(baseline_values, test_values)

    row = {
        "metric_name": metric_name,
        "n0": baseline_n,
        "n1": test_n,
        "outliers_cutoff": outliers_cutoff,
        "outliers_n_control": outliers_n_control,
        "outliers_n_test": outliers_n_test,
        "metric_control": baseline_mean,
        "metric_test": test_mean,
        "variance_control": baseline_variance,
        "variance_test": test_variance,
        "delta_abs": delta_abs,
        "delta_relative": _safe_relative(delta_abs, baseline_mean),
        "mde_abs": _compute_mde_abs(
            baseline_values,
            test_values,
            alpha=mde_alpha,
            power=mde_power,
        ),
        "mde_relative": math.nan,
        "s.e.": standard_error,
        "p-value": p_value,
        "s.e. bootstrap": math.nan,
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
    outlier_context: dict[str, object] | None = None,
) -> dict[str, object]:
    metric_name = metric_key
    numerator = _get_numeric_metric_series(df, ratio_spec["numerator"])
    denominator = _get_numeric_metric_series(df, ratio_spec["denominator"])

    valid_mask = _build_ratio_valid_mask(
        numerator=numerator,
        denominator=denominator,
        level=ratio_spec["level"],
    )

    if ratio_spec["level"] == "user":
        ratio_values = pd.Series(math.nan, index=df.index, dtype=float)
        ratio_values.loc[valid_mask] = numerator.loc[valid_mask] / denominator.loc[valid_mask]
        ratio_values, outlier_mask = _apply_outliers_to_values(
            ratio_values,
            outlier_context,
        )
        outliers_n_control, outliers_n_test = _count_outliers_by_group(
            outlier_mask=outlier_mask,
            group_values=df[group_column],
            baseline_group=baseline_group,
            test_group=test_group,
        )
        baseline_values = ratio_values[df[group_column] == baseline_group].dropna()
        test_values = ratio_values[df[group_column] == test_group].dropna()
        return _build_mean_metric_row(
            metric_name=metric_name,
            metric_key=metric_key,
            baseline_values=baseline_values,
            test_values=test_values,
            mde_alpha=mde_alpha,
            mde_power=mde_power,
            outliers_cutoff=_get_outlier_cutoff(outlier_context),
            outliers_n_control=outliers_n_control,
            outliers_n_test=outliers_n_test,
        )

    numerator, denominator, outlier_mask = _apply_outliers_to_agg_ratio_components(
        numerator=numerator,
        denominator=denominator,
        outlier_context=outlier_context,
    )
    valid_mask = _build_ratio_valid_mask(
        numerator=numerator,
        denominator=denominator,
        level=ratio_spec["level"],
    )
    baseline_mask = (df[group_column] == baseline_group) & valid_mask
    test_mask = (df[group_column] == test_group) & valid_mask
    baseline_frame = pd.DataFrame(
        {"numerator": numerator[baseline_mask], "denominator": denominator[baseline_mask]}
    )
    test_frame = pd.DataFrame(
        {"numerator": numerator[test_mask], "denominator": denominator[test_mask]}
    )
    outliers_n_control, outliers_n_test = _count_outliers_by_group(
        outlier_mask=outlier_mask,
        group_values=df[group_column],
        baseline_group=baseline_group,
        test_group=test_group,
    )
    baseline_stats = _compute_agg_ratio_group_stats(baseline_frame)
    test_stats = _compute_agg_ratio_group_stats(test_frame)

    delta_abs = math.nan
    if _both_present(test_stats["ratio"], baseline_stats["ratio"]):
        delta_abs = test_stats["ratio"] - baseline_stats["ratio"]

    baseline_variance = _compute_agg_ratio_variance(baseline_frame, baseline_stats["ratio"])
    test_variance = _compute_agg_ratio_variance(test_frame, test_stats["ratio"])
    se_diff = math.nan
    if not math.isnan(baseline_variance) and not math.isnan(test_variance):
        se_diff = math.sqrt(baseline_variance + test_variance)
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
        "outliers_cutoff": _get_outlier_cutoff(outlier_context),
        "outliers_n_control": outliers_n_control,
        "outliers_n_test": outliers_n_test,
        "metric_control": baseline_stats["ratio"],
        "metric_test": test_stats["ratio"],
        "variance_control": baseline_variance,
        "variance_test": test_variance,
        "delta_abs": delta_abs,
        "delta_relative": _safe_relative(delta_abs, baseline_stats["ratio"]),
        "mde_abs": mde_abs,
        "mde_relative": _safe_relative(mde_abs, baseline_stats["ratio"]),
        "s.e.": se_diff,
        "p-value": p_value,
        "s.e. bootstrap": math.nan,
        "bootstrap_adj_p": math.nan,
        "_metric_key": metric_key,
        "_test_stat": _compute_studentized_statistic(delta_abs, se_diff),
    }
