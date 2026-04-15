from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from analytics_toolkit.ab_utils import compute_test_metrics
from analytics_toolkit.ab_utils.metrics import (
    _build_comparisons,
    _build_metric_definitions,
    _build_ratio_valid_mask,
    _compute_agg_ratio_diff_standard_error,
    _compute_agg_ratio_group_stats,
    _compute_studentized_statistic,
    _compute_ttest_stat_and_p_value,
    _get_numeric_metric_series,
    _normalize_ratio_metrics,
)


def _build_sample_metrics_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id": list(range(1, 13)),
            "group_name": [
                "control",
                "control",
                "control",
                "control",
                "test_a",
                "test_a",
                "test_a",
                "test_a",
                "test_b",
                "test_b",
                "test_b",
                "test_b",
            ],
            "orders": [10, 12, 9, np.nan, 13, 15, 11, 14, 8, 10, 9, 11],
            "gmv": [100.0, 120.0, 95.0, 110.0, 130.0, 145.0, 118.0, 140.0, 92.0, 105.0, 99.0, 108.0],
            "clicks": [5, 3, 4, 2, 7, 5, 6, 8, 4, 5, 3, 4],
            "impressions": [10, 8, 0, 4, 14, 10, 12, 16, 8, 10, 6, 8],
        }
    )


def _legacy_metric_test_statistic(
    df: pd.DataFrame,
    group_column: str,
    baseline_group: str,
    test_group: str,
    metric_definition: dict[str, object],
) -> float:
    if metric_definition["kind"] == "mean":
        metric_values = _get_numeric_metric_series(df, str(metric_definition["column"]))
        baseline_values = metric_values[df[group_column] == baseline_group].dropna()
        test_values = metric_values[df[group_column] == test_group].dropna()
        statistic, _ = _compute_ttest_stat_and_p_value(baseline_values, test_values)
        return statistic

    ratio_spec = dict(metric_definition["ratio_spec"])
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
        statistic, _ = _compute_ttest_stat_and_p_value(baseline_values, test_values)
        return statistic

    baseline_frame = pd.DataFrame(
        {"numerator": numerator[baseline_mask], "denominator": denominator[baseline_mask]}
    )
    test_frame = pd.DataFrame(
        {"numerator": numerator[test_mask], "denominator": denominator[test_mask]}
    )
    baseline_stats = _compute_agg_ratio_group_stats(baseline_frame)
    test_stats = _compute_agg_ratio_group_stats(test_frame)
    if math.isnan(test_stats["ratio"]) or math.isnan(baseline_stats["ratio"]):
        return math.nan

    delta_abs = test_stats["ratio"] - baseline_stats["ratio"]
    se_diff = _compute_agg_ratio_diff_standard_error(
        baseline_frame=baseline_frame,
        baseline_ratio=baseline_stats["ratio"],
        test_frame=test_frame,
        test_ratio=test_stats["ratio"],
    )
    return _compute_studentized_statistic(delta_abs, se_diff)


def _legacy_bootstrap_adjustment(
    df: pd.DataFrame,
    *,
    group: str,
    control: str,
    user_id: str,
    ratio_metrics: list[dict[str, object]] | None,
    test_vs_test: bool,
    resamples: int,
) -> pd.DataFrame:
    metric_columns = [column for column in df.columns if column not in {group, user_id}]
    ratio_specs = _normalize_ratio_metrics(df, ratio_metrics, reserved_columns={group, user_id})
    metric_definitions = _build_metric_definitions(metric_columns, ratio_specs)
    group_names = df[group].drop_duplicates().tolist()
    comparisons = _build_comparisons(group_names, control, test_vs_test=test_vs_test)
    include_groups = len(group_names) > 2

    family_max_statistics: dict[str, list[float]] = {
        str(metric_definition["metric_key"]): []
        for metric_definition in metric_definitions
    }
    rng = np.random.default_rng(0)

    for _ in range(resamples):
        sample_indices = rng.integers(0, len(df), size=len(df))
        bootstrap_df = df.iloc[sample_indices].reset_index(drop=True).copy()
        for metric_definition in metric_definitions:
            metric_key = str(metric_definition["metric_key"])
            comparison_statistics: list[float] = []
            for test_group, baseline_group in comparisons:
                statistic = _legacy_metric_test_statistic(
                    bootstrap_df,
                    group_column=group,
                    baseline_group=baseline_group,
                    test_group=test_group,
                    metric_definition=metric_definition,
                )
                if not math.isnan(statistic):
                    comparison_statistics.append(abs(statistic))
            family_max_statistics[metric_key].append(
                max(comparison_statistics) if comparison_statistics else math.nan
            )

    rows: list[dict[str, object]] = []
    for test_group, baseline_group in comparisons:
        for metric_definition in metric_definitions:
            metric_key = str(metric_definition["metric_key"])
            observed_stat = _legacy_metric_test_statistic(
                df,
                group_column=group,
                baseline_group=baseline_group,
                test_group=test_group,
                metric_definition=metric_definition,
            )
            if math.isnan(observed_stat):
                adjusted_p = math.nan
            else:
                bootstrap_stats = [
                    value for value in family_max_statistics[metric_key] if not math.isnan(value)
                ]
                adjusted_p = (
                    sum(value >= abs(observed_stat) for value in bootstrap_stats) / len(bootstrap_stats)
                    if bootstrap_stats
                    else math.nan
                )

            row = {"metric_name": metric_key, "bootstrap_adj_p": adjusted_p}
            if include_groups:
                row["group_1"] = test_group
                row["group_2"] = baseline_group
            rows.append(row)

    columns = ["metric_name", "bootstrap_adj_p"]
    if include_groups:
        columns = ["group_1", "group_2", *columns]
    return pd.DataFrame(rows, columns=columns)


def test_compute_test_metrics_matches_legacy_bootstrap_adjustment_single_thread() -> None:
    df = _build_sample_metrics_df()
    ratio_metrics = [
        {"name": "ctr_user", "numerator": "clicks", "denominator": "impressions"},
        {"name": "ctr_agg", "numerator": "clicks", "denominator": "impressions", "level": "agg"},
    ]

    result = compute_test_metrics(
        df,
        ratio_metrics=ratio_metrics,
        multiple_comparisons_adjustment=True,
        multiple_comparisons_adjustment_resamples=40,
        bootstrap_random_state=0,
        bootstrap_n_jobs=1,
    )
    legacy = _legacy_bootstrap_adjustment(
        df,
        group="group_name",
        control="control",
        user_id="user_id",
        ratio_metrics=ratio_metrics,
        test_vs_test=True,
        resamples=40,
    )

    pd.testing.assert_series_equal(result["metric_name"], legacy["metric_name"])
    pd.testing.assert_series_equal(result["group_1"], legacy["group_1"])
    pd.testing.assert_series_equal(result["group_2"], legacy["group_2"])
    np.testing.assert_allclose(result["bootstrap_adj_p"], legacy["bootstrap_adj_p"], equal_nan=True)


def test_compute_test_metrics_adds_metric_control_and_metric_test_columns() -> None:
    df = _build_sample_metrics_df()

    result = compute_test_metrics(df, test_vs_test=False)

    assert result.columns.tolist()[:9] == [
        "metric_type",
        "group_1",
        "group_2",
        "metric_name",
        "n0",
        "n1",
        "metric_control",
        "metric_test",
        "delta_abs",
    ]

    orders_row = result[
        (result["group_1"] == "test_a")
        & (result["group_2"] == "control")
        & (result["metric_name"] == "orders")
    ].iloc[0]
    assert orders_row["metric_type"] == "mean"
    assert orders_row["metric_control"] == pytest.approx((10 + 12 + 9) / 3)
    assert orders_row["metric_test"] == pytest.approx((13 + 15 + 11 + 14) / 4)


def test_compute_test_metrics_uses_raw_relative_fields() -> None:
    df = _build_sample_metrics_df()

    result = compute_test_metrics(df, test_vs_test=False)

    assert "delta_relative" in result.columns
    assert "mde_relative" in result.columns
    assert "uplift" not in result.columns
    assert "mde_percentage" not in result.columns

    orders_row = result[
        (result["group_1"] == "test_a")
        & (result["group_2"] == "control")
        & (result["metric_name"] == "orders")
    ].iloc[0]
    expected_control = (10 + 12 + 9) / 3
    expected_test = (13 + 15 + 11 + 14) / 4
    expected_delta_abs = expected_test - expected_control
    assert orders_row["delta_relative"] == pytest.approx(expected_delta_abs / expected_control)


def test_ratio_metrics_default_to_agg_level() -> None:
    df = _build_sample_metrics_df()

    result = compute_test_metrics(
        df,
        ratio_metrics=[{"name": "ctr", "numerator": "clicks", "denominator": "impressions"}],
        test_vs_test=False,
    )

    ratio_row = result[
        (result["group_1"] == "test_a")
        & (result["group_2"] == "control")
        & (result["metric_name"] == "ctr")
    ].iloc[0]
    assert ratio_row["metric_type"] == "ratio"
    assert ratio_row["metric_control"] == pytest.approx((5 + 3 + 4 + 2) / (10 + 8 + 0 + 4))
    assert ratio_row["metric_test"] == pytest.approx((7 + 5 + 6 + 8) / (14 + 10 + 12 + 16))


def test_compute_test_metrics_parallel_bootstrap_is_reproducible() -> None:
    df = _build_sample_metrics_df()

    first = compute_test_metrics(
        df,
        multiple_comparisons_adjustment=True,
        multiple_comparisons_adjustment_resamples=30,
        bootstrap_random_state=17,
        bootstrap_n_jobs=2,
    )
    second = compute_test_metrics(
        df,
        multiple_comparisons_adjustment=True,
        multiple_comparisons_adjustment_resamples=30,
        bootstrap_random_state=17,
        bootstrap_n_jobs=2,
    )

    pd.testing.assert_frame_equal(first, second)


def test_compute_test_metrics_accepts_bootstrap_progress() -> None:
    df = _build_sample_metrics_df()

    result = compute_test_metrics(
        df,
        multiple_comparisons_adjustment=True,
        multiple_comparisons_adjustment_resamples=5,
        bootstrap_random_state=0,
        bootstrap_progress=True,
    )

    assert "bootstrap_adj_p" in result.columns


@pytest.mark.parametrize(
    ("kwargs", "error_type", "message"),
    [
        ({"bootstrap_random_state": True}, TypeError, "bootstrap_random_state must be an integer or None"),
        ({"bootstrap_n_jobs": 0}, ValueError, "bootstrap_n_jobs must be positive"),
        ({"bootstrap_n_jobs": True}, TypeError, "bootstrap_n_jobs must be an integer"),
        ({"bootstrap_progress": 1}, TypeError, "bootstrap_progress must be a boolean"),
    ],
)
def test_compute_test_metrics_validates_bootstrap_parameters(
    kwargs: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    df = _build_sample_metrics_df()

    with pytest.raises(error_type, match=message):
        compute_test_metrics(df, **kwargs)
