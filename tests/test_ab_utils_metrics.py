from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from analytics_toolkit.ab_utils import compute_test_metrics
from analytics_toolkit.ab_utils.metrics import (
    _apply_outliers_to_agg_ratio_components,
    _apply_outliers_to_values,
    _build_comparisons,
    _build_metric_definitions,
    _build_outlier_contexts,
    _build_ratio_valid_mask,
    _compute_agg_ratio_diff_standard_error,
    _compute_agg_ratio_group_stats,
    _compute_agg_ratio_variance,
    _compute_cuped_statistics_from_frame,
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
    outlier_context: dict[str, object] | None = None,
) -> float:
    if metric_definition["kind"] == "mean":
        metric_values = _get_numeric_metric_series(df, str(metric_definition["column"]))
        metric_values, _ = _apply_outliers_to_values(metric_values, outlier_context)
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

    if ratio_spec["level"] == "user":
        ratio_values = pd.Series(np.nan, index=df.index, dtype=float)
        ratio_values.loc[valid_mask] = numerator.loc[valid_mask] / denominator.loc[valid_mask]
        ratio_values, _ = _apply_outliers_to_values(ratio_values, outlier_context)
        baseline_values = ratio_values[df[group_column] == baseline_group].dropna()
        test_values = ratio_values[df[group_column] == test_group].dropna()
        statistic, _ = _compute_ttest_stat_and_p_value(baseline_values, test_values)
        return statistic

    numerator, denominator, _ = _apply_outliers_to_agg_ratio_components(
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
    outliers_quantile: float = 0.999,
    outliers_policy: str = "truncate",
) -> pd.DataFrame:
    metric_columns = [column for column in df.columns if column not in {group, user_id}]
    ratio_specs = _normalize_ratio_metrics(df, ratio_metrics, reserved_columns={group, user_id})
    metric_definitions = _build_metric_definitions(metric_columns, ratio_specs)
    outlier_contexts = _build_outlier_contexts(
        df=df,
        metric_definitions=metric_definitions,
        outliers_quantile=outliers_quantile,
        outliers_policy=outliers_policy,
    )
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
                    outlier_context=outlier_contexts[str(metric_definition["metric_key"])],
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
                outlier_context=outlier_contexts[metric_key],
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

    assert result.columns.tolist()[:14] == [
        "metric_type",
        "group_1",
        "group_2",
        "metric_name",
        "n0",
        "n1",
        "outliers_cutoff",
        "outliers_n_control",
        "outliers_n_test",
        "metric_control",
        "metric_test",
        "variance_control",
        "variance_test",
        "delta_abs",
    ]
    assert result.columns[result.columns.get_loc("mde_relative") + 1] == "s.e."
    assert result.columns[result.columns.get_loc("s.e.") + 1] == "p-value"

    orders_row = result[
        (result["group_1"] == "test_a")
        & (result["group_2"] == "control")
        & (result["metric_name"] == "orders")
    ].iloc[0]
    orders_cutoff = float(df["orders"].quantile(0.999))
    assert orders_row["metric_type"] == "mean"
    assert orders_row["metric_control"] == pytest.approx((10 + 12 + 9) / 3)
    assert orders_row["metric_test"] == pytest.approx((13 + orders_cutoff + 11 + 14) / 4)
    assert orders_row["outliers_cutoff"] == pytest.approx(orders_cutoff)
    assert orders_row["outliers_n_control"] == 0
    assert orders_row["outliers_n_test"] == 1
    control_values = pd.Series([10, 12, 9], dtype=float)
    test_values = pd.Series([13, orders_cutoff, 11, 14], dtype=float)
    expected_control_variance = control_values.var(ddof=1)
    expected_test_variance = test_values.var(ddof=1)
    assert orders_row["variance_control"] == pytest.approx(expected_control_variance)
    assert orders_row["variance_test"] == pytest.approx(expected_test_variance)
    assert orders_row["s.e."] == pytest.approx(
        math.sqrt((expected_control_variance / 3) + (expected_test_variance / 4))
    )


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
    orders_cutoff = float(df["orders"].quantile(0.999))
    expected_control = (10 + 12 + 9) / 3
    expected_test = (13 + orders_cutoff + 11 + 14) / 4
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

    numerator = _get_numeric_metric_series(df, "clicks")
    denominator = _get_numeric_metric_series(df, "impressions")
    valid_mask = _build_ratio_valid_mask(numerator=numerator, denominator=denominator, level="agg")
    baseline_frame = pd.DataFrame(
        {
            "numerator": numerator[(df["group_name"] == "control") & valid_mask],
            "denominator": denominator[(df["group_name"] == "control") & valid_mask],
        }
    )
    test_frame = pd.DataFrame(
        {
            "numerator": numerator[(df["group_name"] == "test_a") & valid_mask],
            "denominator": denominator[(df["group_name"] == "test_a") & valid_mask],
        }
    )
    baseline_stats = _compute_agg_ratio_group_stats(baseline_frame)
    test_stats = _compute_agg_ratio_group_stats(test_frame)
    expected_control_variance = _compute_agg_ratio_variance(
        baseline_frame,
        baseline_stats["ratio"],
    )
    expected_test_variance = _compute_agg_ratio_variance(test_frame, test_stats["ratio"])
    assert ratio_row["variance_control"] == pytest.approx(expected_control_variance)
    assert ratio_row["variance_test"] == pytest.approx(expected_test_variance)
    assert ratio_row["s.e."] == pytest.approx(
        _compute_agg_ratio_diff_standard_error(
            baseline_frame=baseline_frame,
            baseline_ratio=baseline_stats["ratio"],
            test_frame=test_frame,
            test_ratio=test_stats["ratio"],
        )
    )


def test_compute_test_metrics_drop_outliers_updates_counts() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 7)),
            "group_name": ["control", "control", "control", "test", "test", "test"],
            "orders": [1, 2, 100, 3, 4, 200],
        }
    )

    result = compute_test_metrics(
        df,
        control="control",
        test_vs_test=False,
        outliers_quantile=0.8,
        outliers_policy="drop",
    )

    orders_row = result[result["metric_name"] == "orders"].iloc[0]
    assert orders_row["outliers_cutoff"] == pytest.approx(float(df["orders"].quantile(0.8)))
    assert orders_row["outliers_n_control"] == 0
    assert orders_row["outliers_n_test"] == 1
    assert orders_row["n0"] == 3
    assert orders_row["n1"] == 2
    assert orders_row["metric_test"] == pytest.approx((3 + 4) / 2)


def test_compute_test_metrics_uses_global_outlier_cutoff_across_groups() -> None:
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "group_name": ["control", "control", "test", "test"],
            "orders": [1, 2, 100, 200],
        }
    )

    result = compute_test_metrics(
        df,
        control="control",
        test_vs_test=False,
        outliers_quantile=0.75,
    )

    orders_row = result[result["metric_name"] == "orders"].iloc[0]
    cutoff = float(df["orders"].quantile(0.75))
    assert cutoff == pytest.approx(125.0)
    assert orders_row["outliers_cutoff"] == pytest.approx(cutoff)
    assert orders_row["metric_test"] == pytest.approx((100 + cutoff) / 2)


def test_compute_test_metrics_user_ratio_outliers_truncate_and_drop() -> None:
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "group_name": ["control", "control", "test", "test"],
            "clicks": [1, 2, 3, 100],
            "impressions": [10, 10, 10, 10],
        }
    )
    ratio_metrics = [
        {
            "name": "ctr_user",
            "numerator": "clicks",
            "denominator": "impressions",
            "level": "user",
        }
    ]

    truncate_result = compute_test_metrics(
        df,
        control="control",
        ratio_metrics=ratio_metrics,
        test_vs_test=False,
        outliers_quantile=0.75,
        outliers_policy="truncate",
    )
    drop_result = compute_test_metrics(
        df,
        control="control",
        ratio_metrics=ratio_metrics,
        test_vs_test=False,
        outliers_quantile=0.75,
        outliers_policy="drop",
    )

    cutoff = float(pd.Series([0.1, 0.2, 0.3, 10.0]).quantile(0.75))
    truncate_row = truncate_result[truncate_result["metric_name"] == "ctr_user"].iloc[0]
    drop_row = drop_result[drop_result["metric_name"] == "ctr_user"].iloc[0]
    assert truncate_row["outliers_cutoff"] == pytest.approx(cutoff)
    assert truncate_row["outliers_n_test"] == 1
    assert truncate_row["metric_test"] == pytest.approx((0.3 + cutoff) / 2)
    assert truncate_row["n1"] == 2
    assert drop_row["metric_test"] == pytest.approx(0.3)
    assert drop_row["n1"] == 1


def test_compute_test_metrics_agg_ratio_outliers_drop_and_truncate() -> None:
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "group_name": ["control", "control", "test", "test"],
            "clicks": [1, 2, 3, 100],
            "impressions": [10, 10, 10, 10],
        }
    )
    ratio_metrics = [{"name": "ctr", "numerator": "clicks", "denominator": "impressions"}]

    truncate_result = compute_test_metrics(
        df,
        control="control",
        ratio_metrics=ratio_metrics,
        test_vs_test=False,
        outliers_quantile=0.75,
        outliers_policy="truncate",
    )
    drop_result = compute_test_metrics(
        df,
        control="control",
        ratio_metrics=ratio_metrics,
        test_vs_test=False,
        outliers_quantile=0.75,
        outliers_policy="drop",
    )

    cutoff = float(pd.Series([0.1, 0.2, 0.3, 10.0]).quantile(0.75))
    truncate_row = truncate_result[truncate_result["metric_name"] == "ctr"].iloc[0]
    drop_row = drop_result[drop_result["metric_name"] == "ctr"].iloc[0]
    assert truncate_row["outliers_cutoff"] == pytest.approx(cutoff)
    assert truncate_row["outliers_n_test"] == 1
    assert truncate_row["metric_test"] == pytest.approx((3 + cutoff * 10) / 20)
    assert truncate_row["n1"] == 2
    assert drop_row["metric_test"] == pytest.approx(3 / 10)
    assert drop_row["n1"] == 1


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
    assert first.columns[first.columns.get_loc("p-value") + 1] == "s.e. bootstrap"
    assert first.columns[first.columns.get_loc("s.e. bootstrap") + 1] == "bootstrap_adj_p"
    orders_row = first[
        (first["group_1"] == "test_a")
        & (first["group_2"] == "control")
        & (first["metric_name"] == "orders")
    ].iloc[0]
    assert not math.isnan(float(orders_row["s.e. bootstrap"]))


def test_compute_test_metrics_accepts_bootstrap_progress() -> None:
    df = _build_sample_metrics_df()

    result = compute_test_metrics(
        df,
        multiple_comparisons_adjustment=True,
        multiple_comparisons_adjustment_resamples=5,
        bootstrap_random_state=0,
        bootstrap_progress=True,
    )

    assert "s.e. bootstrap" in result.columns
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


@pytest.mark.parametrize(
    ("kwargs", "error_type", "message"),
    [
        ({"outliers_quantile": 0}, ValueError, "outliers_quantile must be strictly between 0 and 1"),
        ({"outliers_quantile": 1}, ValueError, "outliers_quantile must be strictly between 0 and 1"),
        ({"outliers_quantile": True}, TypeError, "outliers_quantile must be numeric"),
        ({"outliers_quantile": "0.9"}, TypeError, "outliers_quantile must be numeric"),
        ({"outliers_policy": "winsorize"}, ValueError, "outliers_policy must be 'truncate' or 'drop'"),
        ({"outliers_policy": None}, TypeError, "outliers_policy must be a string"),
    ],
)
def test_compute_test_metrics_validates_outlier_parameters(
    kwargs: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    df = _build_sample_metrics_df()

    with pytest.raises(error_type, match=message):
        compute_test_metrics(df, **kwargs)


def test_compute_test_metrics_adds_cuped_p_value_for_mean_metrics() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "orders": [10, 11, 9, 12, 14, 15, 13, 16],
        }
    )
    pre_df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "orders": [8, 10, 6, 11, 12, 12, 10, 15],
        }
    )

    result = compute_test_metrics(
        df,
        control="control",
        test_vs_test=False,
        pre_exp_metrics_df=pre_df,
    )

    assert result.columns[result.columns.get_loc("p-value") + 1] == "s.e. CUPED"
    assert result.columns[result.columns.get_loc("s.e. CUPED") + 1] == "p-value CUPED"
    orders_row = result[result["metric_name"] == "orders"].iloc[0]
    assert not math.isnan(float(orders_row["s.e. CUPED"]))
    assert not math.isnan(float(orders_row["p-value CUPED"]))


def test_compute_test_metrics_cuped_uses_transformed_values() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 7)),
            "group_name": ["control", "control", "control", "test", "test", "test"],
            "orders": [1, 2, 3, 4, 5, 100],
        }
    )
    pre_df = pd.DataFrame(
        {
            "user_id": list(range(1, 7)),
            "group_name": ["control", "control", "control", "test", "test", "test"],
            "orders": [10, 1, 8, 3, 6, 200],
        }
    )

    result = compute_test_metrics(
        df,
        control="control",
        test_vs_test=False,
        pre_exp_metrics_df=pre_df,
        outliers_quantile=0.8,
        outliers_policy="truncate",
    )

    cuped_frame = pd.DataFrame(
        {
            "group_name": df["group_name"],
            "metric_exp": [1.0, 2.0, 3.0, 4.0, 5.0, 5.0],
            "metric_pre": [10.0, 1.0, 8.0, 3.0, 6.0, 10.0],
        }
    )
    expected_p_value, expected_standard_error, reason = _compute_cuped_statistics_from_frame(
        cuped_frame=cuped_frame,
        group_column="group_name",
        baseline_group="control",
        test_group="test",
    )
    assert reason is None

    orders_row = result[result["metric_name"] == "orders"].iloc[0]
    assert orders_row["outliers_cutoff"] == pytest.approx(5.0)
    assert orders_row["outliers_n_test"] == 1
    assert orders_row["s.e. CUPED"] == pytest.approx(expected_standard_error)
    assert orders_row["p-value CUPED"] == pytest.approx(expected_p_value)


def test_compute_test_metrics_adds_cuped_p_value_for_ratio_metrics() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "clicks": [5, 6, 4, 5, 8, 9, 7, 8],
            "impressions": [10, 12, 8, 10, 12, 14, 10, 12],
        }
    )
    pre_df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "clicks": [4, 5, 3, 4, 6, 7, 5, 6],
            "impressions": [9, 11, 8, 10, 11, 15, 9, 13],
        }
    )

    result = compute_test_metrics(
        df,
        control="control",
        ratio_metrics=[
            {
                "name": "ctr_user",
                "numerator": "clicks",
                "denominator": "impressions",
                "level": "user",
            }
        ],
        test_vs_test=False,
        pre_exp_metrics_df=pre_df,
    )

    ratio_row = result[result["metric_name"] == "ctr_user"].iloc[0]
    assert ratio_row["metric_type"] == "ratio"
    assert not math.isnan(float(ratio_row["s.e. CUPED"]))
    assert not math.isnan(float(ratio_row["p-value CUPED"]))


def test_compute_test_metrics_warns_and_sets_nan_when_pre_metric_is_missing() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "orders": [10, 11, 9, 12, 14, 15, 13, 16],
        }
    )
    pre_df = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "group_name": ["control"] * 4 + ["test"] * 4,
            "gmv": [100, 110, 90, 120, 140, 150, 130, 160],
        }
    )

    with pytest.warns(UserWarning, match="Could not compute CUPED p-value for metric 'orders'"):
        result = compute_test_metrics(
            df,
            control="control",
            test_vs_test=False,
            pre_exp_metrics_df=pre_df,
        )

    orders_row = result[result["metric_name"] == "orders"].iloc[0]
    assert math.isnan(float(orders_row["s.e. CUPED"]))
    assert math.isnan(float(orders_row["p-value CUPED"]))


def test_compute_test_metrics_validates_pre_experiment_group_assignments() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 5)),
            "group_name": ["control", "control", "test", "test"],
            "orders": [10, 11, 14, 15],
        }
    )
    pre_df = pd.DataFrame(
        {
            "user_id": list(range(1, 5)),
            "group_name": ["control", "test", "test", "test"],
            "orders": [8, 9, 12, 13],
        }
    )

    with pytest.raises(ValueError, match="must match between df and pre_exp_metrics_df"):
        compute_test_metrics(
            df,
            control="control",
            test_vs_test=False,
            pre_exp_metrics_df=pre_df,
        )
