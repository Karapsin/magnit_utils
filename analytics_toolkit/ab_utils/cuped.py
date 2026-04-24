from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd

from .ratio import _build_agg_ratio_linearized_values, _build_ratio_valid_mask
from .stats import _compute_ttest_stat_and_p_value, _get_numeric_metric_series


def _compute_cuped_p_value(
    df: pd.DataFrame,
    pre_exp_metrics_df: pd.DataFrame,
    group_column: str,
    user_id_column: str,
    baseline_group: str,
    test_group: str,
    metric_definition: dict[str, object],
) -> float:
    cuped_frame, reason = _build_cuped_frame(
        df=df,
        pre_exp_metrics_df=pre_exp_metrics_df,
        user_id_column=user_id_column,
        group_column=group_column,
        baseline_group=baseline_group,
        test_group=test_group,
        metric_definition=metric_definition,
    )
    metric_name = str(metric_definition["metric_key"])
    if reason is not None:
        warnings.warn(
            (
                f"Could not compute CUPED p-value for metric '{metric_name}' "
                f"({test_group!r} vs {baseline_group!r}): {reason}."
            ),
            stacklevel=2,
        )
        return math.nan

    assert cuped_frame is not None
    p_value, reason = _compute_cuped_p_value_from_frame(
        cuped_frame=cuped_frame,
        group_column=group_column,
        baseline_group=baseline_group,
        test_group=test_group,
    )
    if reason is not None:
        warnings.warn(
            (
                f"Could not compute CUPED p-value for metric '{metric_name}' "
                f"({test_group!r} vs {baseline_group!r}): {reason}."
            ),
            stacklevel=2,
        )
        return math.nan
    return p_value


def _build_cuped_frame(
    df: pd.DataFrame,
    pre_exp_metrics_df: pd.DataFrame,
    user_id_column: str,
    group_column: str,
    baseline_group: str,
    test_group: str,
    metric_definition: dict[str, object],
) -> tuple[pd.DataFrame | None, str | None]:
    comparison_mask = df[group_column].isin([baseline_group, test_group])
    comparison_df = df.loc[comparison_mask, [user_id_column, group_column]].copy()

    exp_values, exp_error = _build_metric_values_by_user(
        df=df.loc[comparison_mask].copy(),
        user_id_column=user_id_column,
        metric_definition=metric_definition,
        value_column="metric_exp",
    )
    if exp_error is not None:
        return None, f"experiment metric values are unavailable: {exp_error}"

    pre_values, pre_error = _build_metric_values_by_user(
        df=pre_exp_metrics_df,
        user_id_column=user_id_column,
        metric_definition=metric_definition,
        value_column="metric_pre",
    )
    if pre_error is not None:
        return None, f"pre-experiment metric values are unavailable: {pre_error}"

    cuped_frame = comparison_df.merge(exp_values, on=user_id_column, how="left").merge(
        pre_values,
        on=user_id_column,
        how="left",
    )
    cuped_frame = cuped_frame.dropna(subset=["metric_exp", "metric_pre"]).reset_index(drop=True)
    if cuped_frame.empty:
        return None, "no overlapping non-missing experiment/pre-experiment observations"

    return cuped_frame, None


def _build_metric_values_by_user(
    df: pd.DataFrame,
    user_id_column: str,
    metric_definition: dict[str, object],
    value_column: str,
) -> tuple[pd.DataFrame, str | None]:
    if metric_definition["kind"] == "mean":
        metric_name = str(metric_definition["column"])
        if metric_name not in df.columns:
            return pd.DataFrame(columns=[user_id_column, value_column]), f"missing column '{metric_name}'"
        values = _get_numeric_metric_series(df, metric_name)
        return pd.DataFrame({user_id_column: df[user_id_column].to_numpy(), value_column: values.to_numpy()}), None

    ratio_spec = dict(metric_definition["ratio_spec"])
    numerator_column = ratio_spec["numerator"]
    denominator_column = ratio_spec["denominator"]
    missing_columns = [
        column
        for column in (numerator_column, denominator_column)
        if column not in df.columns
    ]
    if missing_columns:
        missing = ", ".join(f"'{column}'" for column in missing_columns)
        return pd.DataFrame(columns=[user_id_column, value_column]), f"missing columns: {missing}"

    numerator = _get_numeric_metric_series(df, numerator_column)
    denominator = _get_numeric_metric_series(df, denominator_column)
    if ratio_spec["level"] == "user":
        valid_mask = _build_ratio_valid_mask(
            numerator=numerator,
            denominator=denominator,
            level=ratio_spec["level"],
        )
        values = pd.Series(np.nan, index=df.index, dtype=float)
        values.loc[valid_mask] = numerator.loc[valid_mask] / denominator.loc[valid_mask]
        return pd.DataFrame({user_id_column: df[user_id_column].to_numpy(), value_column: values.to_numpy()}), None

    values, error = _build_agg_ratio_linearized_values(
        numerator=numerator,
        denominator=denominator,
    )
    if error is not None:
        return pd.DataFrame(columns=[user_id_column, value_column]), error
    return pd.DataFrame({user_id_column: df[user_id_column].to_numpy(), value_column: values.to_numpy()}), None


def _compute_cuped_p_value_from_frame(
    cuped_frame: pd.DataFrame,
    group_column: str,
    baseline_group: str,
    test_group: str,
) -> tuple[float, str | None]:
    metric_exp = cuped_frame["metric_exp"].astype(float)
    metric_pre = cuped_frame["metric_pre"].astype(float)
    pre_variance = float(metric_pre.var(ddof=1))
    if math.isnan(pre_variance) or pre_variance <= 0:
        return math.nan, "pre-experiment covariate variance is not positive"

    theta = float(metric_exp.cov(metric_pre) / pre_variance)
    adjusted = metric_exp - theta * (metric_pre - float(metric_pre.mean()))
    baseline_values = adjusted[cuped_frame[group_column] == baseline_group]
    test_values = adjusted[cuped_frame[group_column] == test_group]
    _, p_value = _compute_ttest_stat_and_p_value(
        pd.Series(baseline_values),
        pd.Series(test_values),
    )
    if math.isnan(p_value):
        return math.nan, "not enough overlapping observations to run the CUPED t-test"
    return p_value, None
