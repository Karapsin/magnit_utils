from __future__ import annotations

import pandas as pd

from .bootstrap import _apply_multiple_comparisons_adjustment
from .constants import DEFAULT_ALPHA, DEFAULT_POWER
from .cuped import _compute_cuped_p_value
from .ratio import _normalize_ratio_metrics
from .rows import _build_comparisons, _build_metric_definitions, _build_metric_row
from .validation import (
    _validate_input_columns,
    _validate_mde_parameters,
    _validate_multiple_comparisons_parameters,
    _validate_pre_experiment_dataframe,
)


def compute_test_metrics(
    df: pd.DataFrame,
    group: str = "group_name",
    control: str = "control",
    user_id: str = "user_id",
    mde_alpha: float = DEFAULT_ALPHA,
    mde_power: float = DEFAULT_POWER,
    ratio_metrics: list[dict[str, object]] | None = None,
    test_vs_test: bool = True,
    multiple_comparisons_adjustment: bool = False,
    multiple_comparisons_adjustment_resamples: int = 2000,
    bootstrap_random_state: int | None = 0,
    bootstrap_n_jobs: int = 1,
    bootstrap_progress: bool = True,
    pre_exp_metrics_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute per-metric experiment comparison statistics.

    Notes:
    - The input must contain exactly one row per user.
    - All columns except `group` and `user_id` are treated as metric columns.
    - Missing metric values are ignored independently for each metric/group pair.
    - `mde_abs` and `mde_relative` use a two-sided normal approximation based
      on the observed sample variances.
    - Ratio metrics can be passed through `ratio_metrics`.
    """

    _validate_input_columns(df, group=group, user_id=user_id)
    _validate_mde_parameters(mde_alpha=mde_alpha, mde_power=mde_power)
    _validate_multiple_comparisons_parameters(
        multiple_comparisons_adjustment=multiple_comparisons_adjustment,
        multiple_comparisons_adjustment_resamples=multiple_comparisons_adjustment_resamples,
        bootstrap_random_state=bootstrap_random_state,
        bootstrap_n_jobs=bootstrap_n_jobs,
        bootstrap_progress=bootstrap_progress,
    )

    if df[user_id].isna().any():
        raise ValueError(f"Column '{user_id}' must not contain missing values.")
    if df[user_id].duplicated().any():
        raise ValueError(f"Column '{user_id}' must contain unique user ids.")
    if df[group].isna().any():
        raise ValueError(f"Column '{group}' must not contain missing values.")

    if pre_exp_metrics_df is not None:
        _validate_pre_experiment_dataframe(
            df=df,
            pre_exp_metrics_df=pre_exp_metrics_df,
            group=group,
            control=control,
            user_id=user_id,
        )

    metric_columns = [column for column in df.columns if column not in {group, user_id}]
    if not metric_columns:
        if not ratio_metrics:
            raise ValueError("The dataframe must contain at least one metric column.")

    group_names = df[group].drop_duplicates().tolist()
    if control not in group_names:
        raise ValueError(f"Control label '{control}' was not found in column '{group}'.")

    include_groups = True
    ratio_specs = _normalize_ratio_metrics(df, ratio_metrics, reserved_columns={group, user_id})
    comparisons = _build_comparisons(group_names, control, test_vs_test=test_vs_test)
    metric_definitions = _build_metric_definitions(metric_columns, ratio_specs)

    rows: list[dict[str, object]] = []
    for test_group, baseline_group in comparisons:
        for metric_definition in metric_definitions:
            row = _build_metric_row(
                df=df,
                group_column=group,
                baseline_group=baseline_group,
                test_group=test_group,
                metric_definition=metric_definition,
                mde_alpha=mde_alpha,
                mde_power=mde_power,
            )
            if pre_exp_metrics_df is not None:
                row["p-value CUPED"] = _compute_cuped_p_value(
                    df=df,
                    pre_exp_metrics_df=pre_exp_metrics_df,
                    group_column=group,
                    user_id_column=user_id,
                    baseline_group=baseline_group,
                    test_group=test_group,
                    metric_definition=metric_definition,
                )
            if include_groups:
                row = {
                    "metric_type": str(metric_definition["kind"]),
                    "group_1": test_group,
                    "group_2": baseline_group,
                    **row,
                }
            else:
                row = {"metric_type": str(metric_definition["kind"]), **row}
            rows.append(row)

    if multiple_comparisons_adjustment:
        _apply_multiple_comparisons_adjustment(
            rows=rows,
            df=df,
            group_column=group,
            metric_definitions=metric_definitions,
            comparisons=comparisons,
            resamples=multiple_comparisons_adjustment_resamples,
            random_state=bootstrap_random_state,
            n_jobs=bootstrap_n_jobs,
            show_progress=bootstrap_progress,
        )

    columns = [
        "metric_name",
        "n0",
        "n1",
        "metric_control",
        "metric_test",
        "delta_abs",
        "delta_relative",
        "mde_abs",
        "mde_relative",
        "p-value",
    ]
    if pre_exp_metrics_df is not None:
        columns.append("p-value CUPED")
    if multiple_comparisons_adjustment:
        columns.append("bootstrap_adj_p")
    if include_groups:
        columns = ["metric_type", "group_1", "group_2", *columns]
    else:
        columns = ["metric_type", *columns]

    for row in rows:
        row.pop("_metric_key", None)
        row.pop("_test_stat", None)

    return pd.DataFrame(rows, columns=columns)
