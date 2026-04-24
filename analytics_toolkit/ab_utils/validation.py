from __future__ import annotations

import pandas as pd


def _validate_input_columns(df: pd.DataFrame, group: str, user_id: str) -> None:
    missing_columns = [column for column in (group, user_id) if column not in df.columns]
    if missing_columns:
        missing = ", ".join(f"'{column}'" for column in missing_columns)
        raise ValueError(f"Missing required columns: {missing}.")


def _validate_mde_parameters(mde_alpha: float, mde_power: float) -> None:
    if not 0 < mde_alpha < 1:
        raise ValueError("mde_alpha must be between 0 and 1.")
    if not 0 < mde_power < 1:
        raise ValueError("mde_power must be between 0 and 1.")


def _validate_pre_experiment_dataframe(
    df: pd.DataFrame,
    pre_exp_metrics_df: pd.DataFrame,
    group: str,
    control: str,
    user_id: str,
) -> None:
    _validate_input_columns(pre_exp_metrics_df, group=group, user_id=user_id)

    if pre_exp_metrics_df[user_id].isna().any():
        raise ValueError(f"Column '{user_id}' in pre_exp_metrics_df must not contain missing values.")
    if pre_exp_metrics_df[user_id].duplicated().any():
        raise ValueError(f"Column '{user_id}' in pre_exp_metrics_df must contain unique user ids.")
    if pre_exp_metrics_df[group].isna().any():
        raise ValueError(f"Column '{group}' in pre_exp_metrics_df must not contain missing values.")
    if control not in pre_exp_metrics_df[group].drop_duplicates().tolist():
        raise ValueError(
            f"Control label '{control}' was not found in column '{group}' of pre_exp_metrics_df."
        )

    overlap = df[[user_id, group]].merge(
        pre_exp_metrics_df[[user_id, group]],
        on=user_id,
        how="inner",
        suffixes=("_exp", "_pre"),
    )
    mismatch = overlap[overlap[f"{group}_exp"] != overlap[f"{group}_pre"]]
    if not mismatch.empty:
        raise ValueError(
            f"Column '{group}' must match between df and pre_exp_metrics_df for overlapping user ids."
        )


def _validate_multiple_comparisons_parameters(
    multiple_comparisons_adjustment: bool,
    multiple_comparisons_adjustment_resamples: int,
    bootstrap_random_state: int | None,
    bootstrap_n_jobs: int,
    bootstrap_progress: bool,
) -> None:
    if bootstrap_random_state is not None:
        if isinstance(bootstrap_random_state, bool) or not isinstance(bootstrap_random_state, int):
            raise TypeError("bootstrap_random_state must be an integer or None.")
    if isinstance(bootstrap_n_jobs, bool) or not isinstance(bootstrap_n_jobs, int):
        raise TypeError("bootstrap_n_jobs must be an integer.")
    if bootstrap_n_jobs <= 0:
        raise ValueError("bootstrap_n_jobs must be positive.")
    if not isinstance(bootstrap_progress, bool):
        raise TypeError("bootstrap_progress must be a boolean.")
    if not multiple_comparisons_adjustment:
        return
    if isinstance(multiple_comparisons_adjustment_resamples, bool) or not isinstance(
        multiple_comparisons_adjustment_resamples, int
    ):
        raise TypeError("multiple_comparisons_adjustment_resamples must be an integer.")
    if multiple_comparisons_adjustment_resamples <= 0:
        raise ValueError("multiple_comparisons_adjustment_resamples must be positive.")
