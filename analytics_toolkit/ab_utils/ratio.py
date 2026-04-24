from __future__ import annotations

import math

import numpy as np
import pandas as pd


def _normalize_ratio_metrics(
    df: pd.DataFrame,
    ratio_metrics: list[dict[str, object]] | None,
    reserved_columns: set[str],
) -> list[dict[str, str]]:
    if not ratio_metrics:
        return []

    normalized_specs: list[dict[str, str]] = []
    ratio_names: set[str] = set()
    for index, raw_spec in enumerate(ratio_metrics):
        if not isinstance(raw_spec, dict):
            raise TypeError(f"ratio_metrics[{index}] must be a dictionary.")

        name = _require_ratio_spec_value(raw_spec, "name", index)
        numerator = _require_ratio_spec_value(raw_spec, "numerator", index)
        denominator = _require_ratio_spec_value(raw_spec, "denominator", index)

        level = str(raw_spec.get("level", "agg")).strip().lower()
        invalid_denominator = str(raw_spec.get("invalid_denominator", "ignore")).strip().lower()

        if level not in {"agg", "user"}:
            raise ValueError(
                f"ratio_metrics[{index}] has invalid level '{level}'. Expected 'agg' or 'user'."
            )
        if invalid_denominator != "ignore":
            raise ValueError(
                f"ratio_metrics[{index}] has invalid invalid_denominator '{invalid_denominator}'. "
                "Only 'ignore' is supported."
            )
        if name in ratio_names:
            raise ValueError(f"Duplicate ratio metric name '{name}'.")
        for column_name in (numerator, denominator):
            if column_name in reserved_columns:
                raise ValueError(
                    f"ratio_metrics[{index}] references reserved column '{column_name}'."
                )
            if column_name not in df.columns:
                raise ValueError(f"ratio_metrics[{index}] references missing column '{column_name}'.")

        ratio_names.add(name)
        normalized_specs.append(
            {
                "name": name,
                "numerator": numerator,
                "denominator": denominator,
                "level": level,
                "invalid_denominator": invalid_denominator,
            }
        )

    return normalized_specs


def _require_ratio_spec_value(raw_spec: dict[str, object], key: str, index: int) -> str:
    if key not in raw_spec:
        raise ValueError(f"ratio_metrics[{index}] is missing required key '{key}'.")
    value = str(raw_spec[key]).strip()
    if not value:
        raise ValueError(f"ratio_metrics[{index}] has empty '{key}'.")
    return value


def _build_agg_ratio_linearized_values(
    numerator: pd.Series,
    denominator: pd.Series,
) -> tuple[pd.Series, str | None]:
    valid_mask = _build_ratio_valid_mask(
        numerator=numerator,
        denominator=denominator,
        level="agg",
    )
    if not valid_mask.any():
        return pd.Series(np.nan, index=numerator.index, dtype=float), "no valid numerator/denominator pairs"

    denominator_sum = float(denominator.loc[valid_mask].sum())
    if denominator_sum <= 0:
        return pd.Series(np.nan, index=numerator.index, dtype=float), "aggregate denominator sum is non-positive"

    ratio = float(numerator.loc[valid_mask].sum()) / denominator_sum
    values = pd.Series(np.nan, index=numerator.index, dtype=float)
    values.loc[valid_mask] = numerator.loc[valid_mask] - ratio * denominator.loc[valid_mask]
    return values, None


def _compute_agg_ratio_group_stats(group_frame: pd.DataFrame) -> dict[str, float]:
    n = int(group_frame.shape[0])
    if n == 0:
        return {"n": 0, "ratio": math.nan}

    denominator_sum = float(group_frame["denominator"].sum())
    if denominator_sum <= 0:
        return {"n": n, "ratio": math.nan}

    numerator_sum = float(group_frame["numerator"].sum())
    return {"n": n, "ratio": numerator_sum / denominator_sum}


def _compute_agg_ratio_diff_standard_error(
    baseline_frame: pd.DataFrame,
    baseline_ratio: float,
    test_frame: pd.DataFrame,
    test_ratio: float,
) -> float:
    baseline_variance = _compute_agg_ratio_variance(baseline_frame, baseline_ratio)
    test_variance = _compute_agg_ratio_variance(test_frame, test_ratio)
    if math.isnan(baseline_variance) or math.isnan(test_variance):
        return math.nan
    return math.sqrt(baseline_variance + test_variance)


def _compute_agg_ratio_variance(group_frame: pd.DataFrame, ratio: float) -> float:
    n = int(group_frame.shape[0])
    if n < 2 or math.isnan(ratio):
        return math.nan

    denominator_mean = float(group_frame["denominator"].mean())
    if denominator_mean <= 0:
        return math.nan

    centered = group_frame["numerator"] - ratio * group_frame["denominator"]
    centered_variance = float(centered.var(ddof=1))
    if math.isnan(centered_variance):
        return math.nan

    return centered_variance / (n * (denominator_mean ** 2))


def _compute_agg_ratio_group_stats_arrays(
    numerator: np.ndarray,
    denominator: np.ndarray,
) -> dict[str, float]:
    n = int(numerator.shape[0])
    if n == 0:
        return {"n": 0, "ratio": math.nan}

    denominator_sum = float(denominator.sum())
    if denominator_sum <= 0:
        return {"n": n, "ratio": math.nan}

    numerator_sum = float(numerator.sum())
    return {"n": n, "ratio": numerator_sum / denominator_sum}


def _build_ratio_frame_from_arrays(
    numerator: np.ndarray,
    denominator: np.ndarray,
) -> pd.DataFrame:
    return pd.DataFrame({"numerator": numerator, "denominator": denominator})


def _build_ratio_valid_mask_from_arrays(
    numerator: np.ndarray,
    denominator: np.ndarray,
    level: str,
) -> np.ndarray:
    nonmissing_mask = ~np.isnan(numerator) & ~np.isnan(denominator)
    if level == "user":
        return nonmissing_mask & (denominator > 0)
    return nonmissing_mask


def _build_ratio_valid_mask(
    numerator: pd.Series,
    denominator: pd.Series,
    level: str,
) -> pd.Series:
    nonmissing_mask = numerator.notna() & denominator.notna()
    if level == "user":
        return nonmissing_mask & (denominator > 0)
    return nonmissing_mask
