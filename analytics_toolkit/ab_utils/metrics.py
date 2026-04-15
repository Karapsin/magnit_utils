from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import combinations
import math
import warnings

import numpy as np
import pandas as pd
from scipy.stats import norm, ttest_ind
from tqdm import tqdm

DEFAULT_ALPHA = 0.05
DEFAULT_POWER = 0.80


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
) -> pd.DataFrame:
    """Compute per-metric experiment comparison statistics.

    Notes:
    - The input must contain exactly one row per user.
    - All columns except `group` and `user_id` are treated as metric columns.
    - Missing metric values are ignored independently for each metric/group pair.
    - `mde_abs` and `mde_relative` use a two-sided normal approximation based
      on the observed sample variances.
    - Ratio metrics can be passed through `ratio_metrics`; their output names are
      tagged with `[ratio]`.
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

    metric_columns = [column for column in df.columns if column not in {group, user_id}]
    if not metric_columns:
        if not ratio_metrics:
            raise ValueError("The dataframe must contain at least one metric column.")

    group_names = df[group].drop_duplicates().tolist()
    if control not in group_names:
        raise ValueError(f"Control label '{control}' was not found in column '{group}'.")

    include_groups = len(group_names) > 2
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
            if include_groups:
                row = {"groups": f"{test_group} vs {baseline_group}", **row}
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
    if multiple_comparisons_adjustment:
        columns.append("bootstrap_adj_p")
    if include_groups:
        columns = ["groups", *columns]

    for row in rows:
        row.pop("_metric_key", None)
        row.pop("_test_stat", None)

    return pd.DataFrame(rows, columns=columns)


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
            "metric_key": f"[ratio] {ratio_spec['name']}",
            "ratio_spec": ratio_spec,
        }
        for ratio_spec in ratio_specs
    )
    return metric_definitions


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


def _get_numeric_metric_series(df: pd.DataFrame, metric_name: str) -> pd.Series:
    original = df[metric_name]
    numeric = pd.to_numeric(original, errors="coerce")

    if numeric.notna().sum() != original.notna().sum():
        raise TypeError(f"Metric column '{metric_name}' contains non-numeric values.")

    numeric.name = metric_name
    return numeric


def _safe_mean(values: pd.Series) -> float:
    if values.empty:
        return math.nan
    return float(values.mean())


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


def _compute_ttest_stat_and_p_value(
    baseline_values: pd.Series,
    test_values: pd.Series,
) -> tuple[float, float]:
    if baseline_values.shape[0] < 2 or test_values.shape[0] < 2:
        return math.nan, math.nan

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        result = ttest_ind(test_values, baseline_values, equal_var=False, nan_policy="omit")
    return float(result.statistic), float(result.pvalue)


def _compute_mde_abs(
    baseline_values: pd.Series,
    test_values: pd.Series,
    alpha: float = DEFAULT_ALPHA,
    power: float = DEFAULT_POWER,
) -> float:
    n0 = baseline_values.shape[0]
    n1 = test_values.shape[0]
    if n0 < 2 or n1 < 2:
        return math.nan

    variance0 = float(baseline_values.var(ddof=1))
    variance1 = float(test_values.var(ddof=1))
    if math.isnan(variance0) or math.isnan(variance1):
        return math.nan

    z_alpha = float(norm.ppf(1 - alpha / 2))
    z_power = float(norm.ppf(power))
    return (z_alpha + z_power) * math.sqrt((variance0 / n0) + (variance1 / n1))


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


def _compute_normal_p_value(delta_abs: float, standard_error: float) -> float:
    statistic = _compute_studentized_statistic(delta_abs, standard_error)
    if math.isnan(statistic):
        return math.nan
    return float(2 * norm.sf(abs(statistic)))


def _compute_mde_from_standard_error(
    standard_error: float,
    alpha: float,
    power: float,
) -> float:
    if math.isnan(standard_error) or standard_error <= 0:
        return math.nan
    z_alpha = float(norm.ppf(1 - alpha / 2))
    z_power = float(norm.ppf(power))
    return (z_alpha + z_power) * standard_error


def _compute_studentized_statistic(delta_abs: float, standard_error: float) -> float:
    if math.isnan(delta_abs) or math.isnan(standard_error) or standard_error <= 0:
        return math.nan
    return delta_abs / standard_error


def _apply_multiple_comparisons_adjustment(
    rows: list[dict[str, object]],
    df: pd.DataFrame,
    group_column: str,
    metric_definitions: list[dict[str, object]],
    comparisons: list[tuple[str, str]],
    resamples: int,
    random_state: int | None,
    n_jobs: int,
    show_progress: bool,
) -> None:
    if not rows:
        return

    bootstrap_context = _prepare_bootstrap_context(
        df=df,
        group_column=group_column,
        metric_definitions=metric_definitions,
        comparisons=comparisons,
    )
    family_max_statistics = _compute_bootstrap_family_max_statistics(
        bootstrap_context=bootstrap_context,
        resamples=resamples,
        random_state=random_state,
        n_jobs=n_jobs,
        show_progress=show_progress,
    )

    for row in rows:
        observed_stat = row.get("_test_stat")
        metric_key = str(row.get("_metric_key"))
        if not isinstance(observed_stat, (int, float)) or math.isnan(float(observed_stat)):
            row["bootstrap_adj_p"] = math.nan
            continue

        bootstrap_stats = [
            value
            for value in family_max_statistics.get(metric_key, [])
            if not math.isnan(value)
        ]
        if not bootstrap_stats:
            row["bootstrap_adj_p"] = math.nan
            continue

        observed_abs_stat = abs(float(observed_stat))
        exceedances = sum(value >= observed_abs_stat for value in bootstrap_stats)
        row["bootstrap_adj_p"] = exceedances / len(bootstrap_stats)


def _prepare_bootstrap_context(
    df: pd.DataFrame,
    group_column: str,
    metric_definitions: list[dict[str, object]],
    comparisons: list[tuple[str, str]],
) -> dict[str, object]:
    group_values = df[group_column].to_numpy()
    group_names = list(dict.fromkeys(group_values.tolist()))
    group_code_by_name = {name: code for code, name in enumerate(group_names)}
    group_codes = np.array([group_code_by_name[value] for value in group_values], dtype=np.int16)

    metric_contexts: list[dict[str, object]] = []
    for metric_definition in metric_definitions:
        metric_key = str(metric_definition["metric_key"])
        if metric_definition["kind"] == "mean":
            metric_contexts.append(
                {
                    "kind": "mean",
                    "metric_key": metric_key,
                    "values": _get_numeric_metric_series(
                        df, str(metric_definition["column"])
                    ).to_numpy(dtype=float),
                }
            )
            continue

        ratio_spec = dict(metric_definition["ratio_spec"])
        numerator = _get_numeric_metric_series(df, ratio_spec["numerator"]).to_numpy(dtype=float)
        denominator = _get_numeric_metric_series(df, ratio_spec["denominator"]).to_numpy(
            dtype=float
        )
        valid_mask = _build_ratio_valid_mask_from_arrays(
            numerator=numerator,
            denominator=denominator,
            level=ratio_spec["level"],
        )

        ratio_context: dict[str, object] = {
            "kind": "ratio",
            "metric_key": metric_key,
            "level": ratio_spec["level"],
            "numerator": numerator,
            "denominator": denominator,
            "valid_mask": valid_mask,
        }
        if ratio_spec["level"] == "user":
            ratio_values = np.full(numerator.shape[0], np.nan, dtype=float)
            ratio_values[valid_mask] = numerator[valid_mask] / denominator[valid_mask]
            ratio_context["values"] = ratio_values
        metric_contexts.append(ratio_context)

    return {
        "group_codes": group_codes,
        "metric_contexts": metric_contexts,
        "comparisons": [
            (group_code_by_name[test_group], group_code_by_name[baseline_group])
            for test_group, baseline_group in comparisons
        ],
    }


def _compute_bootstrap_family_max_statistics(
    bootstrap_context: dict[str, object],
    resamples: int,
    random_state: int | None,
    n_jobs: int,
    show_progress: bool,
) -> dict[str, list[float]]:
    metric_keys = [
        str(metric_context["metric_key"])
        for metric_context in list(bootstrap_context["metric_contexts"])
    ]
    family_max_statistics: dict[str, list[float]] = {metric_key: [] for metric_key in metric_keys}

    batch_sizes = _split_resamples_into_batches(resamples, n_jobs=n_jobs)
    if not batch_sizes:
        return family_max_statistics

    if n_jobs == 1 or len(batch_sizes) == 1:
        rng = np.random.default_rng(random_state)
        batch_result = _compute_bootstrap_family_max_statistics_batch(
            bootstrap_context=bootstrap_context,
            resamples=batch_sizes[0],
            rng_or_seed=rng,
            progress_position=0 if show_progress else None,
        )
        for metric_key, values in batch_result.items():
            family_max_statistics[metric_key].extend(values)
        return family_max_statistics

    seed_sequence = np.random.SeedSequence(random_state)
    child_sequences = seed_sequence.spawn(len(batch_sizes))
    try:
        batch_results = _compute_bootstrap_family_max_statistics_in_executor(
            executor_cls=ProcessPoolExecutor,
            bootstrap_context=bootstrap_context,
            batch_sizes=batch_sizes,
            child_sequences=child_sequences,
            n_jobs=n_jobs,
            show_progress=show_progress,
        )
    except (NotImplementedError, PermissionError, OSError):
        batch_results = _compute_bootstrap_family_max_statistics_in_executor(
            executor_cls=ThreadPoolExecutor,
            bootstrap_context=bootstrap_context,
            batch_sizes=batch_sizes,
            child_sequences=child_sequences,
            n_jobs=n_jobs,
            show_progress=show_progress,
        )

    for batch_result in batch_results:
        for metric_key, values in batch_result.items():
            family_max_statistics[metric_key].extend(values)

    return family_max_statistics


def _compute_bootstrap_family_max_statistics_in_executor(
    executor_cls: type[ProcessPoolExecutor] | type[ThreadPoolExecutor],
    bootstrap_context: dict[str, object],
    batch_sizes: list[int],
    child_sequences: list[np.random.SeedSequence],
    n_jobs: int,
    show_progress: bool,
) -> list[dict[str, list[float]]]:
    with executor_cls(max_workers=n_jobs) as executor:
        futures = [
            executor.submit(
                _compute_bootstrap_family_max_statistics_batch,
                bootstrap_context,
                batch_size,
                child_sequence,
                index if show_progress else None,
            )
            for index, (batch_size, child_sequence) in enumerate(
                zip(batch_sizes, child_sequences, strict=True)
            )
        ]
        return [future.result() for future in futures]


def _split_resamples_into_batches(resamples: int, n_jobs: int) -> list[int]:
    batch_count = min(resamples, max(1, n_jobs))
    base_batch_size, remainder = divmod(resamples, batch_count)
    return [
        base_batch_size + (1 if batch_index < remainder else 0)
        for batch_index in range(batch_count)
        if base_batch_size + (1 if batch_index < remainder else 0) > 0
    ]


def _compute_bootstrap_family_max_statistics_batch(
    bootstrap_context: dict[str, object],
    resamples: int,
    rng_or_seed: np.random.Generator | np.random.SeedSequence,
    progress_position: int | None = None,
) -> dict[str, list[float]]:
    rng = (
        rng_or_seed
        if isinstance(rng_or_seed, np.random.Generator)
        else np.random.default_rng(rng_or_seed)
    )
    group_codes = np.asarray(bootstrap_context["group_codes"], dtype=np.int16)
    comparisons = list(bootstrap_context["comparisons"])
    metric_contexts = list(bootstrap_context["metric_contexts"])

    family_max_statistics: dict[str, list[float]] = {
        str(metric_context["metric_key"]): [] for metric_context in metric_contexts
    }
    sample_size = group_codes.shape[0]

    iterator = range(resamples)
    if progress_position is not None:
        iterator = tqdm(
            iterator,
            total=resamples,
            desc="bootstrap",
            position=progress_position,
            leave=(progress_position == 0),
        )

    for _ in iterator:
        sample_indices = rng.integers(0, sample_size, size=sample_size)
        sampled_group_codes = group_codes[sample_indices]
        iteration_max_stats = _compute_metric_family_max_statistics_from_indices(
            metric_contexts=metric_contexts,
            sampled_group_codes=sampled_group_codes,
            sample_indices=sample_indices,
            comparisons=comparisons,
        )
        for metric_key, max_stat in iteration_max_stats.items():
            family_max_statistics[metric_key].append(max_stat)

    return family_max_statistics


def _compute_metric_family_max_statistics_from_indices(
    metric_contexts: list[dict[str, object]],
    sampled_group_codes: np.ndarray,
    sample_indices: np.ndarray,
    comparisons: list[tuple[int, int]],
) -> dict[str, float]:
    max_statistics: dict[str, float] = {}
    for metric_context in metric_contexts:
        metric_key = str(metric_context["metric_key"])
        comparison_statistics: list[float] = []
        for test_group_code, baseline_group_code in comparisons:
            statistic = _compute_metric_test_statistic_from_indices(
                metric_context=metric_context,
                sampled_group_codes=sampled_group_codes,
                sample_indices=sample_indices,
                baseline_group_code=baseline_group_code,
                test_group_code=test_group_code,
            )
            if not math.isnan(statistic):
                comparison_statistics.append(abs(statistic))
        if comparison_statistics:
            max_statistics[metric_key] = max(comparison_statistics)
        else:
            max_statistics[metric_key] = math.nan
    return max_statistics


def _compute_metric_test_statistic_from_indices(
    metric_context: dict[str, object],
    sampled_group_codes: np.ndarray,
    sample_indices: np.ndarray,
    baseline_group_code: int,
    test_group_code: int,
) -> float:
    baseline_mask = sampled_group_codes == baseline_group_code
    test_mask = sampled_group_codes == test_group_code

    if metric_context["kind"] == "mean":
        sampled_values = np.asarray(metric_context["values"], dtype=float)[sample_indices]
        baseline_values = sampled_values[baseline_mask & ~np.isnan(sampled_values)]
        test_values = sampled_values[test_mask & ~np.isnan(sampled_values)]
        statistic, _ = _compute_ttest_stat_and_p_value_arrays(baseline_values, test_values)
        return statistic

    if metric_context["level"] == "user":
        sampled_values = np.asarray(metric_context["values"], dtype=float)[sample_indices]
        baseline_values = sampled_values[baseline_mask & ~np.isnan(sampled_values)]
        test_values = sampled_values[test_mask & ~np.isnan(sampled_values)]
        statistic, _ = _compute_ttest_stat_and_p_value_arrays(baseline_values, test_values)
        return statistic

    sampled_numerator = np.asarray(metric_context["numerator"], dtype=float)[sample_indices]
    sampled_denominator = np.asarray(metric_context["denominator"], dtype=float)[sample_indices]
    sampled_valid_mask = np.asarray(metric_context["valid_mask"], dtype=bool)[sample_indices]

    baseline_valid_mask = baseline_mask & sampled_valid_mask
    test_valid_mask = test_mask & sampled_valid_mask

    baseline_stats = _compute_agg_ratio_group_stats_arrays(
        sampled_numerator[baseline_valid_mask],
        sampled_denominator[baseline_valid_mask],
    )
    test_stats = _compute_agg_ratio_group_stats_arrays(
        sampled_numerator[test_valid_mask],
        sampled_denominator[test_valid_mask],
    )
    if not _both_present(test_stats["ratio"], baseline_stats["ratio"]):
        return math.nan
    delta_abs = test_stats["ratio"] - baseline_stats["ratio"]
    se_diff = _compute_agg_ratio_diff_standard_error(
        baseline_frame=_build_ratio_frame_from_arrays(
            sampled_numerator[baseline_valid_mask], sampled_denominator[baseline_valid_mask]
        ),
        baseline_ratio=baseline_stats["ratio"],
        test_frame=_build_ratio_frame_from_arrays(
            sampled_numerator[test_valid_mask], sampled_denominator[test_valid_mask]
        ),
        test_ratio=test_stats["ratio"],
    )
    return _compute_studentized_statistic(delta_abs, se_diff)


def _compute_ttest_stat_and_p_value_arrays(
    baseline_values: np.ndarray,
    test_values: np.ndarray,
) -> tuple[float, float]:
    return _compute_ttest_stat_and_p_value(
        pd.Series(baseline_values),
        pd.Series(test_values),
    )


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


def _safe_relative(numerator: float, denominator: float) -> float:
    if math.isnan(numerator) or math.isnan(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


def _both_present(left: float, right: float) -> bool:
    return not math.isnan(left) and not math.isnan(right)
