from __future__ import annotations

import math
import warnings
from collections.abc import Sequence
from numbers import Real
from typing import Hashable

import numpy as np
import pandas as pd

_MISSING_STRATUM_VALUE = object()
_ALL_STRATUM_KEY = ("__all__",)


def do_split(
    df: pd.DataFrame,
    split_col: str = "user_id",
    stratification_cols: str | Sequence[str] | None = None,
    mandatory_users_df: pd.DataFrame | None = None,
    mandatory_users_group: str = "any",
    target_sample_size: int | None = None,
    test_groups_num: int = 1,
    compensate_mandatory_users: bool = False,
    test_group_ratios: Sequence[Real] | None = None,
    random_state: int | None = 42,
    group_col: str = "group_name",
) -> pd.DataFrame:
    """Deterministically sample users and assign them to AB groups.

    The output contains sampled input rows plus ``group_col`` and
    ``is_mandatory_user``.
    """

    group_names = _build_group_names(test_groups_num)
    ratios = _normalize_group_ratios(test_group_ratios, expected_size=len(group_names))
    strata_columns = _normalize_stratification_cols(stratification_cols)
    _validate_split_dataframe(df, split_col=split_col, stratification_cols=strata_columns)
    _validate_group_col(group_col)
    _validate_random_state(random_state)

    if not isinstance(compensate_mandatory_users, bool):
        raise TypeError("compensate_mandatory_users must be a boolean.")

    mandatory_mode = _normalize_mandatory_users_group(mandatory_users_group, group_names)
    rng = np.random.default_rng(random_state)

    target_size = _normalize_target_sample_size(target_sample_size, max_size=len(df))
    strata_keys = _build_stratum_keys(df, strata_columns)
    id_to_position = {user_id: position for position, user_id in enumerate(df[split_col].tolist())}

    mandatory_positions = _get_present_mandatory_positions(
        mandatory_users_df=mandatory_users_df,
        split_col=split_col,
        id_to_position=id_to_position,
    )
    if len(mandatory_positions) > target_size:
        raise ValueError(
            "Present mandatory users exceed target_sample_size and cannot all be included."
        )

    mandatory_position_set = set(mandatory_positions)
    randomized_target_size = target_size - len(mandatory_positions)
    randomized_candidates = [
        position for position in range(len(df)) if position not in mandatory_position_set
    ]
    randomized_positions = _sample_positions_by_strata(
        randomized_candidates,
        strata_keys=strata_keys,
        sample_size=randomized_target_size,
        rng=rng,
    )

    forced_assignments = _build_forced_mandatory_assignments(
        mandatory_positions=mandatory_positions,
        mandatory_mode=mandatory_mode,
        group_names=group_names,
        rng=rng,
    )
    forced_counts = _count_assignments(forced_assignments, group_names)

    if mandatory_mode == "any":
        assignable_positions = [*randomized_positions, *mandatory_positions]
        assignable_counts = _round_counts(
            len(assignable_positions),
            ratios,
            rng=rng,
        )
    elif compensate_mandatory_users:
        final_counts = _round_counts(target_size, ratios, rng=rng)
        assignable_counts = [
            desired_count - forced_count
            for desired_count, forced_count in zip(final_counts, forced_counts)
        ]
        impossible_groups = [
            group
            for group, count in zip(group_names, assignable_counts)
            if count < 0
        ]
        if impossible_groups:
            labels = ", ".join(impossible_groups)
            raise ValueError(
                "Mandatory users make compensated group quotas impossible "
                f"for group(s): {labels}."
            )
        assignable_positions = randomized_positions
    else:
        assignable_positions = randomized_positions
        assignable_counts = _round_counts(
            len(assignable_positions),
            ratios,
            rng=rng,
        )

    randomized_assignments = _assign_positions_to_groups(
        assignable_positions,
        group_names=group_names,
        group_counts=assignable_counts,
        strata_keys=strata_keys,
        rng=rng,
    )
    assignments = {**randomized_assignments, **forced_assignments}
    selected_positions = sorted(assignments)

    result = df.iloc[selected_positions].copy()
    result[group_col] = [assignments[position] for position in selected_positions]
    result["is_mandatory_user"] = [
        position in mandatory_position_set for position in selected_positions
    ]
    return result


def _build_group_names(test_groups_num: int) -> list[str]:
    if isinstance(test_groups_num, bool) or not isinstance(test_groups_num, int):
        raise TypeError("test_groups_num must be an integer.")
    if test_groups_num <= 0:
        raise ValueError("test_groups_num must be positive.")
    return ["control", *[f"test_{index}" for index in range(1, test_groups_num + 1)]]


def _normalize_group_ratios(
    test_group_ratios: Sequence[Real] | None,
    *,
    expected_size: int,
) -> list[float]:
    if test_group_ratios is None:
        return [1.0] * expected_size

    if isinstance(test_group_ratios, (str, bytes)) or not isinstance(
        test_group_ratios,
        Sequence,
    ):
        raise TypeError("test_group_ratios must be a sequence of positive numeric values.")
    if len(test_group_ratios) != expected_size:
        raise ValueError(
            "test_group_ratios must contain one value for control and each test group."
        )

    ratios: list[float] = []
    for ratio in test_group_ratios:
        if isinstance(ratio, bool) or not isinstance(ratio, Real):
            raise TypeError("test_group_ratios values must be numeric.")
        ratio_float = float(ratio)
        if not math.isfinite(ratio_float) or ratio_float <= 0:
            raise ValueError("test_group_ratios values must be positive finite numbers.")
        ratios.append(ratio_float)

    if not math.isclose(sum(ratios), 100.0, rel_tol=0.0, abs_tol=1e-8):
        raise ValueError("test_group_ratios must sum to 100.")
    return ratios


def _normalize_stratification_cols(
    stratification_cols: str | Sequence[str] | None,
) -> list[str]:
    if stratification_cols is None:
        return []
    if isinstance(stratification_cols, str):
        return [stratification_cols]
    if isinstance(stratification_cols, (bytes, bytearray)):
        raise TypeError("stratification_cols must be a string or a sequence of strings.")
    if not isinstance(stratification_cols, Sequence):
        raise TypeError("stratification_cols must be a string or a sequence of strings.")

    columns = list(stratification_cols)
    if not all(isinstance(column, str) for column in columns):
        raise TypeError("stratification_cols must contain only strings.")
    return columns


def _validate_split_dataframe(
    df: pd.DataFrame,
    *,
    split_col: str,
    stratification_cols: Sequence[str],
) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if not isinstance(split_col, str):
        raise TypeError("split_col must be a string.")
    if split_col not in df.columns:
        raise ValueError(f"Column '{split_col}' was not found in df.")
    missing_columns = [column for column in stratification_cols if column not in df.columns]
    if missing_columns:
        missing = ", ".join(f"'{column}'" for column in missing_columns)
        raise ValueError(f"Missing stratification columns: {missing}.")
    if df[split_col].isna().any():
        raise ValueError(f"Column '{split_col}' must not contain missing values.")
    if df[split_col].duplicated().any():
        raise ValueError(f"Column '{split_col}' must contain unique user ids.")


def _validate_group_col(group_col: str) -> None:
    if not isinstance(group_col, str):
        raise TypeError("group_col must be a string.")
    if group_col == "is_mandatory_user":
        raise ValueError("group_col conflicts with output flag columns.")


def _validate_random_state(random_state: int | None) -> None:
    if random_state is None:
        return
    if isinstance(random_state, bool) or not isinstance(random_state, int):
        raise TypeError("random_state must be an integer or None.")


def _normalize_target_sample_size(
    target_sample_size: int | None,
    *,
    max_size: int,
) -> int:
    if target_sample_size is None:
        return max_size
    if isinstance(target_sample_size, bool) or not isinstance(target_sample_size, int):
        raise TypeError("target_sample_size must be an integer or None.")
    if target_sample_size <= 0:
        raise ValueError("target_sample_size must be positive.")
    return min(target_sample_size, max_size)


def _normalize_mandatory_users_group(
    mandatory_users_group: str,
    group_names: Sequence[str],
) -> str:
    if not isinstance(mandatory_users_group, str):
        raise TypeError("mandatory_users_group must be a string.")
    normalized_group = mandatory_users_group.strip().lower()
    allowed_groups = {"any", "control", "test_any", *group_names[1:]}
    if normalized_group not in allowed_groups:
        allowed = ", ".join(sorted(allowed_groups))
        raise ValueError(f"mandatory_users_group must be one of: {allowed}.")
    return normalized_group


def _get_present_mandatory_positions(
    *,
    mandatory_users_df: pd.DataFrame | None,
    split_col: str,
    id_to_position: dict[Hashable, int],
) -> list[int]:
    if mandatory_users_df is None:
        return []
    if not isinstance(mandatory_users_df, pd.DataFrame):
        raise TypeError("mandatory_users_df must be a pandas DataFrame.")
    if split_col not in mandatory_users_df.columns:
        raise ValueError(f"Column '{split_col}' was not found in mandatory_users_df.")
    if mandatory_users_df[split_col].isna().any():
        raise ValueError(
            f"Column '{split_col}' in mandatory_users_df must not contain missing values."
        )
    if mandatory_users_df[split_col].duplicated().any():
        raise ValueError(
            f"Column '{split_col}' in mandatory_users_df must contain unique user ids."
        )

    mandatory_ids = mandatory_users_df[split_col].tolist()
    present_positions = [
        id_to_position[user_id]
        for user_id in mandatory_ids
        if user_id in id_to_position
    ]
    missing_ids = [user_id for user_id in mandatory_ids if user_id not in id_to_position]
    if missing_ids:
        examples = ", ".join(repr(user_id) for user_id in missing_ids[:5])
        warnings.warn(
            f"{len(missing_ids)} mandatory user ids were not found in df and will be ignored. "
            f"Examples: {examples}.",
            UserWarning,
            stacklevel=2,
        )
    return present_positions


def _build_stratum_keys(
    df: pd.DataFrame,
    stratification_cols: Sequence[str],
) -> list[tuple[object, ...]]:
    if not stratification_cols:
        return [_ALL_STRATUM_KEY] * len(df)

    keys: list[tuple[object, ...]] = []
    for row in df.loc[:, list(stratification_cols)].itertuples(index=False, name=None):
        keys.append(
            tuple(
                _MISSING_STRATUM_VALUE if _is_missing_stratum_value(value) else value
                for value in row
            )
        )
    return keys


def _is_missing_stratum_value(value: object) -> bool:
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, (bool, np.bool_)):
        return bool(missing)
    return False


def _sample_positions_by_strata(
    positions: Sequence[int],
    *,
    strata_keys: Sequence[tuple[object, ...]],
    sample_size: int,
    rng: np.random.Generator,
) -> list[int]:
    positions = list(positions)
    if sample_size == len(positions):
        return positions
    if sample_size > len(positions):
        raise ValueError("sample_size cannot exceed the number of available positions.")
    if sample_size == 0:
        return []

    strata_positions = _positions_by_stratum(positions, strata_keys=strata_keys)
    strata = list(strata_positions)
    capacities = [len(strata_positions[stratum]) for stratum in strata]
    counts = _round_counts(sample_size, capacities, rng=rng)
    counts = _fit_counts_to_capacities(
        counts,
        capacities=capacities,
        total=sample_size,
        rng=rng,
    )

    sampled_positions: list[int] = []
    for stratum, count in zip(strata, counts):
        stratum_positions = strata_positions[stratum]
        sampled_positions.extend(_take_random_positions(stratum_positions, count, rng=rng))
    return sampled_positions


def _build_forced_mandatory_assignments(
    *,
    mandatory_positions: Sequence[int],
    mandatory_mode: str,
    group_names: Sequence[str],
    rng: np.random.Generator,
) -> dict[int, str]:
    if mandatory_mode == "any":
        return {}
    if mandatory_mode == "control":
        return {position: "control" for position in mandatory_positions}
    if mandatory_mode.startswith("test_") and mandatory_mode != "test_any":
        return {position: mandatory_mode for position in mandatory_positions}

    test_groups = list(group_names[1:])
    counts = _round_counts(len(mandatory_positions), [1.0] * len(test_groups), rng=rng)
    shuffled_positions = _take_random_positions(
        list(mandatory_positions),
        len(mandatory_positions),
        rng=rng,
    )
    assignments: dict[int, str] = {}
    start = 0
    for group_name, count in zip(test_groups, counts):
        for position in shuffled_positions[start : start + count]:
            assignments[position] = group_name
        start += count
    return assignments


def _assign_positions_to_groups(
    positions: Sequence[int],
    *,
    group_names: Sequence[str],
    group_counts: Sequence[int],
    strata_keys: Sequence[tuple[object, ...]],
    rng: np.random.Generator,
) -> dict[int, str]:
    positions = list(positions)
    if sum(group_counts) != len(positions):
        raise ValueError("group_counts must sum to the number of positions.")
    if not positions:
        return {}

    strata_positions = _positions_by_stratum(sorted(positions), strata_keys=strata_keys)
    strata = list(strata_positions)
    stratum_sizes = [len(strata_positions[stratum]) for stratum in strata]
    count_matrix = _build_stratified_count_matrix(
        stratum_sizes=stratum_sizes,
        group_counts=list(group_counts),
        rng=rng,
    )

    assignments: dict[int, str] = {}
    for stratum_index, stratum in enumerate(strata):
        shuffled_positions = _take_random_positions(
            strata_positions[stratum],
            len(strata_positions[stratum]),
            rng=rng,
        )
        start = 0
        for group_name, count in zip(group_names, count_matrix[stratum_index]):
            for position in shuffled_positions[start : start + count]:
                assignments[position] = group_name
            start += count
    return assignments


def _positions_by_stratum(
    positions: Sequence[int],
    *,
    strata_keys: Sequence[tuple[object, ...]],
) -> dict[tuple[object, ...], list[int]]:
    strata_positions: dict[tuple[object, ...], list[int]] = {}
    for position in positions:
        strata_positions.setdefault(strata_keys[position], []).append(position)
    return strata_positions


def _build_stratified_count_matrix(
    *,
    stratum_sizes: Sequence[int],
    group_counts: Sequence[int],
    rng: np.random.Generator,
) -> list[list[int]]:
    total = sum(stratum_sizes)
    if total == 0:
        return [[0 for _ in group_counts] for _ in stratum_sizes]

    expected = np.outer(
        np.asarray(stratum_sizes, dtype=float),
        np.asarray(group_counts, dtype=float),
    )
    expected = expected / float(total)
    floor_counts = np.floor(expected + 1e-12).astype(int)
    row_deficits = np.asarray(stratum_sizes, dtype=int) - floor_counts.sum(axis=1)
    column_deficits = np.asarray(group_counts, dtype=int) - floor_counts.sum(axis=0)

    fractions = expected - np.floor(expected + 1e-12)
    tie_breakers = rng.random(size=fractions.shape)
    cells = [
        (
            fractions[row_index, column_index],
            tie_breakers[row_index, column_index],
            row_index,
            column_index,
        )
        for row_index in range(fractions.shape[0])
        for column_index in range(fractions.shape[1])
    ]
    cells.sort(key=lambda item: (-item[0], item[1]))

    for _, _, row_index, column_index in cells:
        if row_deficits[row_index] <= 0 or column_deficits[column_index] <= 0:
            continue
        floor_counts[row_index, column_index] += 1
        row_deficits[row_index] -= 1
        column_deficits[column_index] -= 1

    if row_deficits.sum() or column_deficits.sum():
        _fill_remaining_matrix_deficits(
            floor_counts=floor_counts,
            row_deficits=row_deficits,
            column_deficits=column_deficits,
            rng=rng,
        )

    return floor_counts.tolist()


def _fill_remaining_matrix_deficits(
    *,
    floor_counts: np.ndarray,
    row_deficits: np.ndarray,
    column_deficits: np.ndarray,
    rng: np.random.Generator,
) -> None:
    while row_deficits.sum() > 0:
        row_candidates = np.flatnonzero(row_deficits > 0)
        column_candidates = np.flatnonzero(column_deficits > 0)
        if len(row_candidates) == 0 or len(column_candidates) == 0:
            raise ValueError("Unable to build stratified group counts.")

        row_order = row_candidates[np.argsort(rng.random(len(row_candidates)))]
        column_order = column_candidates[np.argsort(rng.random(len(column_candidates)))]
        made_progress = False
        for row_index in row_order:
            if row_deficits[row_index] <= 0:
                continue
            for column_index in column_order:
                if column_deficits[column_index] <= 0:
                    continue
                floor_counts[row_index, column_index] += 1
                row_deficits[row_index] -= 1
                column_deficits[column_index] -= 1
                made_progress = True
                break
        if not made_progress:
            raise ValueError("Unable to build stratified group counts.")


def _count_assignments(
    assignments: dict[int, str],
    group_names: Sequence[str],
) -> list[int]:
    return [sum(group == group_name for group in assignments.values()) for group_name in group_names]


def _round_counts(
    total: int,
    weights: Sequence[Real],
    *,
    rng: np.random.Generator,
) -> list[int]:
    if total < 0:
        raise ValueError("total must be non-negative.")
    if not weights:
        if total == 0:
            return []
        raise ValueError("weights must not be empty.")

    weight_values = np.asarray([float(weight) for weight in weights], dtype=float)
    if (weight_values < 0).any() or not np.isfinite(weight_values).all():
        raise ValueError("weights must be non-negative finite values.")
    weight_sum = float(weight_values.sum())
    if weight_sum <= 0:
        raise ValueError("weights must have a positive sum.")

    raw_counts = weight_values / weight_sum * total
    floor_counts = np.floor(raw_counts + 1e-12).astype(int)
    remainder = int(total - int(floor_counts.sum()))
    if remainder <= 0:
        return floor_counts.tolist()

    fractions = raw_counts - np.floor(raw_counts + 1e-12)
    tie_breakers = rng.random(len(weight_values))
    order = sorted(
        range(len(weight_values)),
        key=lambda index: (-fractions[index], tie_breakers[index]),
    )
    for index in order[:remainder]:
        floor_counts[index] += 1
    return floor_counts.tolist()


def _fit_counts_to_capacities(
    counts: Sequence[int],
    *,
    capacities: Sequence[int],
    total: int,
    rng: np.random.Generator,
) -> list[int]:
    adjusted_counts = [min(count, capacity) for count, capacity in zip(counts, capacities)]
    deficit = total - sum(adjusted_counts)
    if deficit <= 0:
        return adjusted_counts

    candidates = [
        index for index, capacity in enumerate(capacities) if adjusted_counts[index] < capacity
    ]
    while deficit > 0 and candidates:
        candidate_order = sorted(candidates, key=lambda _: rng.random())
        for index in candidate_order:
            if deficit <= 0:
                break
            available = capacities[index] - adjusted_counts[index]
            if available <= 0:
                continue
            add_count = min(available, deficit)
            adjusted_counts[index] += add_count
            deficit -= add_count
        candidates = [
            index
            for index, capacity in enumerate(capacities)
            if adjusted_counts[index] < capacity
        ]
    if deficit:
        raise ValueError("Unable to fit counts to available strata.")
    return adjusted_counts


def _take_random_positions(
    positions: Sequence[int],
    count: int,
    *,
    rng: np.random.Generator,
) -> list[int]:
    if count == 0:
        return []
    if count > len(positions):
        raise ValueError("count cannot exceed the number of positions.")
    shuffled_indices = rng.permutation(len(positions))
    return [positions[index] for index in shuffled_indices[:count]]
