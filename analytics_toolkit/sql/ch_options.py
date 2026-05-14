from __future__ import annotations

from collections.abc import Sequence


DEFAULT_CH_ENGINE = "ReplicatedMergeTree"
DEFAULT_CH_CLUSTER = "{cluster}"
DEFAULT_CH_SHARDING_KEY = "rand()"


def normalize_ch_columns_or_expression(
    value: Sequence[str] | str | None,
    option_name: str,
) -> list[str] | str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return normalize_ch_string(value, option_name)

    normalized = [normalize_ch_string(column, option_name) for column in value]
    if not normalized:
        raise ValueError(f"{option_name} must not be empty when provided.")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{option_name} must not contain duplicate column names.")
    return normalized


def normalize_ch_string(value: str, option_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{option_name} must not be empty.")
    return normalized


def validate_ch_options_not_used(
    *,
    target_backend: str,
    option_owner: str,
    ch_partition_by: list[str] | str | None,
    ch_order_by: list[str] | str | None,
    ch_engine: str,
    ch_cluster: str,
    ch_sharding_key: str,
) -> None:
    if target_backend == "ch":
        return

    if ch_partition_by is not None:
        raise ValueError(
            f"ch_partition_by can only be used when {option_owner} has type 'ch'."
        )
    if ch_order_by is not None:
        raise ValueError(
            f"ch_order_by can only be used when {option_owner} has type 'ch'."
        )
    if ch_engine != DEFAULT_CH_ENGINE:
        raise ValueError(
            f"ch_engine can only be used when {option_owner} has type 'ch'."
        )
    if ch_cluster != DEFAULT_CH_CLUSTER:
        raise ValueError(
            f"ch_cluster can only be used when {option_owner} has type 'ch'."
        )
    if ch_sharding_key != DEFAULT_CH_SHARDING_KEY:
        raise ValueError(
            f"sharding_key can only be used when {option_owner} has type 'ch'."
        )


def validate_ch_columns_in_columns(
    value: list[str] | str | None,
    columns: Sequence[str],
    option_name: str,
    *,
    data_name: str,
) -> None:
    if value is None or isinstance(value, str):
        return

    available_columns = {str(column) for column in columns}
    missing_columns = [column for column in value if column not in available_columns]
    if missing_columns:
        raise ValueError(
            f"{option_name} columns were not found in the {data_name}: "
            + ", ".join(missing_columns)
        )
