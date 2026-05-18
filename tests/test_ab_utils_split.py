from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import analytics_toolkit.ab_utils as ab_utils
from analytics_toolkit.ab_utils import do_split


def _build_users(size: int = 30) -> pd.DataFrame:
    segments = ["A", "B", "C"]
    return pd.DataFrame(
        {
            "user_id": list(range(1, size + 1)),
            "segment": [segments[index % len(segments)] for index in range(size)],
        }
    )


def test_do_split_is_exported() -> None:
    assert ab_utils.do_split is do_split
    assert "do_split" in ab_utils.__all__


def test_do_split_default_equal_split_is_deterministic() -> None:
    df = _build_users(30)

    first = do_split(df, target_sample_size=20, random_state=123)
    second = do_split(df, target_sample_size=20, random_state=123)

    pd.testing.assert_frame_equal(first, second)
    assert len(first) == 20
    assert first["group_name"].value_counts().to_dict() == {"control": 10, "test_1": 10}
    assert first["is_mandatory_user"].eq(False).all()
    assert "is_randomized_user" not in first.columns


def test_do_split_uses_custom_group_ratios() -> None:
    df = _build_users(90)

    result = do_split(
        df,
        target_sample_size=60,
        test_groups_num=2,
        test_group_ratios=[100 / 6, (100 / 6) * 2, (100 / 6) * 3],
        random_state=7,
    )

    assert result["group_name"].value_counts().to_dict() == {
        "control": 10,
        "test_1": 20,
        "test_2": 30,
    }


@pytest.mark.parametrize(
    "ratios, error_type",
    [
        ([50, 50], ValueError),
        ([50, 50, 0], ValueError),
        ([50, 25, "25"], TypeError),
        ([5, 45, 45], ValueError),
    ],
)
def test_do_split_validates_custom_group_ratios(
    ratios: list[object],
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        do_split(_build_users(30), test_groups_num=2, test_group_ratios=ratios)


def test_do_split_noncompensated_mandatory_users_do_not_reduce_randomized_quotas() -> None:
    df = _build_users(20)
    mandatory_df = pd.DataFrame({"user_id": [1, 2, 3, 4]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="control",
        target_sample_size=10,
        compensate_mandatory_users=False,
        random_state=4,
    )

    assert result["group_name"].value_counts().to_dict() == {"control": 7, "test_1": 3}
    mandatory_rows = result[result["is_mandatory_user"]]
    assert set(mandatory_rows["user_id"]) == {1, 2, 3, 4}
    assert mandatory_rows["group_name"].eq("control").all()


def test_do_split_compensated_mandatory_users_are_counted_in_final_quotas() -> None:
    df = _build_users(20)
    mandatory_df = pd.DataFrame({"user_id": [1, 2, 3, 4]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="control",
        target_sample_size=10,
        compensate_mandatory_users=True,
        random_state=4,
    )

    assert result["group_name"].value_counts().to_dict() == {"control": 5, "test_1": 5}
    mandatory_rows = result[result["is_mandatory_user"]]
    assert set(mandatory_rows["user_id"]) == {1, 2, 3, 4}
    assert mandatory_rows["group_name"].eq("control").all()


def test_do_split_stratifies_exact_values_and_combines_missing_values() -> None:
    df = pd.DataFrame(
        {
            "user_id": list(range(1, 13)),
            "segment": ["A"] * 4 + ["B"] * 4 + [None, np.nan, pd.NA, None],
        }
    )

    result = do_split(df, stratification_cols="segment", random_state=12)

    for _, stratum in result.groupby("segment", dropna=False):
        assert stratum["group_name"].value_counts().to_dict() == {
            "control": 2,
            "test_1": 2,
        }


def test_do_split_mandatory_any_guarantees_inclusion_and_uses_final_ratios() -> None:
    df = _build_users(20)
    mandatory_df = pd.DataFrame({"user_id": [1, 2]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="any",
        target_sample_size=6,
        random_state=5,
    )

    assert set(mandatory_df["user_id"]).issubset(set(result["user_id"]))
    assert result["group_name"].value_counts().to_dict() == {"control": 3, "test_1": 3}
    assert set(result.loc[result["is_mandatory_user"], "user_id"]) == {1, 2}


def test_do_split_forces_mandatory_users_to_control() -> None:
    df = _build_users(20)
    mandatory_df = pd.DataFrame({"user_id": [1, 2, 3]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="control",
        target_sample_size=8,
        random_state=6,
    )

    mandatory_rows = result[result["is_mandatory_user"]]
    assert set(mandatory_rows["user_id"]) == {1, 2, 3}
    assert mandatory_rows["group_name"].eq("control").all()


def test_do_split_splits_mandatory_users_across_test_groups() -> None:
    df = _build_users(24)
    mandatory_df = pd.DataFrame({"user_id": [1, 2, 3]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="test_any",
        target_sample_size=12,
        test_groups_num=2,
        random_state=3,
    )

    mandatory_counts = (
        result[result["is_mandatory_user"]]["group_name"].value_counts().to_dict()
    )
    assert set(mandatory_counts) <= {"test_1", "test_2"}
    assert sorted(mandatory_counts.values()) == [1, 2]


def test_do_split_forces_mandatory_users_to_exact_test_group() -> None:
    df = _build_users(24)
    mandatory_df = pd.DataFrame({"user_id": [1, 2, 3]})

    result = do_split(
        df,
        mandatory_users_df=mandatory_df,
        mandatory_users_group="test_2",
        target_sample_size=12,
        test_groups_num=2,
        random_state=3,
    )

    mandatory_rows = result[result["is_mandatory_user"]]
    assert set(mandatory_rows["user_id"]) == {1, 2, 3}
    assert mandatory_rows["group_name"].eq("test_2").all()


def test_do_split_warns_and_ignores_missing_mandatory_ids() -> None:
    df = _build_users(9)
    mandatory_df = pd.DataFrame({"user_id": [2, 99, 100]})

    with pytest.warns(UserWarning, match="2 mandatory user ids were not found"):
        result = do_split(
            df,
            mandatory_users_df=mandatory_df,
            mandatory_users_group="control",
            random_state=2,
        )

    mandatory_rows = result[result["is_mandatory_user"]]
    assert mandatory_rows["user_id"].tolist() == [2]
    assert mandatory_rows["group_name"].tolist() == ["control"]


@pytest.mark.parametrize(
    "bad_df",
    [
        pd.DataFrame({"user_id": [1, 1], "segment": ["A", "B"]}),
        pd.DataFrame({"user_id": [1, None], "segment": ["A", "B"]}),
    ],
)
def test_do_split_validates_split_ids(bad_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError):
        do_split(bad_df)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"split_col": "missing"},
        {"stratification_cols": "missing"},
        {"mandatory_users_group": "test_3", "test_groups_num": 2},
        {"test_groups_num": 0},
    ],
)
def test_do_split_validates_columns_and_group_parameters(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        do_split(_build_users(12), **kwargs)


def test_do_split_validates_mandatory_user_inputs() -> None:
    with pytest.raises(ValueError):
        do_split(_build_users(12), mandatory_users_df=pd.DataFrame({"missing": [1]}))
    with pytest.raises(ValueError):
        do_split(_build_users(12), mandatory_users_df=pd.DataFrame({"user_id": [1, 1]}))
    with pytest.raises(ValueError):
        do_split(
            _build_users(12),
            mandatory_users_df=pd.DataFrame({"user_id": [1, 2, 3]}),
            target_sample_size=2,
        )


def test_do_split_raises_for_impossible_compensated_quotas() -> None:
    with pytest.raises(ValueError, match="compensated group quotas impossible"):
        do_split(
            _build_users(20),
            mandatory_users_df=pd.DataFrame({"user_id": [1, 2, 3, 4, 5, 6]}),
            mandatory_users_group="control",
            target_sample_size=10,
            compensate_mandatory_users=True,
            random_state=10,
        )
