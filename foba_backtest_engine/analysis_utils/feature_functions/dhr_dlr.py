from collections.abc import Sequence
from typing import Any, Literal, TypeVar, cast
TZ_STR = "Asia/Hong_Kong"
from pandas import DataFrame
from foba_backtest_engine.analysis_utils.calc_utils.decayed_change import positive_decayed_change
from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import bfill, ffill
from numpy import dtype, int64, float64, ndarray, bool_, array, indices, where

DIRECTIONS = (1, -1)
DECAYED_RETURN_HALFLIVES = (1, 8, 60, 480)
RDVWAP_HALFLIVES = (1, 2, 4, 8, 15, 30, 60, 120, 240, 480, 900, 1800, 3600)


# Decayed High/Low Returns
def t_calculate_dhr_dlrs(
    timestamp: ndarray[Any, dtype[int64]],
    is_trading: ndarray[Any, dtype[bool_]],
    trigger_direction: ndarray[Any, dtype[int64]],
    log_value: ndarray[Any, dtype[float64]],
    halflife: Sequence[int],
    value_str: str = "midspot",
) -> DataFrame:
    # [DIRECTIONS, HALFLIVES, TIMESTAMP]
    value_ddr = positive_decayed_change(
        timestamp,
        bfill(ffill(log_value)) * array(DIRECTIONS).reshape((-1, 1, 1)),
        is_trading * array(halflife).reshape((-1, 1)),
    )

    sparse_indices = indices(value_ddr.shape, sparse=True)

    # [SUPPORT|RESISTANCE, HALFLIVES, TIMESTAMP]
    value_ddr_dir = value_ddr[
        array((1, 0)).reshape((-1, 1, 1)) - (trigger_direction > 0),
        sparse_indices[1],
        sparse_indices[2],
    ]

    return DataFrame(
        {
            **{
                f"{value_str}_{prefix}_{hl}": value_ddr_dir[i, j, :]
                for i, prefix in enumerate(("dlr", "dhr"))
                for j, hl in enumerate(DECAYED_RETURN_HALFLIVES)
            },
            **{
                f"{value_str}_dtr_{hl}": value_ddr_dir[0, j, :] + value_ddr_dir[1, j, :]
                for j, hl in enumerate(DECAYED_RETURN_HALFLIVES)
            },
            **{
                f"{value_str}_dlr_skew_{hl}": where(
                    value_ddr_dir[0, j, :] + value_ddr_dir[1, j, :] == 0,
                    0.5,
                    value_ddr_dir[0, j, :]
                    / (value_ddr_dir[0, j, :] + value_ddr_dir[1, j, :]),
                )
                for j, hl in enumerate(DECAYED_RETURN_HALFLIVES)
            },
        }
    )
