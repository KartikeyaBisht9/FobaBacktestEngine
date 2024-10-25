from functools import partial
from typing import Any

import numpy as np
from numpy import dtype, float64, ndarray
from pandas import DataFrame

from foba_backtest_engine.analysis_utils.target_function.utils import (
    fixed_tick_bps,
    hkex_calculate_tick_size,
    weighted_column_sum_target,
)

FROM_BPS = 0.0001
TARGET_FACTOR = 1_000_000


def fixed_tick_bps_target_function(
    df: DataFrame,
    *,
    code,
    mid_spot: ndarray[Any, dtype[float64]],
    calc_target=partial(
        weighted_column_sum_target, (("rws_2", 0.5), ("rdvwap_30", 0.5))
    ),
) -> ndarray[Any, dtype[float64]]:
    delta = calc_target(df)

    tickSize = [hkex_calculate_tick_size(val) for val in mid_spot]
    tick_bps = 10_000 * np.mean(tickSize / mid_spot)

    return (
        fixed_tick_bps(
            delta,
            df["pre_midspot"].values,
            df["pre_tick_size"].values,
            tick_bps * FROM_BPS,
            100 * FROM_BPS,
        )
        * TARGET_FACTOR
    )


def calculate_tick_size(price):
    def get_precision(price):
        price_str = f"{price:.10f}".rstrip("0").rstrip(".")
        if "." in price_str:
            return len(price_str.split(".")[1])
        return 0

    tick_size = 10 ** -get_precision(price)
    return tick_size
