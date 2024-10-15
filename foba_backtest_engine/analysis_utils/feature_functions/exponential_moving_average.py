from foba_backtest_engine.analysis_utils.calc_utils.decayed_change import decayed_change
from numpy import ndarray, dtype, int64, float64, errstate
from typing import Any


def exponential_moving_average(
    timestamp: ndarray[Any, dtype[int64]],
    value: ndarray[Any, dtype[float64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    *,
    reverse: bool = False,
) -> ndarray[Any, dtype[float64]]:
    """
    timestamp.shape: [..., TIMESTAMP]
    value.shape: [..., TIMESTAMP]
    halflife.shape: [..., TIMESTAMP]
    """

    return value - decayed_change(timestamp, value, halflife, reverse=reverse)
