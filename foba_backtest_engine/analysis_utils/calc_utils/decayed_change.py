from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import _change
from foba_backtest_engine.analysis_utils.calc_utils.decayed_sum_module import decayed_sum, positive_decayed_sum
from numpy import ndarray, dtype, int64, float64, errstate
from typing import Any, Literal, TypeVar, cast


def decayed_change(
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

    return decayed_sum(
        timestamp, _change(value, halflife, reverse), halflife, reverse=reverse
    )

def positive_decayed_change(
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

    return positive_decayed_sum(
        timestamp, _change(value, halflife, reverse), halflife, reverse=reverse
    )
