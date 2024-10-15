from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import _decayed_sum, broadcast_halflife, _positive_decayed_sum
from numpy import ndarray, dtype, int64, float64, errstate
from typing import Any, Literal, TypeVar, cast

def decayed_sum(
    timestamp: ndarray[Any, dtype[int64]],
    value: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    *,
    reverse: bool = False,
) -> ndarray[Any, dtype[float64]]:
    """
    timestamp.shape: [..., TIMESTAMP]
    value.shape: [..., TIMESTAMP]
    halflife.shape: [..., TIMESTAMP]
    """

    halflife = broadcast_halflife(halflife, timestamp)

    if reverse:
        return decayed_sum(
            -timestamp[..., ::-1], value[..., ::-1], halflife[..., ::-1]
        )[..., ::-1]

    with errstate(divide="ignore"):
        return _decayed_sum(timestamp, value, halflife)  # pyright: ignore


def positive_decayed_sum(
    timestamp: ndarray[Any, dtype[int64]],
    value: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    *,
    reverse: bool = False,
) -> ndarray[Any, dtype[float64]]:
    """
    timestamp.shape: [..., TIMESTAMP]
    value.shape: [..., TIMESTAMP]
    halflife.shape: [..., TIMESTAMP]
    """

    halflife = broadcast_halflife(halflife, timestamp)

    if reverse:
        return positive_decayed_sum(
            -timestamp[..., ::-1], value[..., ::-1], halflife[..., ::-1]
        )[..., ::-1]

    with errstate(divide="ignore"):
        return _positive_decayed_sum(timestamp, value, halflife)