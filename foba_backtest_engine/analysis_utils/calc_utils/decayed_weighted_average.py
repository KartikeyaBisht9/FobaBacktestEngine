from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import _change, full_like, _decayed_weighted_average
from numpy import ndarray, dtype, int64, float64, errstate
from typing import Any, Literal, TypeVar, cast

def decayed_weighted_average(
    timestamp: ndarray[Any, dtype[int64]],
    value: ndarray[Any, dtype[float64] or dtype[int64]],
    weight: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    *,
    reverse: bool = False,
) -> ndarray[Any, dtype[float64]]:
    """
    timestamp.shape: [..., TIMESTAMP]
    value.shape: [..., TIMESTAMP]
    weight.shape: [..., TIMESTAMP]
    halflife.shape: [..., TIMESTAMP]
    """

    if isinstance(weight, (float, int)):
        weight = cast(
            "ndarray[Any, dtype[float64]]", full_like(timestamp, weight, dtype=float64)
        )

    if isinstance(halflife, (float, int)):
        halflife = cast(
            "ndarray[Any, dtype[float64]]",
            full_like(timestamp, halflife, dtype=float64),
        )

    if reverse:
        return decayed_weighted_average(
            -timestamp[..., ::-1],
            value[..., ::-1],
            weight[..., ::-1],
            halflife[..., ::-1],
        )[..., ::-1]

    return _decayed_weighted_average(  # pyright: ignore
        timestamp, value, weight, halflife
    )
