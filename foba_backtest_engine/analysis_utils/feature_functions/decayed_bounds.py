from typing import Any

from numpy import dtype, float64, int64, ndarray

from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import (
    broadcast_halflife,
    max_bound_calculator,
    min_bound_calculator,
)


# Decayed Bounds (different to dhr/dlr)
def decayed_bound_calculator(
    timestamp: ndarray[Any, dtype[int64]],
    trigger_direction: ndarray[Any, dtype[float64] or dtype[int64]],
    rws: ndarray[Any, dtype[float64] or dtype[int64]],
    bid_price: ndarray[Any, dtype[float64] or dtype[int64]],
    ask_price: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
    dhr_type: bool,
) -> ndarray[Any, dtype[float64]]:
    halflife = broadcast_halflife(halflife, timestamp)
    max_bound = max_bound_calculator(timestamp, ask_price, rws, halflife)
    min_bound = min_bound_calculator(timestamp, bid_price, rws, halflife)
    dhr = trigger_direction * (max_bound - rws)
    dlr = trigger_direction * (min_bound - rws)
    if dhr_type:
        return max_bound, dhr
    else:
        return min_bound, dlr
