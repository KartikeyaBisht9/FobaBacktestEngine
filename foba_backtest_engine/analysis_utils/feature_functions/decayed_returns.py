from foba_backtest_engine.analysis_utils.calc_utils.decayed_sum_module import decayed_sum
from numpy import ndarray, dtype, int64, float64,log2, errstate
from typing import Any, Literal, TypeVar, cast

# Decayed Returns
def decayed_returns(
    timestamp: ndarray[Any, dtype[int64]],
    trigger_direction: ndarray[Any, dtype[float64] or dtype[int64]],
    mid_spot_prev: ndarray[Any, dtype[float64] or dtype[int64]],
    mid_spot_now: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
) -> ndarray[Any, dtype[float64]]:
    log_returns = log2(mid_spot_now / mid_spot_prev)
    return trigger_direction * (decayed_sum(timestamp, log_returns, halflife))
