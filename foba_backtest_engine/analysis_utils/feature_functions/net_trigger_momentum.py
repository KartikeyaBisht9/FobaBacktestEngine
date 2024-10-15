from foba_backtest_engine.analysis_utils.calc_utils.decayed_sum_module import decayed_sum
from numpy import ndarray, dtype, int64, float64, errstate
from typing import Any, Literal, TypeVar, cast
# Momentums
def net_trigger_momentum(
    timestamp: ndarray[Any, dtype[int64]],
    trigger_direction: ndarray[Any, dtype[float64] or dtype[int64]],
    trigger_boolean: ndarray[Any, dtype[float64] or dtype[int64]],
    buy_units: ndarray[Any, dtype[float64] or dtype[int64]],
    sell_units: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
) -> ndarray[Any, dtype[float64]]:
    return trigger_direction * (
        decayed_sum(timestamp, trigger_boolean * buy_units, halflife)
        - decayed_sum(timestamp, trigger_boolean * sell_units, halflife)
    )
