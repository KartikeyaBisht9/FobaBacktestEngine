from typing import Any, Literal, TypeVar, cast
TZ_STR = "Asia/Hong_Kong"
from datetime import time as Time
from pandas import DataFrame, Series, DatetimeIndex
from numpy import dtype, int64, ndarray, float64, array, where, maximum, tile
from numba import guvectorize, float64, njit
import numpy as np



RDVWAP_HALFLIVES = (1, 2, 4, 8, 15, 30, 60, 120, 240, 480, 900, 1800, 3600)

"""
np version for those who dont like gu-vectorize
"""

def calculate_delayed_rws_ex_lunch(
    timestamp: ndarray[Any, dtype[int64]],
    rws: ndarray[Any, dtype[float64]],
    exclude_lunch: bool = True,
) -> DataFrame:
    # ndarray.__mul__ fix
    rws_periods_array: ndarray[Any, dtype[int64]] = (
        array(RDVWAP_HALFLIVES) * 1_000_000_000
    )

    HOUR_TO_NS = 60 * 60 * 1_000_000_000

    times = Series(DatetimeIndex(timestamp, tz=TZ_STR)).dt.time
    timestamp_adjusted = timestamp + where(
        exclude_lunch
        & cast("ndarray[Any, dtype[int64]]", (times > Time(12, 0, 1)).values),
        -HOUR_TO_NS,
        0,
    )

    idx = maximum(
        timestamp_adjusted.searchsorted(
            tile(timestamp_adjusted, (len(RDVWAP_HALFLIVES), 1)) + rws_periods_array.reshape((-1, 1)),  # type: ignore
        )
        - 1,
        0,
    )

    return DataFrame(
        {f"rws_{period}": rws[idx[i]] for i, period in enumerate(RDVWAP_HALFLIVES)}
    )


@njit
def adjust_for_lunch_inplace(timestamp: np.ndarray, exclude_lunch: bool) -> None:
    """
    Adjusts timestamps in-place by subtracting one hour for times after 12:00:01 PM.

    Parameters:
    - timestamp (np.ndarray): 1D array of timestamps in nanoseconds since epoch.
    - exclude_lunch (bool): If True, adjust timestamps after lunch.

    Returns:
    - None: The input array is modified in-place.
    """
    if not exclude_lunch:
        return

    n = timestamp.shape[0]
    HOUR_NS = 3600 * 1_000_000_000
    LUNCH_TIME_NS = (12 * 3600 + 0 * 60 + 1) * 1_000_000_000
    DAY_NS = 24 * HOUR_NS

    for i in range(n):
        ts = timestamp[i]
        time_since_midnight = ts % DAY_NS
        if time_since_midnight > LUNCH_TIME_NS:
            timestamp[i] = ts - HOUR_NS


@guvectorize(
    ['void(float64[:], float64[:], float64[:], float64[:], float64[:], float64[:,:])'],
    '(n),(m),(m),(m),(S),(n,J)',
    target='parallel'
)
def calculate_delayed_rws_midspot(
    trade_times,
    exchange_times,
    midspots,
    rws,
    intervals,
    result
):
    n = trade_times.shape[0]
    m = exchange_times.shape[0]
    S = intervals.shape[0]
    J = 2 * S 

    """
    Iterate over each time interval (S) and for each timestamp
    """

    for p in range(S):                                      
        current_position = 0    
        interval_ns = intervals[p] * 1_000_000_000      
        for i in range(n):      
            cutoff_time = trade_times[i] + interval_ns

            if i > 0 and trade_times[i] == trade_times[i-1]:
                result[i, p] = result[i - 1, p]
                result[i, p + S] = result[i - 1, p + S]
                continue

            midspot_val = midspots[-1]  
            rws_val = rws[-1]  

            for j in range(current_position, m):
                if exchange_times[j] > cutoff_time:
                    midspot_val = midspots[j]
                    rws_val = rws[j]
                    current_position = j  
                    break

            result[i, p] = midspot_val
            result[i, p + S] = rws_val
