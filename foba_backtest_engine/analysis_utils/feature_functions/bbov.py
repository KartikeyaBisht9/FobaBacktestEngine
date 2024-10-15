from typing import Any, Literal, TypeVar, cast
TZ_STR = "Asia/Hong_Kong"
from datetime import time as Time
from pandas import DataFrame, Series, DatetimeIndex
from numpy import dtype, int64, float64, ndarray, array, where, maximum, tile
from numba import guvectorize, float64, njit
import numpy as np

@njit
def raw_bbov(volumes, weights):
    n, m = volumes.shape
    result = np.empty(n)
    weight_total = np.sum(weights)
    for i in range(n):
        temp_volumes = np.sort(volumes[i, :])[::-1] 
        weighted_sum = 0.0
        for j in range(m):
            weighted_sum += temp_volumes[j] * weights[j]
        result[i] = weighted_sum / weight_total
    return result

"""
EWMA(x, t) = (1-a)EWMA(x, t-1) + a * (x,t)

If sample_interval = 10s, alpha = 0.1 then 0.1 = 1 - np.exp(-10000000000)

"""
@guvectorize(
    ['void(float64[:], float64[:], float64[:], float64[:], float64[:])'],
    '(n),(n),(n),(n),(n)',
    target='parallel'
)

def smooth_bbov(timestamps, bbov_values, alpha_array, result, sample_interval):
    n = timestamps.shape[0]
    result[0] = bbov_values[0] if not np.isnan(bbov_values[0]) else 0.0

    last_sample_time = timestamps[0]
    for i in range(1, n):
        if np.isnan(bbov_values[i]):
            if i == 0:
                result[i] = 0
            else:
                result[i] = result[i-1]
            last_sample_time = timestamps[i]
        else:
            alpha = alpha_array[i]  
            dt = timestamps[i] - last_sample_time
            if dt >= sample_interval[i]*1_000_000_00: 
                result[i] = result[i - 1] * (1 - alpha) + bbov_values[i] * alpha
                last_sample_time = timestamps[i]
            else:
                result[i] = result[i - 1]



