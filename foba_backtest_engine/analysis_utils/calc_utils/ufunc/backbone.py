from collections.abc import Sequence
from typing import Any, Literal, TypeVar, cast
TZ_STR = "Asia/Hong_Kong"
from datetime import time as Time
import numpy as np
import pandas as pd
from numba import b1, f8, guvectorize, i8, vectorize  # type: ignore
from numpy import (
    absolute,
    arange,
    arctan2,
    array,
    bool_,
    concatenate,
    cumsum,
    diff,
    dtype,
    empty,
    errstate,
    flip,
    float32,
    float64,
    full_like,
    generic,
    indices,
    inf,
    int64,
    intp,
    isnan,
    log,
    log2,
    maximum,
    mean,
    minimum,
    nan,
    nan_to_num,
    ndarray,
    ones_like,
    pad,
    power,
    result_type,
    searchsorted,
    sign,
    sqrt,
    tile,
    where,
    zeros,
    zeros_like
)
from pandas import DataFrame, DatetimeIndex, Series, concat



DIRECTIONS = (1, -1)
DECAYED_RETURN_HALFLIVES = (1, 8, 60, 480)
RDVWAP_HALFLIVES = (1, 2, 4, 8, 15, 30, 60, 120, 240, 480, 900, 1800, 3600)

_T = TypeVar("_T", bound=generic)


def ffill(
        a: ndarray[Any, dtype[_T]], axis: int or None = None
) -> ndarray[Any, dtype[_T]]:
    """
    Return a with non-NaN values propagated forward over NaN's along axis.

    Args:
        a (array_like): input data.
        axis (int): axis along which to operate. By default, flattened input is used.

    Returns:
        a with non-NaN values propagated forward over NaN's along axis.
    """
    if axis is None:
        return ffill(a.reshape(-1), axis=0).reshape(a.shape)

    if axis < 0:
        return ffill(a, axis=a.ndim + axis)

    sparse_indices = indices(a.shape, sparse=True)

    idx = where(~isnan(a), sparse_indices[axis], 0)
    maximum.accumulate(idx, axis=axis, out=idx)
    return a[tuple(idx if ax == axis else sparse_indices[ax] for ax in range(a.ndim))]


def bfill(
        a: ndarray[Any, dtype[_T]], axis: int or None = None
) -> ndarray[Any, dtype[_T]]:
    """
    Return a with non-NaN values propagated backward over NaN's along axis.

    Args:
        a (array_like): input data.
        axis (int): axis along which to operate. By default, flattened input is used.

    Returns:
        a with non-NaN values propagated backward over NaN's along axis.
    """
    if axis is None:
        return flip(ffill(flip(a).reshape(-1), axis=0).reshape(a.shape))

    return flip(ffill(flip(a, axis), axis), axis)


def broadcast_halflife(
        halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
        timestamp: ndarray[Any, dtype[int64]],
) -> ndarray[Any, dtype[float64] or dtype[int64]]:
    if isinstance(halflife, (float, int)):
        return full_like(timestamp, halflife, dtype=float64)

    return halflife


@vectorize([b1(f8)], nopython=True)
def valid_halflife(
        halflife: float or ndarray[Any, dtype[float64]]
) -> bool or ndarray[Any, dtype[bool_]]:
    return halflife > 0.0 or halflife < 0.0


@vectorize([f8(i8, i8, f8), f8(f8, f8, f8)], nopython=True)
def decay_factor(
        old: int or float or ndarray[Any, dtype[int64] or dtype[float64]],
        new: int or float or ndarray[Any, dtype[int64] or dtype[float64]],
        halflife: float or ndarray[Any, dtype[float64]],
) -> float or ndarray[Any, dtype[float64]]:
    return 2.0 ** ((old - new) / halflife)


@vectorize([f8(i8, i8, f8), f8(f8, f8, f8)], nopython=True)
def cleaned_decay_factor(
        old: int or float or ndarray[Any, dtype[int64] or dtype[float64]],
        new: int or float or ndarray[Any, dtype[int64] or dtype[float64]],
        halflife: float or ndarray[Any, dtype[float64]],
) -> float or ndarray[Any, dtype[float64]]:
    return decay_factor(old, new, halflife) if valid_halflife(halflife) else 0.0


@guvectorize(
    [(i8[:], f8[:], f8[:], f8[:])],
    "(n),(n),(n)->(n)",
)
def _decayed_sum(
        timestamp: ndarray[Any, dtype[int64]],
        value: ndarray[Any, dtype[float64] or dtype[int64]],
        halflife: ndarray[Any, dtype[float64] or dtype[int64]],
        out: ndarray[Any, dtype[float64]] = ...,  # type: ignore
) -> None:
    out[0] = value[0]

    for i in range(1, timestamp.shape[0]):
        decay = cleaned_decay_factor(
            timestamp[i - 1], timestamp[i], halflife[i - 1] * 1_000_000_000.0
        )

        out[i] = decay * out[i - 1] + value[i]


@guvectorize(
    [(i8[:], f8[:], f8[:], f8[:])],
    "(n),(n),(n)->(n)",
)
def _positive_decayed_sum(
        timestamp: ndarray[Any, dtype[int64]],
        value: ndarray[Any, dtype[float64] or dtype[int64]],
        halflife: ndarray[Any, dtype[float64] or dtype[int64]],
        out: ndarray[Any, dtype[float64]] = ...,  # type: ignore
) -> None:
    out[0] = value[0]

    for i in range(1, timestamp.shape[0]):
        decay = cleaned_decay_factor(
            timestamp[i - 1], timestamp[i], halflife[i - 1] * 1_000_000_000.0
        )

        out[i] = max(decay * out[i - 1] + value[i], 0.0)


def _change(
        value: ndarray[Any, dtype[float64]],
        halflife: float or int or ndarray[Any, dtype[float64] or dtype[int64]],
        reverse: bool,
) -> ndarray[Any, dtype[float64]]:
    return (halflife != 0.0) * (
        -diff(value, append=value[..., -1:])
        if reverse
        else diff(value, prepend=value[..., :1])
    )


# Gets an [1 x n] array for the column we are requesting
def _t_get_values(data: DataFrame, key: str) -> ndarray[Any, dtype[Any]]:
    return cast("ndarray[Any, dtype[Any]]", data[key].values)


@guvectorize(
    [(i8[:], f8[:], f8[:], f8[:], f8[:])],
    "(n),(n),(n), (n) ->(n)",
)
def max_bound_calculator(
    timestamp: ndarray[Any, dtype[int64]],
    ask: ndarray[Any, dtype[float64] or dtype[int64]],
    rws: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: ndarray[Any, dtype[float64] or dtype[int64]],
    out: ndarray[Any, dtype[float64]] = ...,  # type: ignore
) -> None:
    out[0] = ask[0]

    for i in range(1, timestamp.shape[0]):
        decay = cleaned_decay_factor(
            timestamp[i - 1], timestamp[i], halflife[i - 1] * 1_000_000_000.0
        )
        decayed_bound = decay * out[i - 1] + (1 - decay) * rws[i]
        out[i] = np.max(np.array([decayed_bound, ask[i]]))


@guvectorize(
    [(i8[:], f8[:], f8[:], f8[:], f8[:])],
    "(n),(n),(n), (n) ->(n)",
)
def min_bound_calculator(
    timestamp: ndarray[Any, dtype[int64]],
    bid: ndarray[Any, dtype[float64] or dtype[int64]],
    rws: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: ndarray[Any, dtype[float64] or dtype[int64]],
    out: ndarray[Any, dtype[float64]] = ...,  # type: ignore
) -> None:
    out[0] = bid[0]

    for i in range(1, timestamp.shape[0]):
        decay = cleaned_decay_factor(
            timestamp[i - 1], timestamp[i], halflife[i - 1] * 1_000_000_000.0
        )
        decayed_bound = decay * out[i - 1] + (1 - decay) * rws[i]
        out[i] = np.min(np.array([decayed_bound, bid[i]]))


@guvectorize(
    [(i8[:], f8[:], f8[:], f8[:], f8[:])],
    "(n),(n),(n),(n)->(n)",
)
def _decayed_weighted_average(
    timestamp: ndarray[Any, dtype[int64]],
    value: ndarray[Any, dtype[float64] or dtype[int64]],
    weight: ndarray[Any, dtype[float64] or dtype[int64]],
    halflife: ndarray[Any, dtype[float64] or dtype[int64]],
    out: ndarray[Any, dtype[float64]] = ...,  # type: ignore
) -> None:
    out[0] = value[0]
    accumulated_weight = weight[0]

    for i in range(1, timestamp.shape[0]):
        if not valid_halflife(halflife[i - 1]) or accumulated_weight == 0.0:
            out[i] = value[i]
            accumulated_weight = weight[i]
            continue

        accumulated_weight *= decay_factor(
            timestamp[i - 1], timestamp[i], halflife[i - 1] * 1_000_000_000.0
        )

        out[i] = (accumulated_weight * out[i - 1] + weight[i] * value[i]) / (
            accumulated_weight + weight[i]
        )
        accumulated_weight += weight[i]

# @guvectorize(['void(float64[:], float64[:], float64[:], float64[:], float64[:], float64[:,:])'],
#              '(n),(m),(m),(m),(S),(n,J)', target='parallel')
# def calculate_midspot_rws_(trade_times, exchange_times, midspots, rws, intervals, result):
#     n = trade_times.shape[0]
#     m = exchange_times.shape[0]
#     S = intervals.shape[0]
#     J = 2 * S  # Ensure J is correctly set to 2 * S
#
#     for p in range(S):  # Iterate over each forward time interval
#         current_position = 0  # Initialize the pointer for each interval
#         for i in range(n):  # Iterate over each trade time
#             cutoff_time = trade_times[i] + intervals[p]*1_000_000_000
#             midspot_val = midspots[-1]  # Default to the last value if no valid exchange_time found
#             rws_val = rws[-1]  # Default to the last value if no valid exchange_time found
#
#             # Iterate from the current position to find the first valid exchange time
#             for j in range(current_position, m):
#                 if exchange_times[j] > cutoff_time:
#                     midspot_val = midspots[j]
#                     rws_val = rws[j]
#                     current_position = j  # Update the pointer for the next trade time
#                     break
#
#             # Assign values to the result array
#             result[i, p] = midspot_val
#             result[i, p + S] = rws_val


@guvectorize(
    ['void(float64[:], float64[:], float64[:], float64[:], float64[:], float64[:,:])'],
    '(n),(m),(m),(m),(S),(n,J)',
    target='parallel'
)
def calculate_midspot_rws(
    trade_times: ndarray[np.float64],
    exchange_times: ndarray[np.float64],
    midspots: ndarray[np.float64],
    rws: ndarray[np.float64],
    intervals: ndarray[np.float64],
    result: ndarray[np.float64]
) -> None:
    n = trade_times.shape[0]
    m = exchange_times.shape[0]
    S = intervals.shape[0]
    J = 2 * S  # Ensure J is correctly set to 2 * S

    for p in range(S):  # Iterate over each forward time interval
        current_position = 0  # Initialize the pointer for each interval
        for i in range(n):  # Iterate over each trade time
            cutoff_time = trade_times[i] + intervals[p] * 1_000_000_000  # Convert interval to nanoseconds
            midspot_val = midspots[-1]  # Default to the last value if no valid exchange_time found
            rws_val = rws[-1]  # Default to the last value if no valid exchange_time found

            # Iterate from the current position to find the first valid exchange time
            for j in range(current_position, m):
                if exchange_times[j] > cutoff_time:
                    midspot_val = midspots[j]
                    rws_val = rws[j]
                    current_position = j  # Update the pointer for the next trade time
                    break

            # Assign values to the result array
            result[i, p] = midspot_val
            result[i, p + S] = rws_val