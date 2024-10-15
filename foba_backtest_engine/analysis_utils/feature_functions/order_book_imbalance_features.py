from numpy import ndarray, dtype, int64, float64
import numpy as np
from typing import Any, Literal
import pandas as pd


def order_book_imbalance1(
    bid_0_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
) -> ndarray[Any, dtype[float64]]:

    return (bid_0_volume / ask_0_volume) - 1.

def order_book_imbalance2(
    bid_0_volume: ndarray[Any, dtype[float64]],
    bid_1_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
    ask_1_volume: ndarray[Any, dtype[float64]],
) -> ndarray[Any, dtype[float64]]:

    return ((bid_0_volume + bid_1_volume) / (ask_0_volume + ask_1_volume)) - 1.

def order_book_imbalance_interval_mean(
    timestamps: ndarray[Any, np.int64],
    bid_0_volume: ndarray[Any, np.float64],
    bid_1_volume: ndarray[Any, np.float64],
    ask_0_volume: ndarray[Any, np.float64],
    ask_1_volume: ndarray[Any, np.float64],
    type: Literal["one", "two"],
    interval: float,
):
    if type == 'one':
        OBI = order_book_imbalance1(bid_0_volume, ask_0_volume)
    elif type == 'two':
        OBI = order_book_imbalance2(bid_0_volume, bid_1_volume, ask_0_volume, ask_1_volume)
    else:
        raise NotImplementedError
    OBI_df = pd.DataFrame({'OBI': OBI, 'timeindex': timestamps})
    OBI_df = OBI_df.groupby('timeindex').agg({'OBI':'mean', 'timeindex':'mean'}).reset_index(drop = True).sort_values('timeindex')
    new_timestamps = OBI_df['timeindex'].values

    assert np.all(np.diff(new_timestamps) >= 0), "Timestamps must be in non-decreasing order."

    OBI_df['timeindex'] = pd.to_datetime(OBI_df['timeindex'], unit='ns')
    OBI_df.index = OBI_df['timeindex']
    OBI_df.drop(columns='timeindex', inplace=True)

    rolling_result = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).mean()
    rolling_df = pd.DataFrame({'timestamp':new_timestamps, 'rolling_result':rolling_result['OBI'].values})

    return rolling_df

def order_book_imbalance_interval_argmax_diff(
    timestamps: ndarray[Any, dtype[int64]],
    bid_0_volume: ndarray[Any, dtype[float64]],
    bid_1_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
    ask_1_volume: ndarray[Any, dtype[float64]],
    type: Literal["one", "two"],
    interval: float,
):
    if type == 'one':
        OBI = order_book_imbalance1(bid_0_volume, ask_0_volume)
    elif type == 'two':
        OBI = order_book_imbalance2(bid_0_volume, bid_1_volume, ask_0_volume, ask_1_volume)
    else:
        raise NotImplementedError

    OBI_df = pd.DataFrame({'OBI': OBI, 'timeindex': timestamps})
    OBI_df = OBI_df.groupby('timeindex').agg({'OBI': 'mean', 'timeindex': 'mean'}).reset_index(drop=True).sort_values(
        'timeindex')
    new_timestamps = OBI_df['timeindex'].values

    assert np.all(np.diff(new_timestamps) >= 0), "Timestamps must be in non-decreasing order."

    OBI_df['timeindex'] = pd.to_datetime(OBI_df['timeindex'], unit='ns')
    OBI_df.index = OBI_df['timeindex']
    OBI_df.drop(columns='timeindex', inplace=True)

    argmax_diff = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).apply(
        lambda arr: (arr.shape[0] - np.argmax(arr)) / float(arr.shape[0]), raw=True, engine='numba',
        engine_kwargs={'nopython': True, 'nogil': False, 'parallel': False}
    )

    rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': argmax_diff['OBI'].values})

    return rolling_df

def order_book_imbalance_interval_std(
    timestamps: ndarray[Any, dtype[int64]],
    bid_0_volume: ndarray[Any, dtype[float64]],
    bid_1_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
    ask_1_volume: ndarray[Any, dtype[float64]],
    type: Literal["one", "two"],
    interval: float,
):
    if type == 'one':
        OBI = order_book_imbalance1(bid_0_volume, ask_0_volume)
    elif type == 'two':
        OBI = order_book_imbalance2(bid_0_volume, bid_1_volume, ask_0_volume, ask_1_volume)
    else:
        raise NotImplementedError
    OBI_df = pd.DataFrame({'OBI': OBI, 'timeindex': timestamps})
    OBI_df = OBI_df.groupby('timeindex').agg({'OBI': 'mean', 'timeindex': 'mean'}).reset_index(drop=True).sort_values(
        'timeindex')
    new_timestamps = OBI_df['timeindex'].values

    assert np.all(np.diff(new_timestamps) >= 0), "Timestamps must be in non-decreasing order."

    OBI_df['timeindex'] = pd.to_datetime(OBI_df['timeindex'], unit='ns')
    OBI_df.index = OBI_df['timeindex']
    OBI_df.drop(columns='timeindex', inplace=True)

    rolling_result = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).std()

    rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': rolling_result['OBI'].values})

    return rolling_df

def order_book_imbalance_interval_skew(
    timestamps: ndarray[Any, dtype[int64]],
    bid_0_volume: ndarray[Any, dtype[float64]],
    bid_1_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
    ask_1_volume: ndarray[Any, dtype[float64]],
    type: Literal["one", "two"],
    interval: float,
):
    if type == 'one':
        OBI = order_book_imbalance1(bid_0_volume, ask_0_volume)
    elif type == 'two':
        OBI = order_book_imbalance2(bid_0_volume, bid_1_volume, ask_0_volume, ask_1_volume)
    else:
        raise NotImplementedError
    OBI_df = pd.DataFrame({'OBI': OBI, 'timeindex': timestamps})
    OBI_df = OBI_df.groupby('timeindex').agg({'OBI': 'mean', 'timeindex': 'mean'}).reset_index(drop=True).sort_values(
        'timeindex')
    new_timestamps = OBI_df['timeindex'].values

    assert np.all(np.diff(new_timestamps) >= 0), "Timestamps must be in non-decreasing order."

    OBI_df['timeindex'] = pd.to_datetime(OBI_df['timeindex'], unit='ns')
    OBI_df.index = OBI_df['timeindex']
    OBI_df.drop(columns='timeindex', inplace=True)

    rolling_result = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).skew()

    rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': rolling_result['OBI'].values})

    return rolling_df

def order_book_imbalance_interval_std_direction(
    timestamps: ndarray[Any, dtype[int64]],
    bid_0_volume: ndarray[Any, dtype[float64]],
    bid_1_volume: ndarray[Any, dtype[float64]],
    ask_0_volume: ndarray[Any, dtype[float64]],
    ask_1_volume: ndarray[Any, dtype[float64]],
    type: Literal["one", "two"],
    interval: float,
    direction: Literal["positive", "negative"],
    threshold: Literal["by_sign", "by_mean", "by_median", "by_mask"],
    thr: float = 0,
):
    if type == 'one':
        OBI = order_book_imbalance1(bid_0_volume, ask_0_volume)
    elif type == 'two':
        OBI = order_book_imbalance2(bid_0_volume, bid_1_volume, ask_0_volume, ask_1_volume)
    else:
        raise NotImplementedError
    OBI_df = pd.DataFrame({'OBI': OBI, 'timeindex': timestamps})
    OBI_df = OBI_df.groupby('timeindex').agg({'OBI': 'mean', 'timeindex': 'mean'}).reset_index(drop=True).sort_values(
        'timeindex')
    new_timestamps = OBI_df['timeindex'].values

    assert np.all(np.diff(new_timestamps) >= 0), "Timestamps must be in non-decreasing order."

    OBI_df['timeindex'] = pd.to_datetime(OBI_df['timeindex'], unit='ns')
    OBI_df.index = OBI_df['timeindex']
    OBI_df.drop(columns='timeindex', inplace=True)

    if threshold == "by_sign":
        if direction == 'positive':
            OBI_df['OBI'] = OBI_df['OBI'].apply(lambda x: x if x > thr else np.nan)
        elif direction == 'negative':
            OBI_df['OBI'] = OBI_df['OBI'].apply(lambda x: x if x < thr else np.nan)
        rolling_std = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).std().fillna(0)

        rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': rolling_std['OBI'].values})

        return rolling_df

    elif threshold == "by_mean":
        rolling_std = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).apply(
            lambda arr: np.nanstd(arr[arr > np.nanmean(arr)]) if direction == "positive" else np.nanstd(arr[arr < np.nanmean(arr)]),
            raw=True, engine='numba',
        ).fillna(0)
        rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': rolling_std['OBI'].values})

        return rolling_df

    elif threshold == "by_median":
        rolling_std = OBI_df.rolling(window=pd.Timedelta(seconds=interval)).apply(
            lambda arr: np.nanstd(arr[arr > np.nanmedian(arr)]) if direction == "positive" else np.nanstd(arr[arr < np.nanmedian(arr)]),
            raw=True, engine='numba',
        ).fillna(0)
        rolling_df = pd.DataFrame({'timestamp': new_timestamps, 'rolling_result': rolling_std['OBI'].values})

        return rolling_df

    elif threshold == "by_mask":
        raise NotImplementedError

def order_book_shape_ask(
        timestamps: ndarray[Any, dtype[int64]],
        ask_0_volume: ndarray[Any, dtype[float64]],
        ask_1_volume: ndarray[Any, dtype[float64]],
        interval: int or float,
):
    imbalance = ask_0_volume / ask_1_volume
    return imbalance

def order_book_shape_bid(
        timestamps: ndarray[Any, dtype[int64]],
        bid_0_volume: ndarray[Any, dtype[float64]],
        bid_1_volume: ndarray[Any, dtype[float64]],
        interval: int or float,
):
    imbalance = bid_0_volume / bid_1_volume
    return imbalance

