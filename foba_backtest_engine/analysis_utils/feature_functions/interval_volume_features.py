from numpy import ndarray, dtype, int64, float64
import numpy as np
from typing import Any, Literal
import pandas as pd

def cumsum_buy_sell_volume_interval_directionalised(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    trigger_direction: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval: float,
    side: Literal["buy", "sell"],

) -> ndarray[Any, dtype[float64]]:
    ld_trigger_boolean = ld_trigger_boolean.astype(int)
    if side == 'buy':
        buy_triggers = np.maximum(trigger_direction, 0)
        volume = ld_trigger_boolean*buy_triggers*trigger_units

    elif side == 'sell':
        sell_triggers = np.minimum(trigger_direction,0)
        volume = ld_trigger_boolean*sell_triggers*trigger_units

    volume_df = pd.DataFrame({'volume': volume, 'timeindex': timestamps})
    volume_df.index = pd.to_datetime(volume_df.timeindex, unit='ns')
    volume_df.drop(columns='timeindex', inplace=True)

    rolling_wd_volume = volume_df.volume.rolling(window=pd.Timedelta(seconds=interval)).sum()

    if side == 'buy':
        direc_rolling_wd_volume = rolling_wd_volume * trigger_direction
    else:
        direc_rolling_wd_volume = rolling_wd_volume * trigger_direction*-1

    return direc_rolling_wd_volume


def flow_of_larger_orders_in_interval(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    mid_spot : ndarray[Any, dtype[float64]],
    trigger_direction: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval: float,
    side: Literal["buy", "sell", "net"],
    cash_delta_threshold: float,

) -> ndarray[Any, dtype[float64]]:

    ld_trigger_boolean=ld_trigger_boolean.astype(int)

    if side == 'buy':
        buy_triggers = np.maximum(trigger_direction, 0)
        volume = ld_trigger_boolean * buy_triggers * trigger_units
        cdelta = volume * mid_spot

    elif side == 'sell':
        sell_triggers = np.minimum(trigger_direction, 0)
        volume = ld_trigger_boolean * sell_triggers * trigger_units
        cdelta = volume * mid_spot

    elif side == 'net':
        volume = ld_trigger_boolean * trigger_direction * trigger_units
        cdelta = np.abs(volume * mid_spot)

    cdelta_index = np.where(cdelta > cash_delta_threshold, 1, 0)
    largevolume = np.where(cdelta_index == 1, volume, 0)

    largevolume_df = pd.DataFrame({'largevolume': largevolume, 'timeindex': timestamps})
    largevolume_df.index = pd.to_datetime(largevolume_df.timeindex, unit='ns')
    largevolume_df.drop(columns='timeindex', inplace=True)
    rolling_wd_largevolume = largevolume_df.largevolume.rolling(window=pd.Timedelta(seconds=interval)).sum()

    return rolling_wd_largevolume

def flow_of_smaller_orders_in_interval(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    mid_spot : ndarray[Any, dtype[float64]],
    trigger_direction: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval: float,
    side: Literal["buy", "sell", "net"],
    cash_delta_threshold: float,

) -> ndarray[Any, dtype[float64]]:

    ld_trigger_boolean=ld_trigger_boolean.astype(int)

    if side == 'buy':
        buy_triggers = np.maximum(trigger_direction, 0)
        volume = ld_trigger_boolean * buy_triggers * trigger_units
        cdelta = volume * mid_spot

    elif side == 'sell':
        sell_triggers = np.minimum(trigger_direction, 0)
        volume = ld_trigger_boolean * sell_triggers * trigger_units
        cdelta = volume * mid_spot

    elif side == 'net':
        volume = ld_trigger_boolean * trigger_direction * trigger_units
        cdelta = np.abs(volume * mid_spot)

    cdelta_index = np.where(cdelta < cash_delta_threshold, 1, 0)
    smallvolume = np.where(cdelta_index == 1, volume, 0)

    smallvolume_df = pd.DataFrame({'largevolume': smallvolume, 'timeindex': timestamps})
    smallvolume_df.index = pd.to_datetime(smallvolume_df.timeindex, unit='ns')
    smallvolume_df.drop(columns='timeindex', inplace=True)
    rolling_wd_smallvolume = smallvolume_df.largevolume.rolling(window=pd.Timedelta(seconds=interval)).sum()

    return rolling_wd_smallvolume

def level_thickness_rank(
    timestamps: ndarray[Any, dtype[int64]],
    support_units_0: ndarray[Any, dtype[float64]],
    support_units_1: ndarray[Any, dtype[float64]],
    resistance_units_0: ndarray[Any, dtype[float64]],
    resistance_units_1: ndarray[Any, dtype[float64]],
    side: Literal["support", "resistance", "bbov"],
    interval: float,
    bbov_weights: Any = [1, 6, 6, 1],
)-> ndarray[Any, dtype[float64]]:
    if side == 'support':
        total_volume_units = support_units_0 + support_units_1
    elif side == 'resistance':
        total_volume_units = resistance_units_0 + resistance_units_1
    elif side == 'bbov':
        total_volume_units = bbov_weights[0]*support_units_1 + bbov_weights[1]*support_units_0 + bbov_weights[2]*resistance_units_0 + bbov_weights[3]*resistance_units_1

    volume_df = pd.DataFrame({'volume': total_volume_units, 'timeindex': timestamps})
    volume_df.index = pd.to_datetime(volume_df.timeindex, unit='ns')
    volume_df.drop(columns='timeindex', inplace=True)
    rolling_rank = volume_df.volume.rolling(window=pd.Timedelta(seconds=interval)).rank() / \
                      volume_df.volume.rolling(window=pd.Timedelta(seconds=interval)).count()
    return rolling_rank

def buy_sell_pct_interval_directionalised(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    mid_spot : ndarray[Any, dtype[float64]],
    trigger_direction: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval: float,
    side: Literal["buy", "sell"],

) -> ndarray[Any, dtype[float64]]:
    ld_trigger_boolean = ld_trigger_boolean.astype(int)
    if side == 'buy':
        buy_triggers = np.maximum(trigger_direction, 0)
        volume = ld_trigger_boolean*buy_triggers*trigger_units

    elif side == 'sell':
        sell_triggers = np.minimum(trigger_direction,0)
        volume = ld_trigger_boolean*sell_triggers*trigger_units

    cdelta = volume*mid_spot
    cdelta_total = trigger_units*mid_spot*ld_trigger_boolean

    rolling_df = pd.DataFrame({'cdelta': cdelta, 'cdelta_total': cdelta_total, 'timeindex': timestamps})
    rolling_df.index = pd.to_datetime(rolling_df.timeindex, unit='ns')
    rolling_df.drop(columns='timeindex', inplace=True)
    rolling_volume_pct = rolling_df.cdelta.rolling(window=pd.Timedelta(seconds=interval)).sum() / \
                            rolling_df.cdelta_total.rolling(window=pd.Timedelta(seconds=interval)).sum()

    directionalised_rolling_volume_pct = rolling_volume_pct * trigger_direction if side == 'buy' else rolling_volume_pct * trigger_direction*-1

    return directionalised_rolling_volume_pct

def turnover_impulse(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    mid_spot : ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval1: float,
    interval2: float,
    mode: Literal["mean", "median", "sum"],
) -> ndarray[Any, dtype[float64]]:

    assert (interval1 < interval2), "interval1 must be less than interval2"

    ld_trigger_boolean = ld_trigger_boolean.astype(int)
    cdelta = trigger_units*mid_spot*ld_trigger_boolean
    cdelta_df = pd.DataFrame({'cdelta': cdelta, 'timeindex': timestamps})
    cdelta_df.index = pd.to_datetime(cdelta_df.timeindex, unit='ns')
    cdelta_df.drop(columns='timeindex', inplace=True)

    if mode == 'mean':
        rolling_cdelta = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval1)).mean()
        rolling_cdelta2 = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval2)).mean()
    elif mode == 'median':
        rolling_cdelta = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval1)).median()
        rolling_cdelta2 = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval2)).median()
    elif mode == 'sum':
        rolling_cdelta = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval1)).sum()
        rolling_cdelta2 = cdelta_df.cdelta.rolling(window=pd.Timedelta(seconds=interval2)).sum()
    return rolling_cdelta / rolling_cdelta2

