from typing import Any

import numpy as np
import pandas as pd
from numpy import dtype, float64, int64, ndarray

from foba_backtest_engine.analysis_utils.feature_functions.decayed_returns import (
    decayed_returns,
)


def vwap_over_ref_price(
    timestamps: ndarray[Any, dtype[int64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    trigger_direction: ndarray[Any, dtype[float64]],
    price_value: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    interval: float,
) -> ndarray[Any, dtype[float64]]:
    ld_trigger_boolean = ld_trigger_boolean.astype(int)
    volume = ld_trigger_boolean * trigger_units
    cdelta = ld_trigger_boolean * trigger_units * price_value
    volume_df = pd.DataFrame(
        {"volume": volume, "cdelta": cdelta, "timeindex": timestamps}
    )
    volume_df.index = pd.to_datetime(volume_df.timeindex, unit="ns")
    volume_df.drop(columns="timeindex", inplace=True)

    vwap = (
        volume_df.cdelta.rolling(window=pd.Timedelta(seconds=interval)).sum()
        / volume_df.volume.rolling(window=pd.Timedelta(seconds=interval)).sum()
    )
    vwap - price_value
    directionalised_rv = vwap * trigger_direction
    return directionalised_rv


def trend_strength(
    timestamps: ndarray[Any, dtype[int64]],
    trigger_direction: ndarray[Any, dtype[float64]],
    price_value: ndarray[Any, dtype[float64]],
    interval: float,
) -> ndarray[Any, dtype[float64]]:
    """
    It takes net price move over time window and divides it by the TOTAL DISTANCE traveled by price over that time window.
    Directionalises it for trigger direction.
    """
    price_df = pd.DataFrame({"price": price_value, "timeindex": timestamps})
    price_df.index = pd.to_datetime(price_df.timeindex, unit="ns")
    price_df.drop(columns="timeindex", inplace=True)
    trend = price_df.price.rolling(window=pd.Timedelta(seconds=interval)).apply(
        lambda arr: 0
        if (np.nansum(np.abs(np.diff(arr)))) == 0
        else (arr[-1] - arr[0]) / (np.nansum(np.abs(np.diff(arr)))),
        raw=True,
        engine="numba",
    )

    return trend * trigger_direction


def price_volume_corr_directionalised(
    timestamps: ndarray[Any, dtype[int64]],
    trigger_direction: ndarray[Any, dtype[float64]],
    trigger_units: ndarray[Any, dtype[float64]],
    ld_trigger_boolean: ndarray[Any, dtype[bool]],
    price_value: ndarray[Any, dtype[float64]],
    dr_halflife: float,
    interval: float,
) -> ndarray[Any, dtype[float64]]:
    """
    It takes net price move over time window and divides it by the TOTAL DISTANCE traveled by price over that time window.
    Directionalises it for trigger direction.
    """

    ld_trigger_boolean = ld_trigger_boolean.astype(int)
    trigger_lds_with_direction = trigger_units * trigger_direction * ld_trigger_boolean

    price_prev_row = np.roll(price_value, 1)
    price_prev_row[0] = price_value[0]

    dr = decayed_returns(
        timestamps, trigger_direction, price_prev_row, price_value, dr_halflife
    )
    combined_df = pd.DataFrame(
        {
            "ld_volume_directionalised": trigger_lds_with_direction,
            "dr": dr,
            "timeindex": timestamps,
        }
    )
    ld_df = combined_df.query("ld_volume_directionalised != 0")

    ld_df["implied_correlation"] = (
        ld_df["ld_volume_directionalised"]
        .rolling(window=pd.Timedelta(seconds=interval))
        .corr(ld_df["dr"])
    )
    merged_df = pd.merge(
        combined_df,
        ld_df[["timeindex", "implied_correlation"]],
        on="timeindex",
        how="left",
    )
    merged_df["implied_correlation"] = merged_df["implied_correlation"].fillna(
        method="ffill"
    )
    del ld_df
    del combined_df
    implied_correlation = merged_df["implied_correlation"].values

    return implied_correlation
