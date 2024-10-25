from typing import Any, Literal

TZ_STR = "Asia/Hong_Kong"
from numpy import array, bool_, dtype, float64, int64, maximum, minimum, ndarray
from pandas import DataFrame

from foba_backtest_engine.analysis_utils.calc_utils.decayed_weighted_average import (
    decayed_weighted_average,
)
from foba_backtest_engine.analysis_utils.calc_utils.ufunc.backbone import ones_like

RDVWAP_HALFLIVES = (1, 2, 4, 8, 15, 30, 60, 120, 240, 480, 900, 1800, 3600)


def calculate_rdvwap(
    timestamp: ndarray[Any, dtype[int64]],
    is_last_done: ndarray[Any, dtype[bool_]],
    price: ndarray[Any, dtype[float64]],
    volume: ndarray[Any, dtype[int64]],
    suffix: str = "",
) -> DataFrame:
    """
    For EACH trade that occurs ... say at time t= T,
    i) we collect ALL trades that occured between T < t < END_OF_DAY
    ii) We reverse sort these trades on timestamp
    iii) We calculate the (decayed volume) weighted average price for each halflife in RDVWAP_HALFLIVES
    - Decayed Volume:  we decay the volumes as we go back in time ... trades closer to t = T are given more weight
    - Weighted Average Price: we calculate the weighted average price for each halflife in RDVWAP_HALFLIVES

    """
    # Do use exact matches
    idx = minimum(
        timestamp[is_last_done].searchsorted(timestamp + 1_000_000),
        timestamp[is_last_done].shape[0] - 1,
    )

    rdvwap = decayed_weighted_average(
        timestamp[is_last_done],
        price[is_last_done],
        volume[is_last_done],
        ones_like(timestamp[is_last_done]) * array(RDVWAP_HALFLIVES).reshape((-1, 1)),
        reverse=True,
    )

    return DataFrame(
        {
            f"rdvwap_{halflife}{suffix}": rdvwap[i, idx]
            for i, halflife in enumerate(RDVWAP_HALFLIVES)
        }
    )


def resample_dataframe(
    timestamp: ndarray[Any, dtype[int64]],
    external_dataframe: DataFrame,
    external_timestamp_column: str,
    external_columns: list[str] or dict[str, str],
    side: Literal["left", "right"] = "left",
    directionalise: ndarray[Any, dtype[int64]] or bool = False,
) -> DataFrame:
    external_timestamp = external_dataframe[external_timestamp_column].values

    external_idx = maximum(external_timestamp.searchsorted(timestamp, side=side) - 1, 0)

    dataframe_view: DataFrame = (
        external_dataframe.iloc[external_idx][list(external_columns.keys())].rename(
            columns=external_columns
        )
        if isinstance(external_columns, dict)
        else external_dataframe.iloc[external_idx][external_columns]
    )
    if isinstance(directionalise, bool) and not directionalise:
        return dataframe_view.reset_index(drop=True).copy()
    else:
        return dataframe_view.reset_index(drop=True).copy().mul(directionalise, axis=0)
