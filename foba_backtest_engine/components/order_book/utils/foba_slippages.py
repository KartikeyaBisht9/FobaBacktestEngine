from collections import namedtuple

import numpy as np
import pandas as pd

from foba_backtest_engine.analysis_utils.feature_functions.bbov import (
    raw_bbov,
    smooth_bbov,
)
from foba_backtest_engine.analysis_utils.target_function.delayed_rws import (
    adjust_for_lunch_inplace,
    calculate_delayed_rws_midspot,
)
from foba_backtest_engine.enrichment import provides
from foba_backtest_engine.utils.base_utils import ImmutableDict

"""
Any custom metric or val can be inserted here ... ive done RWS as its a decent indicator of the tick.
We could also get our XGB valuation
"""


def optiver_xgb_valuation():
    """
    xgb valuation pulled as a dataframe ... we would need to align the timestamps with the feedState_ timestamps
    """
    return np.nan


def get_slipped_fields(
    df,
    bookId,
    exclude_lunch=True,
    pnl_slippage_times=[5, 15, 30, 60, 120, 240, 300, 600, 900, 1800, 3600, 7200],
    bbov_weights=[1, 6, 6, 1],
    smooth_bbov_alpha=0.10,
    bbov_interval_s=10,
):
    createdNanos_, received_, timestamp_ = (
        df["createdNanos_"].values,
        df["received_"].values,
        df["timestamp_"].values,
    )
    if exclude_lunch:
        for timestamp in [createdNanos_, received_, timestamp_]:
            adjust_for_lunch_inplace(timestamp=timestamp, exclude_lunch=True)
    else:
        pass

    midspot, rws, pnl_slippage_times = (
        df["midspot"].values,
        df["rws"].values,
        np.array(pnl_slippage_times),
    )
    result_shape = (len(rws), 2 * len(pnl_slippage_times))
    result = np.empty(result_shape, dtype=np.float64)

    calculate_delayed_rws_midspot(
        createdNanos_, createdNanos_, midspot, rws, pnl_slippage_times, result
    )

    bbov_length = len(bbov_weights)
    if bbov_length % 2 != 0:
        raise ValueError("BBOV weights must be a symmetric even item list")

    weights = np.array(bbov_weights).astype(np.float64)
    volumes = df[
        [f"bids_{x}_volume_" for x in range(max(5, bbov_length / 2))]
        + [f"asks_{x}_volume_" for x in range(max(5, bbov_length / 2))]
    ].to_numpy()

    bbov = raw_bbov(volumes, weights)

    smooth = np.empty(result.shape[0])
    alphas = np.full(result.shape[0], smooth_bbov_alpha)
    sample_intervals = np.full(result.shape[0], bbov_interval_s)

    smooth_bbov(createdNanos_, bbov, alphas, smooth, sample_intervals)

    ts = createdNanos_.reshape(-1, 1)
    even_id_array = df["event_id"].astype("int").values.reshape(-1, 1)
    book_id_array = np.full((result.shape[0], 1), int(bookId))
    combined_array = np.hstack(
        (
            ts,
            book_id_array,
            even_id_array,
            bbov.reshape(-1, 1),
            smooth.reshape(-1, 1),
            result,
        )
    )

    result_df = pd.DataFrame(
        combined_array,
        columns=[
            "createdNanos_",
            "bookId_",
            "event_id_match",
            "raw_bbov",
            "smooth_bbov",
        ]
        + [f"midspot_{x}" for x in pnl_slippage_times]
        + [f"rws_{x}" for x in pnl_slippage_times],
    )
    result_df["bookId_"] = result_df["bookId_"].astype(int).astype("str")
    result_df["event_id_match"] = result_df["event_id_match"].astype("int")
    result_df[["rws", "rws_skew", "optiver_xgb_val", "midspot"]] = df[
        ["rws", "rws_skew", "optiver_xgb_val", "midspot"]
    ]

    del createdNanos_
    del received_
    del timestamp_
    del midspot
    del rws
    del pnl_slippage_times
    del result_shape
    del result

    return result_df


@provides("foba_slippages")
def annotate_slippages(
    feed_states,
    pnl_slippage_times,
    exclude_lunch=True,
    bbov_weights=[1, 6, 6, 1],
    smooth_bbov_alpha=0.10,
    bbov_interval_s=10,
):
    """
    This calculates midspot/rws/rdvwap_{x}s for x in [1s,2s,30s,...etc]

    i) First we enrich the feed-states to have rws, midspot
    ii) We then use a guvectorized method to efficiently iterate through the rows and using a type of searchsorted algorithm annotate the correct future price
    iii) We then attach these to feed_states
    """
    event_ids = []
    enriched = []
    for event_id, event in feed_states.items():
        enriched.append(event._asdict())
        event_ids.append(event_id)

    enriched_df = pd.DataFrame(enriched)
    enriched_df["rws"] = (
        enriched_df.bids_0_volume_ * enriched_df.asks_0_price_
        + enriched_df.asks_0_volume_ * enriched_df.bids_0_price_
    ) / (enriched_df.bids_0_volume_ + enriched_df.asks_0_volume_)
    enriched_df["rws_skew"] = (enriched_df.bids_0_volume_) / (
        enriched_df.bids_0_volume_ + enriched_df.asks_0_volume_
    ) - 0.50
    enriched_df["optiver_xgb_val"] = optiver_xgb_valuation()
    enriched_df["midspot"] = (
        enriched_df.asks_0_price_ + enriched_df.bids_0_price_
    ) / 2.0
    enriched_df["event_id"] = event_ids
    enriched_df = enriched_df.sort_values("createdNanos_").reset_index(drop=True)

    book_results = []
    for book in enriched_df["bookId_"].unique():
        book_enriched = enriched_df[enriched_df["bookId_"] == book].reset_index(
            drop=True
        )
        book_future_prices = get_slipped_fields(
            df=book_enriched,
            bookId=book,
            exclude_lunch=exclude_lunch,
            pnl_slippage_times=pnl_slippage_times,
            bbov_weights=bbov_weights,
            smooth_bbov_alpha=smooth_bbov_alpha,
            bbov_interval_s=bbov_interval_s,
        )
        book_results.append(book_future_prices)
        del book_enriched
        del book_future_prices

    future_valuation = pd.concat(book_results, axis=0)
    slippage_columns = [
        c
        for c in future_valuation.columns
        if c not in ["bookId_", "event_id_match", "createdNanos_"]
    ]
    future_valuation[slippage_columns] = (
        future_valuation[slippage_columns].ffill().bfill()
    )

    FeedStateSlippages = namedtuple(
        "FeedStateSlippages",
        [c for c in future_valuation.columns if c != "event_id_match"],
    )
    future_valuation = future_valuation.set_index("event_id_match")

    def items():
        for event_id, event in feed_states.items():
            try:
                row = future_valuation.loc[event_id]
            except:
                raise AssertionError(
                    "enriched slippages should have same number of rows as feed states!"
                )
            FeedStateSlippage = FeedStateSlippages(*row)
            yield event_id, FeedStateSlippage

    return ImmutableDict(items())
