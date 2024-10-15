from foba_backtest_engine.components.order_book.utils.enums import EventType, Side
from foba_backtest_engine.utils.base_utils import ImmutableDict, ImmutableRecord
from foba_backtest_engine.enrichment import provides, enriches
from collections import namedtuple
from numba import njit
import pandas as pd
import numpy as np


@njit
def compute_credits(midspots, rws, optiver_xgb_val, event_price, multiplier):
    midspot_credits = multiplier * (midspots - event_price)
    rws_credits = multiplier * (rws - event_price)
    xgb_val_credits = multiplier * (optiver_xgb_val - event_price)
    return midspot_credits, rws_credits, xgb_val_credits

EventEnrichment = namedtuple('EventEnrichment', (
    'bookId_',
    'max_lifetime_midspot_credit',
    'min_lifetime_midspot_credit',
    'max_lifetime_midspot_credit_bps',
    'min_lifetime_midspot_credit_bps',
    'max_lifetime_midspot_credit_ticks',
    'min_lifetime_midspot_credit_ticks',
    'max_lifetime_rws_credit',
    'min_lifetime_rws_credit',
    'max_lifetime_rws_credit_bps',
    'min_lifetime_rws_credit_bps',
    'max_lifetime_rws_credit_ticks',
    'min_lifetime_rws_credit_ticks',
    'max_lifetime_xgb_credit',
    'min_lifetime_xgb_credit',
    'max_lifetime_xgb_credit_bps',
    'min_lifetime_xgb_credit_bps',
    'max_lifetime_xgb_credit_ticks',
    'min_lifetime_xgb_credit_ticks',
    'credit_midspot_at_event',
    'credit_midspot_at_event_bps',
    'credit_midspot_at_event_ticks',
    'credit_rws_at_event',
    'credit_rws_at_event_bps',
    'credit_rws_at_event_ticks',
    'credit_xgb_at_event',
    'credit_xgb_at_event_bps',
    'credit_xgb_at_event_ticks'
))

def fetch_optiver_val(length):
    return np.full((length,1), np.nan)

@provides('event_enrichment')
@enriches('foba_events')
def event_enricher(foba_events, filter, feed_states, full_feed_state_enrichment, send_times, static_data_enrichment):
    """
    For the event
        - Find join time (min) & eventTime (max)
        - credit against rws, midspot & optiver_xgb in that time period:
                --> get max_lifetime_credit ... + bps/ticks
                --> get min_liftetime_credit ... + bps/ticks
        
        - find credit at event ... + bps/ticks
    """
    feed_state_df = pd.DataFrame(
        [record._asdict() for event_id, record in feed_states.items()]
    ).set_index(["bookId_", "received_"]).sort_index()

    feed_state_df['midspot'] = 0.5 * (feed_state_df['bids_0_price_'] + feed_state_df['asks_0_price_'])
    feed_state_df['rws'] = (
        feed_state_df['bids_0_price_'] * feed_state_df['asks_0_volume_'] +
        feed_state_df['asks_0_price_'] * feed_state_df['bids_0_volume_']
    ) / (feed_state_df['bids_0_volume_'] + feed_state_df['asks_0_volume_'])
    feed_state_df['optiver_xgb_val'] = fetch_optiver_val(len(feed_state_df))



    start = filter.end_time.replace(hour=9, minute=30).float_timestamp*1e9

    def enriched_events():
        for event_id, event in foba_events.items():
            static_data = static_data_enrichment[event_id]
            sent_time = send_times[event_id]
            feed = full_feed_state_enrichment[event_id]

            bookId, event_price, side, avg_join_sent_time, avg_event_sent_time, tick_size = event.book_id, event.event_price, event.side, sent_time.avg_join_sent_time, sent_time.avg_event_sent_time, static_data.tick_size
            
            """
            To not get nonsense values ... we restrict the search time period to 9:30 - 16:00
            """
            try:
                subset = feed_state_df.loc[bookId].loc[max(avg_join_sent_time, start):avg_event_sent_time]
            except: # Deals w/ no key present issues
                yield event_id, EventEnrichment(
                    bookId,
                    *(np.nan for _ in range(27))
                )
                continue

            if (len(subset) == 0):
                yield event_id, EventEnrichment(
                    bookId,
                    *(np.nan for _ in range(27))
                )
                continue

            midspots, rws, optiver_xgb_val = subset["midspot"].values, subset["rws"].values, subset["optiver_xgb_val"].values
            multiplier = 1 if side == Side.BID else -1

            midspot_credits, rws_credits, xgb_val_credits = compute_credits(
                midspots, 
                rws,
                optiver_xgb_val, 
                event_price, 
                multiplier
            )

            credit_midspot_at_event = multiplier * (feed.midspot - event_price)
            credit_rws_at_event = multiplier * (feed.rws - event_price)
            credit_xgb_at_event = multiplier * (feed.optiver_xgb_val - event_price)

            output = EventEnrichment(
                bookId, 
                np.max(midspot_credits), np.min(midspot_credits), 10000*np.max(midspot_credits)/event_price, 10000*np.min(midspot_credits)/event_price, np.max(midspot_credits)/tick_size, np.min(midspot_credits)/tick_size,
                np.max(rws_credits), np.min(rws_credits), 10000*np.max(rws_credits)/event_price, 10000*np.min(rws_credits)/event_price, np.max(rws_credits)/tick_size, np.min(rws_credits)/tick_size,
                np.max(xgb_val_credits), np.min(xgb_val_credits), 10000*np.max(xgb_val_credits)/event_price, 10000*np.min(xgb_val_credits)/event_price, np.max(xgb_val_credits)/tick_size, np.min(xgb_val_credits)/tick_size,
                credit_midspot_at_event, 10000*credit_midspot_at_event/event_price, credit_midspot_at_event/tick_size,
                credit_rws_at_event, 10000*credit_rws_at_event/event_price, credit_rws_at_event/tick_size,
                credit_xgb_at_event, 10000*credit_xgb_at_event/event_price, credit_xgb_at_event/tick_size
            )
            del subset
            yield event_id, output
    
    return ImmutableDict(enriched_events())







            


