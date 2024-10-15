from foba_backtest_engine.utils.base_utils import ImmutableDict, priority_process_and_dispatch, sorted_multi_dict
from foba_backtest_engine.components.order_book.utils.enums import EventType, Side
from foba_backtest_engine.enrichment import enriches, provides, id_dict
from foba_backtest_engine.components.order_book.utils.enums import Side
from collections import namedtuple, defaultdict
from operator import attrgetter
from itertools import chain
import pandas as pd
import numpy as np


@provides('pnl_enrichment')
@enriches('foba_events')
def enrich_pnl(foba_events, full_feed_state_enrichment, static_data_enrichment, pnl_slippage_times=[5,15,30,60,120,240,300,600,900,1800,3600,7200]):
    """
    Provides an ImmutableDict of StaticDataEnrichment (see named tuple defn) which are used for FobaEvent enrichment
    :param foba_events: ImmutableDict of FobaEvents which will be enriched.
    :param static_data_info: ImmutableDict of StaticDataInfo.
    :param excluded_fee_names: configuration of fee names to exclude. may or may not be supplied.
    :param currency_rate: configuration of currency rate to use for fees. may or may not be supplied.
    """
    def items():
        for event_id, event in foba_events.items():
            feed_state = full_feed_state_enrichment[event_id]
            static_data = static_data_enrichment[event_id]

            slipped_pnl_fields = [f'aggressor_pnl_{x}s' for x in pnl_slippage_times] + [f'passive_pnl_{x}s' for x in pnl_slippage_times]
            slipped_pnl_bps_fields = [f'aggressor_bps_{x}s' for x in pnl_slippage_times] + [f'passive_bps_{x}s' for x in pnl_slippage_times]
            slipped_pnl_ticks_fields = [f'aggressor_tick_{x}s' for x in pnl_slippage_times] + [f'passive_tick_{x}s' for x in pnl_slippage_times]
            all_fields = slipped_pnl_fields + slipped_pnl_bps_fields + slipped_pnl_ticks_fields

            PnlEnrichment = namedtuple('PnlEnrichment', all_fields)
            aggressor_pnl, passive_pnl, aggressor_bps, passive_bps, aggressor_ticks, passive_ticks = [],[],[],[],[],[]
            for time in pnl_slippage_times:
                passive_pnl_ = np.where(event.event_type==EventType.TRADE,
                                     np.where(event.side==Side.BID, (getattr(feed_state, f'midspot_{time}')-event.event_price)*(event.event_volume*static_data.contract_size),
                                                (event.event_price-getattr(feed_state, f'midspot_{time}'))*(event.event_volume*static_data.contract_size))-static_data.fees, 0)
                aggro_pnl = np.where(event.event_type==EventType.TRADE,
                                     -1*np.where(event.side==Side.BID, (getattr(feed_state, f'midspot_{time}')-event.event_price)*(event.event_volume*static_data.contract_size),
                                                (event.event_price-getattr(feed_state, f'midspot_{time}'))*(event.event_volume*static_data.contract_size))-static_data.fees, 0)
                aggro_pnl_bps = 10_000 * (aggro_pnl/(event.event_volume * event.event_price))
                passive_pnl_bps = 10_000 * (passive_pnl_/(event.event_volume * event.event_price))

                aggro_pnl_ticks = 10_000 * (aggro_pnl/event.event_volume/static_data.tick_size)
                passive_pnl_ticks = 10_000 * (passive_pnl_/event.event_volume/static_data.tick_size)
                aggressor_pnl.append(aggro_pnl)
                passive_pnl.append(passive_pnl_)
                aggressor_bps.append(aggro_pnl_bps)
                passive_bps.append(passive_pnl_bps)
                aggressor_ticks.append(aggro_pnl_ticks)
                passive_ticks.append(passive_pnl_ticks)
                
            combined_fields = tuple(aggressor_pnl) + tuple(passive_pnl) + tuple(aggressor_bps) + tuple(passive_bps) + tuple(aggressor_ticks) + tuple(passive_ticks)
            output = PnlEnrichment(*combined_fields)

            yield event_id, output

    return ImmutableDict(items())

"""
Need to add:


e) Logic to do the following:
    - for a quote order ... fetch ALL foba_events where order is live ... calculate the credit, credit_bps, priority_offset, priority_adj_credit, priority_adj_credit_bps

    assume val = optiverXgb | rws
        - we want to know:  
                a) max_lifetime_credit & max_lifetime_credit_bps/ticks ++ (max_credit_time_since_join) ++ (max_credit_time_to_trade)
                b) credit_at_event ... bps/ticks
                c) min_liftetime_credit & min_liftetime_credit_bps/ticks ++ (min_credit_time_since_join) ++ (min_credit_time_since_join)

"""