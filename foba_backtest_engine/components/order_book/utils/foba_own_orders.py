from foba_backtest_engine.data.S3.S3OptiverResearchActions import OPTIVER_BUCKET_ACTIONS
from foba_backtest_engine.enrichment import provides, id_dict, enriches
from foba_backtest_engine.utils.base_utils import ImmutableDict, get_logger, ImmutableRecord, multi_dict
from foba_backtest_engine.components.order_book.utils.enums import Side
from collections import namedtuple, defaultdict
from operator import attrgetter
from itertools import chain
import pandas as pd
import numpy as np

VolumeAtSent = namedtuple('VolumeAtSent', (
    'volume_ahead_at_max_join_sent'
))

OrderCountStateAtJoin = namedtuple('OrderCountStateAtJoin', ('bids_0_count_at_join_',
                                                             'bids_0_max_volume_at_join_',
                                                             'asks_0_count_at_join_',
                                                             'asks_0_max_volume_at_join_',
                                                             'bids_1_count_at_join_',
                                                             'bids_1_max_volume_at_join_',
                                                             'asks_1_count_at_join_',
                                                             'asks_1_max_volume_at_join_',
                                                             'bids_2_count_at_join_',
                                                             'bids_2_max_volume_at_join_',
                                                             'asks_2_count_at_join_',
                                                             'asks_2_max_volume_at_join_',
                                                             'bids_3_count_at_join_',
                                                             'bids_3_max_volume_at_join_',
                                                             'asks_3_count_at_join_',
                                                             'asks_3_max_volume_at_join_',
                                                             'bids_4_count_at_join_',
                                                             'bids_4_max_volume_at_join_',
                                                             'asks_4_count_at_join_',
                                                             'asks_4_max_volume_at_join_',))

OrderCountStateAtEvent = namedtuple('OrderCountStateAtEvent', ('bids_0_count_at_event_',
                                                               'bids_0_max_volume_at_event_',
                                                               'asks_0_count_at_event_',
                                                               'asks_0_max_volume_at_event_',
                                                               'bids_1_count_at_event_',
                                                               'bids_1_max_volume_at_event_',
                                                               'asks_1_count_at_event_',
                                                               'asks_1_max_volume_at_event_',
                                                               'bids_2_count_at_event_',
                                                               'bids_2_max_volume_at_event_',
                                                               'asks_2_count_at_event_',
                                                               'asks_2_max_volume_at_event_',
                                                               'bids_3_count_at_event_',
                                                               'bids_3_max_volume_at_event_',
                                                               'asks_3_count_at_event_',
                                                               'asks_3_max_volume_at_event_',
                                                               'bids_4_count_at_event_',
                                                               'bids_4_max_volume_at_event_',
                                                               'asks_4_count_at_event_',
                                                               'asks_4_max_volume_at_event_'))


EnrichmentHelper = namedtuple('EnrichmentHelper', (
    'original_key',
    'group_key',
    'sort_key',
    'payload',
))

OptiverOrderMatch = namedtuple(
        'OptiverOrderMatch', (
            'optiver_order',
            'optiver_aggressor',
            'optiver_trigger_received',
            'optiver_auto_trader',
            'optiver_trigger_book_id',
            'optiver_trigger_book_type',
            'optiver_trigger_type',
            'optiver_sent',                     # send timestamp (ns)
            'volume_ahead_at_optiver_sent',
            'optiver_valuation',                # adjusted
            'optiver_theoretical_price',        # unadjusted
            'optiver_fixed_offset',             # offset
            'optiver_variable_offset',
            'optiver_credit',
            'optiver_credit_bps',   
            'optiver_adjustment',
            'optiver_position_reducing',        # boolean
            'optiver_adjustment_triggered',
            'optiver_send_delay',               
            'optiver_potentially_picked_off'
        )
)
# Note the send delay & pickoffs are filled later by matching logic. 

OptiverPickOffs = namedtuple(
        'OptiverPickOffs', (
            'optiver_pick_off',     # Boolean
            'optiver_pick_off_trigger_received',
            'optiver_pick_off_trigger_book_id',
            'optiver_pick_off_trigger_book_type',
            'optiver_pick_off_trigger_type',            
            'optiver_delete_sent',                     # send timestamp (ns)
            'volume_ahead_at_optiver_delete_sent',
        )
)

OptiverDeleteMatch = namedtuple(
        'OptiverDeleteMatch', (
            'optiver_delete_trigger_received',
            'optiver_delete_trigger_book_id',
            'optiver_delete_trigger_book_type',
            'optiver_delete_trigger_type',
            'optiver_delete_sent',
            'volume_ahead_at_optiver_delete_sent',
            'optiver_delete_valuation',
            'optiver_delete_theoretical_price',
            'optiver_delete_credit',
            'optiver_delete_credit_bps',
            'optiver_delete_adjustment',
            'optiver_delete_adjustment_triggered',
            'optiver_delete_event_type',
        )
)

OptiverHits = namedtuple(
    'OptiverHits', (
            'optiver_hit',
            'optiver_hit_trigger_received',
            'optiver_hit_auto_trader',
            'optiver_hit_trigger_book_id',
            'optiver_hit_trigger_book_type',
            'optiver_hit_trigger_type',
            'optiver_hit_valuation',                # adjusted
            'optiver_hit_theoretical_price',        # unadjusted
            'optiver_hit_fixed_offset',             # offset
            'optiver_hit_variable_offset',
            'optiver_hit_credit',
            'optiver_hit_credit_bps',   
            'optiver_hit_adjustment',
            'optiver_hit_position_reducing',        
            'optiver_hit_adjustment_triggered',
    )
)

OptiverPerformance = namedtuple(
    'OptiverPerformance', (
            'analysis_type',                     # QUOTE or TRADE
            'optiver_performance_evaluation'
    )
)


# TODO : need to make the following based on optiver's order data

"""
The "ORDER" i reference below is just an immutable record with the attributes:
    - bookId, timestampNanos_ (received, exchange, fpga_send)
    - triggerType, orderId, price, volume,
    - bidPrice0, askPrice0, bidVolume0, askVolume0 (top bid/ask price|volume at the time ... needed later for BQ matching)
    - eventType
    - valuation, offset, adjustment (pricing)
"""


@provides('optiver_order_numbers')
def order_numbers_filtered(filter, book_ids, type = 'list'):
    """
    Not yet developed ... need to make this using optiver's order

    Function
        - This returns all our orderId. The key thing is to include every TYPE of order
            -- create can become quotes | full fill | partial fill | miss
            -- deletes can become pick offs | deleted
            -- can also have updates (modify)
            -- we can also have regular trades

    Returns:        A SET of all orderId_ belonging to Optiver orders
    """
    return set()

@provides('optiver_order_state_creates')
def order_state_creates(filter, book_ids, type = 'list'):
    """
    Not yet developed ... need to make this using optiver's orders

    Function
        - This returns an id_dict of orders. The main order types we need are:
            a) when we create an order
            b) when we delete an order
            c) when we update an order
            d) when we deleted but were picked off

    Returns:        an id_dict (uniqueIdentifier : order_state)
    """
    # return id_dict(c for c in order_states)
    return id_dict([])

@provides('manual_orders')
def manual_orders(filter, book_ids, type = 'list'):
    """
    Not yet developed ... need to make this using optiver's orders 

    Returns:        an id_dict (uniqueIdentifier : order_state)
    """
    # return id_dict(c for c in order_states)
    return id_dict([])


@provides('order_matches')
@enriches('foba_events')
def order_matches(foba_events, optiver_order_state_creates):
    """
    Not yet developed

    1) Inputs
        - FobaEvents - there are events we are matching that we derived from the true unfiltered ADD, MODIFY etc
        - optiver_order_state_creates = orders & their states based on eventTypes
    
    2) pre-matching
        We build ...
        i) order_creates ... dictionary is made that we key w/ (bookId, orderId) - filters orders based on event types
        ii) order_ids    ... same but value is optiver's internal orderId (can be same as real orderId)
    
    3) Matching
        We loop over all event_id, event in foba_events:
            - we create an "order key" (bookId & orderId) ... this is used for matching
            - we make an "aggressor key" (bookId, aggressor_order_number)
        
        i) Direct match
            - we check if the orderKey exists in order_creates ... if it does ... further check if we were aggressors by looking at aggressor_number
        ii) Aggressor match
            - If no direct match - check if aggressorkey is in order_creates ... if it is ... set optiver_aggressor to True, optiver_order to false
        iii) No match
            - proceed by setting fields to Nan in OptiverOrderMatch
        
            
        At the end of EACH match we create OptiverOrderMatch objects .... these are critical ... lots of juicy analysis here
    
    Return: ImmutableDict(event_id, match) ... where event_id is that of the corresponding FobaEvent
    """


    def match_orders():
        for event_id, event in foba_events.items():
            order_key = event.book_id, event.order_number
            aggressor_key = event.book_id, event.aggressor_order_number
            """
            Match logic here
            """
            match = OptiverOrderMatch(
                        optiver_order=False,
                        optiver_aggressor=False,
                        optiver_trigger_received=-1,
                        optiver_auto_trader='',
                        optiver_trigger_book_id='',
                        optiver_trigger_book_type='',
                        optiver_trigger_type='',
                        optiver_sent=-1,
                        volume_ahead_at_optiver_sent=-2,           
                        optiver_valuation=float('nan'),
                        optiver_theoretical_price=float('nan'),
                        optiver_fixed_offset=float('nan'),
                        optiver_variable_offset=float('nan'),
                        optiver_credit=float('nan'),
                        optiver_credit_bps=float('nan'),
                        optiver_adjustment=float('nan'),
                        optiver_position_reducing=float('nan'),
                        optiver_adjustment_triggered=float('nan'),
                        optiver_send_delay=float('nan'),
                        optiver_potentially_picked_off=float('nan')
                    )
            yield event_id, match

    return ImmutableDict(match_orders())



def volume_ahead_at_send(foba_event, order):
    """
    This function assumes orders will have:
        - bidPrice0, askPrice0, bidVolume0, askVolume0
        - price & volume
    """
    if foba_event.side is Side.BID:
        if order.bidVolume0 is None:
            return None
        best_price_at_send = order.bidPrice0
        best_volume_at_send = order.bidVolume0
    else:
        if order.askVolume0 is None:
            return None
        best_price_at_send = order.askPrice0
        best_volume_at_send = -order.askVolume0

    if abs(order.price - best_price_at_send) < 1e-10:
        return best_volume_at_send
    
    if foba_event.side.value * (order.price - best_price_at_send) > 0:
        return 0
    
    return None

@provides('optiver_order_deletes')
@enriches('foba_events')
def order_deletes(foba_events, optiver_order_state_creates):
    """
    Similar matching logic to the add(s)
        i) we get all the orders we have marked as deleted ... deletedOrders
        ii) We iterate over all events in the foba_events and if orderKey = (bookId, number) matches to that in deletedOrders ... we create and yield OptiverDeleteMatch objects
    """
    def matches():

        for event_id, event in foba_events.items():
            order_key = event.book_id, event.order_number
            # if order_key in order_pick_offs:
            if 2 == 1:
                pass
            else:
                match = OptiverDeleteMatch(
                        optiver_delete_trigger_received=-1,
                        optiver_delete_trigger_book_id='',
                        optiver_delete_trigger_book_type='',
                        optiver_delete_trigger_type='',
                        optiver_delete_sent=-1,
                        volume_ahead_at_optiver_delete_sent=-2,
                        optiver_delete_valuation=float('nan'),
                        optiver_delete_theoretical_price=float('nan'),
                        optiver_delete_credit=float('nan'),
                        optiver_delete_credit_bps=float('nan'),
                        optiver_delete_adjustment=float('nan'),
                        optiver_delete_adjustment_triggered=float('nan'),
                        optiver_delete_event_type='')
            yield event_id, match
    
    return ImmutableDict(matches())


@provides('optiver_order_pick_offs')
@enriches('foba_events')
def order_pick_offs(foba_events, optiver_order_state_creates):
    """
    Similar matching logic to the add(s)
        i) we get all the orders we have marked as full or partial fills on pending delete and store their (bookId, orderNumber) in pickOffs
        ii) We iterate over all events in the foba_events and if orderKey = (bookId, number) matches to that in pickOffs ... we create and yield pickOff objects
    """
    def matches():

        for event_id, event in foba_events.items():
            order_key = event.book_id, event.order_number
            # if order_key in order_pick_offs:
            if 2 == 1:
                pass
            else:
                match = OptiverPickOffs(
                        optiver_pick_off=False,
                        optiver_pick_off_trigger_received=-1,
                        optiver_pick_off_trigger_book_id='',
                        optiver_pick_off_trigger_book_type='',
                        optiver_pick_off_trigger_type='',
                        optiver_delete_sent=-1,
                        volume_ahead_at_optiver_delete_sent=-2,
                )
            yield event_id, match

    return ImmutableDict(matches())


def get_filled_hit_entries_base(filter, book_ids):
    """
    This needs to return rows that represent moments we swung for a hit (sent add order and were filled)
        - inner merge on all our add orders & our confirmed trades

    the key fields required are same as those of an "Order"
    - bookId, timestampNanos_ (received, exchange, fpga_send)
    - triggerType, orderId, price, volume,
    - bidPrice0, askPrice0, bidVolume0, askVolume0 (top bid/ask price|volume at the time ... needed later for BQ matching)
    - eventType
    - valuation, offset, adjustment (pricing)

    """
    return []

@provides('filled_hit_entries')
def get_filled_hit_entries(filter, book_ids):
    return id_dict(hit_entry for hit_entry in get_filled_hit_entries_base(filter, book_ids))


@provides('hit_entry_enrichment')
@enriches('foba_events')
def hit_entry_enrichment_omdc(foba_events, order_matches, filled_hit_entries):
    """
    For each foba_events' event_id - we can look at the order_matches to find the order the event matches to and then see if it matches to a filled_hit
    Therefore we can make corresponding OptiverHits objects
    """
    # need to match the filled_hit_entries w/ events in the foba_events (i.e. event_ids) ... call these hit_event_ids

    hit_event_ids = []

    def hit_enrichments():
        for event_id, event in foba_events.items():
            if event_id in hit_event_ids:
                credit = event.side.value * (event.event_price - hit_event_ids[event_id].theo)
                creditBps = 10_000 * credit/event.price
                adjustment = hit_event_ids[event_id].theo - hit_event_ids[event_id].valuation
                offset = hit_event_ids[event_id].fixed_offset + hit_event_ids[event_id].variable_offset
                position_reducing = adjustment > 0 if event.side.value > 0 else adjustment < 0
                adjustment_triggered = offset > credit + event.side.value * adjustment


                optiver_hits = OptiverHits(
                    optiver_hit=True,
                    optiver_hit_trigger_received=hit_event_ids[event_id].trigger_received,
                    optiver_hit_auto_trader=hit_event_ids[event_id].auto_trader,
                    optiver_hit_trigger_book_id=hit_event_ids[event_id].trigger_book_id,
                    optiver_hit_trigger_book_type=hit_event_ids[event_id].trigger_book_type,
                    optiver_hit_trigger_type=hit_event_ids[event_id].trigger_type,
                    optiver_hit_valuation=hit_event_ids[event_id].valuation,
                    optiver_hit_theoretical_price=hit_event_ids[event_id].theo,
                    optiver_hit_fixed_offset=hit_event_ids[event_id].fixed_offset,
                    optiver_hit_variable_offset=hit_event_ids[event_id].variable_offset,
                    optiver_hit_credit=credit,
                    optiver_hit_credit_bps=creditBps,
                    optiver_hit_adjustment=adjustment,
                    optiver_hit_position_reducing=position_reducing,
                    optiver_hit_adjustment_triggered=adjustment_triggered,
                )
            else:
                optiver_hits = OptiverHits(
                    optiver_hit=False,
                    optiver_hit_trigger_received=-1,
                    optiver_hit_auto_trader='',
                    optiver_hit_trigger_book_id='',
                    optiver_hit_trigger_book_type='',
                    optiver_hit_trigger_type='',
                    optiver_hit_valuation=float('nan'),
                    optiver_hit_theoretical_price=float('nan'),
                    optiver_hit_fixed_offset=float('nan'),
                    optiver_hit_variable_offset=float('nan'),
                    optiver_hit_credit=float('nan'),
                    optiver_hit_credit_bps=float('nan'),
                    optiver_hit_adjustment=float('nan'),
                    optiver_hit_position_reducing=float('nan'),
                    optiver_hit_adjustment_triggered=float('nan'),
                )

            yield event_id, optiver_hits


    return ImmutableDict(hit_enrichments())



@provides('performance_classification')
@enriches('foba_events')
def performance_classification(foba_events, order_matches, hit_entry_enrichment, optiver_order_deletes):
    """
    For each foba_event we have:
        - eventId : OptiverOrder        ... this is whenever we tried to do something (send an ADD or DELETE)
        - eventId : OptiverHits         ... this is when we swung for trade and got filled (partial or complete)
        - eventId : OptiverDelete       ... this is when we managed to delete a trade


    Using the level_id we set in foba_events we can analyse ALL events that occur on a level:
        - Add orders (passive or aggressie), Delete orders

        i) We group the events by level_id
        ii) If we did not send any add orders to this level ... mark NOT_SENT
        ii) Quote & Hit Analysis
            We group the orders we sent by orderId (i.e. all actions on a single order):

            For all ADD orders not aggressive (QUOTES):
                - If we did not send a corresponding delete:
                    - If we traded completely:      FULL_FILL
                    - If we traded partially:       PARTIAL_FILL_POOR_PRIORITY
                    - If we did not trade:          POOR_PRIORITY
                
                - If we sent a corresponding delete:
                    - If we traded at some point:
                        - If the trade occured in some "pullLatencyTimePeriod" ... e.g. 10 micros or so
                            - full trade:           FULL_FILL_ON_PENDING_DELETE
                            - partial:              PARTIAL_FILL_ON_PENDING_DELETE
                        
                        - If not:
                            - Did the trade occur in a "valuationTimePeriod":

                                e.g. we want our val to be at least 50 micros more predictive than the "tick" so val updated too slow
                            
                                - If Yes:
                                    - full trade:           FULL_FILL_POOR_VAL
                                    - partial:              PARTIAL_FILL_POOR_VAL

                                - If No:
                                    - treat the same as above (FULL_FILL, PARTIAL_FILL_POOR_PRIORITY, POOR_PRIORITY)
                    - If we did not trade ... PULLED

            
            For all ADD orders that are aggressive (HIT):
                - If we traded against the level completely:    FULL_FILL_ON_HIT
                - If we traded partially ... TOO_SLOW_PARTIAL_HIT
                
                - If we did not trade at all:
                    - was there a preeceeding "add" not belonging to optiver that matched ... TOO_SLOW_HIT
                    - was there a preceeding "delete" of the order  ... TOO_SLOW_PULLED
                

    """
    def evaluate():
        for event_id, event in foba_events.items():
            performance = OptiverPerformance(
                    analysis_type=None,
                    optiver_performance_evaluation=None,
                )
            yield event_id, performance

    return ImmutableDict(evaluate())

