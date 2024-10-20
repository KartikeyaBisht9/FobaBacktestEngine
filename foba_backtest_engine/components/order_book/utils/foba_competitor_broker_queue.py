from foba_backtest_engine.utils.base_utils import ImmutableDict, multi_dict, ImmutableRecord
from foba_backtest_engine.components.order_book.utils.enums import EventType
from foba_backtest_engine.enrichment import provides, enriches
from collections import defaultdict
from urllib.error import URLError
import pandas as pd
import urllib
import csv
import io

"""
---------------------
 COMPETITOR MATCHER
---------------------

This is the core class that handles our fuzzy matching logic that maps the CBQ updates to our internal order state view


Note:   
    - The Epsilon is due to float point errors
    - The time allowance was determined by trial and error to 1s as well as analysis of max mismatch time

Important DataStructures
    a) Order Queues 
        ... we have separate bid & ask queues
        ... for each queue ... the strcture is like so:

            QueueLevel = List<Price, List<OrderNumber(s)>>
            Queue = Pair <createdNanos_, sortedList<QueueLevel>>

                    e.g. the orderbook at time = 1728565192000000000 w/ 2 orders at P = 101.5 & 1 at P = 101.0 is:
                        [1010, [[101.5, [1, 3]], [101.0, [2]]]]]
    
    b) Broker queue
        ... in our OMDC dataframe the broker queue items are stored as unique rows and these rows must be parsed back into a order book type object
        - this is handled by parse_broker_queue_new. This converts rows such as:

        broker_queue_sorted = [
            {"timestampNanos_": 1000, "side_": 1, "level_": 0, "brokerNumber_": 9481},  # Bid
            {"timestampNanos_": 1000, "side_": 1, "level_": 0, "brokerNumber_": 9910},  # Bid
            {"timestampNanos_": 1000, "side_": 2, "level_": 0, "brokerNumber_": 2221},  # Ask
            {"timestampNanos_": 2000, "side_": 1, "level_": 0, "brokerNumber_": 9481},  # Bid
            {"timestampNanos_": 2000, "side_": 1, "level_": 0, "brokerNumber_": 9910},  # Bid
            {"timestampNanos_": 2000, "side_": 2, "level_": 0, "brokerNumber_": 2221},  # Ask
            {"timestampNanos_": 2000, "side_": 1, "level_": 1, "brokerNumber_": 4123},  # Bid
            {"timestampNanos_": 2000, "side_": 2, "level_": 1, "brokerNumber_": 5441},  # Ask
        ]

        into ....

        curr_result =  (2000, [[9481, 9910], [4123]], [[2221], [5441]])          
        all_results = [
            (1000, [[9481, 9910]], [[2221]]),
            (2000, [[9481, 9910], [4123]], [[2221], [5441]])
        ]


Attributes ... we store:
    - bid & ask order queue .. these are the Queue above .. indexed by timestamp
    - matched_results .. this is a dictionary of order number to broker number 

Matching Algorithm
    1) FUZZY MATCHING -- temporal * spatial precision
        We iterate over the order queue (internal book) & only look at queues whereby createdNanos_ > timeNow - tolerance 

            **Note:  fill_levels .. converts the order queue structure to a List of Lists - each sublist is a price level & contains order number at that level
        
        i) Part 1: find candidates:
            - using sufficient_numebr_of_orders we check count the number of orders per level & look for 2 things:
            - order queue must have AT LEAST as many levels as the BrokerQueue snapshot
            - at each level of the order queue the number of orders must be >= BrokerQueue snapshot
        
        ii) Part 2: best candidate:
            - First:    we look for candidates where number of orders on the best level perfectly match the snapshot
                - we do this separately for bids & asks
            - Second:   we then favor either the bid OR ask ... based on the number of orders (smaller = more precise match)
            - Third:    now for the un-favoured side select the candidate for which the timestamp is closest to & not earlier than the favored side
            
            Fall back .. if we don't find any exact matches - use the last available candidates
    
    
    2) MATCHING
        Here we simply take the best candidates and brokerCode 1,2,3 ... are attached with orderNumber of orders 1,2,3 ...
            here the 1, 2, 3 ... represent the order's position from the front of the book 
"""

TIME_ALLOWANCE_NANOS = 1e9  # allow 1 second
EPSILON = 1e-4

class CompetitorMatcher:
    def __init__(self, bid_order_queue, ask_order_queue):
        self.bid_order_queue = bid_order_queue
        self.ask_order_queue = ask_order_queue
        self.matched_results = defaultdict(int)  # defaultdict (key, value) = (order_number, broker_number)
        self.last_idx_bid = 0
        self.last_idx_ask = 0

        self.candidate_order_queue_bid = []
        self.candidate_timestamps_bid = []
        self.candidate_order_queue_ask = []
        self.candidate_timestamps_ask = []

        self.total_count = 0
        self.both_match_count = 0
        self.bid_match_count = 0
        self.ask_match_count = 0

    def enrich(self):
        pass

    def match_snapshot(self, snapshot):
        num_orders_bq_snapshot_bid = [len(x) for x in snapshot[1]]
        num_orders_bq_snapshot_ask = [len(x) for x in snapshot[2]]

        self.candidate_order_queue_bid = []
        self.candidate_timestamps_bid = []
        self.candidate_order_queue_ask = []
        self.candidate_timestamps_ask = []
        candidate_idx_bid = []
        candidate_idx_ask = []

        process_bid = True
        process_ask = True

        bid_before_curr_bq_idx = self.last_idx_bid
        ask_before_curr_bq_idx = self.last_idx_ask

        self.total_count += 1

        while process_bid or process_ask:
            if self.last_idx_bid < len(self.bid_order_queue) and self.bid_order_queue[self.last_idx_bid][0] < snapshot[
                0] - TIME_ALLOWANCE_NANOS:
                bid_before_curr_bq_idx = self.last_idx_bid

            if self.last_idx_ask < len(self.ask_order_queue) and self.ask_order_queue[self.last_idx_ask][0] < snapshot[
                0] - TIME_ALLOWANCE_NANOS:
                ask_before_curr_bq_idx = self.last_idx_ask

            if self.last_idx_bid >= len(self.bid_order_queue) or self.bid_order_queue[self.last_idx_bid][0] > snapshot[
                0] + TIME_ALLOWANCE_NANOS:
                process_bid = False
            if self.last_idx_ask >= len(self.ask_order_queue) or self.ask_order_queue[self.last_idx_ask][0] > snapshot[
                0] + TIME_ALLOWANCE_NANOS:
                process_ask = False

            if process_bid:
                if self._sufficient_number_of_orders(num_orders_bq_snapshot_bid,
                                                     self.bid_order_queue[self.last_idx_bid],
                                                     self.candidate_order_queue_bid,
                                                     self.candidate_timestamps_bid,
                                                     True):
                    candidate_idx_bid.append(self.last_idx_bid)
                else:
                    pass
                self.last_idx_bid += 1

            if process_ask:
                if self._sufficient_number_of_orders(num_orders_bq_snapshot_ask,
                                                     self.ask_order_queue[self.last_idx_ask],
                                                     self.candidate_order_queue_ask,
                                                     self.candidate_timestamps_ask,
                                                     False):
                    candidate_idx_ask.append(self.last_idx_ask)
                else:
                    pass
                self.last_idx_ask += 1

        if self.candidate_timestamps_ask and self.candidate_timestamps_bid:
            bid_idx, ask_idx = self._most_suitable_candidate(num_orders_bq_snapshot_bid,
                                                             num_orders_bq_snapshot_ask)
            self.match(self.candidate_order_queue_bid[bid_idx], snapshot[1])
            self.match(self.candidate_order_queue_ask[ask_idx], snapshot[2])

            self.last_idx_bid = candidate_idx_bid[bid_idx] + 1
            self.last_idx_ask = candidate_idx_ask[ask_idx] + 1

            self.both_match_count += 1
        elif self.candidate_timestamps_bid:
            # only match bid side
            self.match(self.candidate_order_queue_bid[-1], snapshot[1])
            self.last_idx_bid = candidate_idx_bid[-1] + 1
            self.last_idx_ask = ask_before_curr_bq_idx

            self.bid_match_count += 1
        elif self.candidate_timestamps_ask:
            # only match ask side
            self.match(self.candidate_order_queue_ask[-1], snapshot[2])
            self.last_idx_ask = candidate_idx_ask[-1] + 1
            self.last_idx_bid = bid_before_curr_bq_idx

            self.ask_match_count += 1
        else:
            self.last_idx_bid = bid_before_curr_bq_idx
            self.last_idx_ask = ask_before_curr_bq_idx
            return

    def match(self, orders_per_level_order_queue, brokers_per_level_broker_queue):
        for (curr_level_orders, curr_level_brokers) in zip(orders_per_level_order_queue,
                                                           brokers_per_level_broker_queue):
            for (i, broker) in enumerate(curr_level_brokers):
                self.matched_results[curr_level_orders[i]] = broker  # Take the latest information by default

    def _sufficient_number_of_orders(self, 
                                     num_of_orders_bq_snapshot, 
                                     order_queue_input, 
                                     candidate_order_queue,
                                     timestamps,
                                     bid_side):
        order_queue = fill_levels(order_queue_input[1], bid_side)
        num_of_orders_order_queue = [len(entry) for entry in order_queue]
        if len(num_of_orders_order_queue) < len(num_of_orders_bq_snapshot):
            return False
        for idx in range(len(num_of_orders_bq_snapshot)):
            if num_of_orders_order_queue[idx] < num_of_orders_bq_snapshot[idx]:
                return False
        candidate_order_queue.append(order_queue)
        timestamps.append(order_queue_input[0])
        return True

    def _most_suitable_candidate(self, orders_bq_snapshot_bid, orders_bq_snapshot_ask):
        bid_idx = []
        if orders_bq_snapshot_bid:
            bid_idx = [idx for (idx, entry) in enumerate(self.candidate_order_queue_bid) if
                       len(entry[0]) == orders_bq_snapshot_bid[0]]

        ask_idx = []
        if orders_bq_snapshot_ask:
            ask_idx = [idx for (idx, entry) in enumerate(self.candidate_order_queue_ask) if
                       len(entry[0]) == orders_bq_snapshot_ask[0]]

        if bid_idx and ask_idx:
            # In the case where both bid & ask idx exist ... take the side w/ smaller order number (more certain)
            if orders_bq_snapshot_bid[0] < orders_bq_snapshot_ask[0]:
                bid_idx_final = bid_idx[-1]
                bid_time = self.candidate_timestamps_bid[bid_idx_final]
                ask_idx_final = None
                for idx in ask_idx:
                    if self.candidate_timestamps_ask[idx] > bid_time:
                        if ask_idx_final is None:
                            ask_idx_final = idx
                        break
                    ask_idx_final = idx
            else:
                ask_idx_final = ask_idx[-1]
                ask_time = self.candidate_timestamps_ask[ask_idx_final]
                bid_idx_final = None
                for idx in bid_idx:
                    if self.candidate_timestamps_bid[idx] > ask_time:
                        if bid_idx_final is None:
                            bid_idx_final = idx
                        break
                    bid_idx_final = idx
            return bid_idx_final, ask_idx_final

        if bid_idx:
            # bid side exact match only
            bid_idx_final = bid_idx[-1]
            bid_time = self.candidate_timestamps_bid[bid_idx_final]
            ask_idx_final = None
            for idx in range(len(self.candidate_order_queue_ask)):
                if self.candidate_timestamps_ask[idx] > bid_time:
                    if ask_idx_final is None:
                        ask_idx_final = idx
                    break
                ask_idx_final = idx
            return bid_idx_final, ask_idx_final

        if ask_idx:
            # ask side exact match only
            ask_idx_final = ask_idx[-1]
            ask_time = self.candidate_timestamps_ask[ask_idx_final]
            bid_idx_final = None
            for idx in range(len(self.candidate_order_queue_bid)):
                if self.candidate_timestamps_bid[idx] > ask_time:
                    if bid_idx_final is None:
                        bid_idx_final = idx
                    break
                bid_idx_final = idx
            return bid_idx_final, ask_idx_final

        # if no exact match found in bid or ask, just use the last from each side (this logic is fuzzy)
        return len(self.candidate_order_queue_bid) - 1, len(self.candidate_order_queue_ask) - 1

"""
Note HKEX does not allow a nicely scrapable table ... this must be manually adjusted if exchange changes tickSchedule
"""
def next_tick(price, bid_side=False):
    TICK_SIZES = [(0.25, 0.001),
                  (0.5, 0.005),
                  (10, 0.01),
                  (20, 0.02),
                  (100, 0.05),
                  (200, 0.1),
                  (500, 0.5),
                  (1000, 0.5),
                  (2000, 1),
                  (5000, 2),
                  (10000, 5)]

    if bid_side:
        for (upper_bound, tick_size) in TICK_SIZES:
            if price <= upper_bound:
                return price - tick_size
        return price - TICK_SIZES[-1][1]
    else:
        for (upper_bound, tick_size) in TICK_SIZES:
            if price < upper_bound:
                return price + tick_size
        return price + TICK_SIZES[-1][1]


def add_broker(side_broker_list, curr_level, curr_broker_number):
    if len(side_broker_list) > curr_level:
        side_broker_list[curr_level].append(curr_broker_number)
        return
    while len(side_broker_list) <= curr_level:
        side_broker_list.append([])
    side_broker_list[curr_level].append(curr_broker_number)

def fill_levels(order_queue, bid_side):
    result = []
    idx = 0
    next_price = -1 if bid_side else float('inf')
    while idx < len(order_queue):
        if (next_price - order_queue[idx][0] > EPSILON and bid_side) or (
                EPSILON < order_queue[idx][0] - next_price and not bid_side):
            result.append([])
            next_price = next_tick(next_price, bid_side)
            continue
        else:
            result.append(order_queue[idx][1])
            next_price = next_tick(order_queue[idx][0], bid_side)
            idx += 1
    return result

def parse_broker_queue_new(broker_queue_sorted):
    all_results = []
    curr_timestamp_nanos = -1
    curr_result = ()
    for snapshot in broker_queue_sorted:
        if snapshot.timestampNanos_ > curr_timestamp_nanos:
            if curr_result:
                all_results.append(curr_result)
            curr_timestamp_nanos = snapshot.timestampNanos_
            if snapshot.side_ == 1:
                curr_result = (
                    snapshot.timestampNanos_, [[snapshot.brokerNumber_]],
                    [])  # (timestamp, bid_brokers, ask_brokers)
            else:
                curr_result = (
                    snapshot.timestampNanos_, [],
                    [[snapshot.brokerNumber_]])  # (timestamp, bid_brokers, ask_brokers)
        elif snapshot.timestampNanos_ == curr_timestamp_nanos:
            add_broker(curr_result[snapshot.side_], snapshot.level_, snapshot.brokerNumber_)
        else:
            raise ValueError('seems the data are not sorted properly')
    return all_results

@provides('order_num_to_broker_num')
def omdc_order_number_to_broker_number(pybuilders, book_ids, omdc_broker_queue):
    def items():
        for book, builder in pybuilders.items():

            if book not in [str(x) if isinstance(x, int) else x for x in book_ids]:
                continue
            competitor_matcher = CompetitorMatcher(builder.bid_order_queue.order_queue,
                                                   builder.ask_order_queue.order_queue)
            curr_broker_queue = parse_broker_queue_new(omdc_broker_queue[book])
            for snapshot in curr_broker_queue:
                competitor_matcher.match_snapshot(snapshot)
            yield book, competitor_matcher.matched_results

    return ImmutableDict(items())


"""
================================================================================================

COMPETITOR ENRICHMENT  
    Optiver trades
        - this contains every trade we have been a part of with a tag whether we hit or quoted (optiver_hit)
        - the optiverBrokerId_ is the driver/id of optiver 
        - the counterpartyId_ is the party we traded against = for optiver_hit = False, this is who hit us
    
    Our FobaEvents only record the passive-order-id so we can match this to our own quote fills and find the aggressor who hit us exactly.
    
    So given our own quote fills we need to do the following
    1) Create broker-number fields
        - we use our exact quote-fills to match orderId w/ us
        - for the rest we use the brokerQueue fuzzy match
    
    2) Create foreign_counterparty fields
        - for our own quote fills ... we can exactly match this
        - for our HIT trades ... we first narrow our search window in time
            ... we then look for the correct side & find trades where notional-traded alings
             - we then choose the one closest to the exchange-timestamp
    

"""

@provides('passive_enrichment')
@enriches('foba_events')
def competitor_enrichment(foba_events, order_num_to_broker_num, broker_number_to_broker_name, optiver_trades):
    
    optiver_order_to_broker = {}
    if optiver_trades:
        optiver_quotes = multi_dict([x for x in optiver_trades.values() if x.optiver_hit == False], key=lambda x: (x.bookId_, x.orderId_, abs(x.price * x.volume)))
        
        for event_id, event in foba_events.items():
            bookId, order_number, notional = event.book_id, event.order_number, abs(event.event_price * event.event_volume)
            opti_trades = optiver_quotes[(bookId, order_number, notional)]
            if len(opti_trades) > 0:
                received = [t.received_ for t in opti_trades]
                index = received.index(min(received, key = lambda x : abs(x-event.event_driver_received)))
                broker = int(opti_trades[index].optiverBrokerId_)
            else:
                broker = -1
            
            if event.book_id not in optiver_order_to_broker:
                optiver_order_to_broker[event.book_id] = {}
            
            optiver_order_to_broker[event.book_id][order_number] = broker
        
    improved_order_number_to_broker = dict(order_num_to_broker_num)
    for k,v in improved_order_number_to_broker.items():
        for on, bn in v.items():
            if on in optiver_order_to_broker:
                new_bn = optiver_order_to_broker[on]
                if new_bn == -1:
                    improved_order_number_to_broker[on] = bn
                else:
                    improved_order_number_to_broker[on] = new_bn
    def items():
        for event_id, event in foba_events.items():
            broker_number = improved_order_number_to_broker[event.book_id][event.order_number]
            if broker_number > 0:
                if broker_number in broker_number_to_broker_name:
                    broker_name = broker_number_to_broker_name[broker_number]
                else:
                    broker_name = str(broker_number)
            else:
                broker_name = "UNKNOWN"
            yield event_id, ImmutableRecord(broker_number=broker_number, broker_name=broker_name)
    return ImmutableDict(items())
    
    

"""
================================================================================================
COUNTERPARTY ENRICHMENT
    ... As described above this section does:

    Create foreign_counterparty fields
        - for our own quote fills ... we can exactly match this
        - for our HIT trades ... we first narrow our search window in time
            ... we then look for the correct side & find trades where notional-traded alings
             - we then choose the one closest to the exchange-timestamp
    
    
"""

@provides('aggressive_enrichment')
@enriches('foba_events')
def foreign_counterparty_enrichment(foba_events, optiver_hit_or_quote, broker_number_to_broker_name):
    aggressor_map = {}
    
    for event_id, event in foba_events.items():
        if event.event_type is EventType.TRADE:
            optiver_trade = optiver_hit_or_quote[event_id]
            if optiver_trade.optiver_trade:
                if optiver_trade.optiver_hit:
                    aggressor_map[event_id] = optiver_trade.optiver_broker_id
                else:
                    aggressor_map[event_id] = optiver_trade.counterparty_broker_code
            else:
                pass

    def items():
        for event_id, event in foba_events.items():
            if event_id in aggressor_map:
                broker_number = aggressor_map[event_id]
                broker_name = broker_number_to_broker_name[broker_number] \
                    if broker_number in broker_number_to_broker_name else str(broker_number)
            else:
                broker_number = -1
                broker_name = 'UNKNOWN'
            yield event_id, ImmutableRecord(foreign_counterparty=broker_name,
                                            foreign_counterparty_number=broker_number)
    return ImmutableDict(items())


"""
BROKER ORDER ENRICHMENT

This uses the foba_events as well as competitor enrichment to do the following:
    i) For each broker and price level we track the order by looking at the:
        - order_number
        - earliest timestamp = join_driver_created (when the order was added)
        - latest timestamp   = event_driver_created (when the order was processed)
    
    ii) By tracking the orders we can calculate:
        - broker_orders_placed_on_level = total number of unique orders placed by the broker on the level
        - broker_order_at_join = total number of orders that were active for the broker at the time of the event. 
        - broker_orders_remaining_at_event = how many remain
"""


@provides('broker_orders_enrichment')
@enriches('foba_events')
def broker_orders_enrichment(foba_events, passive_enrichment):

    def items():
        for level_broker_events in multi_dict(foba_events.items(),
                                              key=lambda item: (item[1].level_id,
                                                                passive_enrichment[item[0]].broker_number)
                                              ).values():

            orders = set()
            for order_id in {event.order_number for _, event in level_broker_events}:
                orders.add((
                    order_id,
                    min([event.join_driver_created
                         for _, event in level_broker_events if event.order_number == order_id]),
                    max([event.event_driver_created
                         for _, event in level_broker_events if event.order_number == order_id])
                ))

            times = [event.join_driver_created for _, event in level_broker_events] + \
                    [event.event_driver_created for _, event in level_broker_events]
            num_orders = {}
            for time in times:
                num_orders[time] = sum([1 for order in orders if time >= order[1] and time < order[2]])

            for event_id, event in level_broker_events:
                yield event_id, ImmutableRecord(
                        broker_orders_placed_on_level=len(orders),
                        broker_order_at_join=num_orders[event.join_driver_created],
                        broker_orders_remaining_at_event=num_orders[event.event_driver_created])

    return ImmutableDict(items())


