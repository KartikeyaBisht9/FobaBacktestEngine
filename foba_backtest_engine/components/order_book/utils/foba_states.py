from collections import namedtuple, defaultdict

"""
The different namedTuples here are just different types of snapshots 
of the order book state, order count, auction match etc.

namedTuples are used because I like field names are easily accessable & the namedtuple is immutable
"""

FeedState = namedtuple('FeedState', (
    'bookId_',
    'createdNanos_',
    'received_',
    'timestamp_',
    'bids_0_price_', 'bids_0_volume_',
    'bids_1_price_', 'bids_1_volume_',
    'bids_2_price_', 'bids_2_volume_',
    'bids_3_price_', 'bids_3_volume_',
    'bids_4_price_', 'bids_4_volume_',
    'asks_0_price_', 'asks_0_volume_',
    'asks_1_price_', 'asks_1_volume_',
    'asks_2_price_', 'asks_2_volume_',
    'asks_3_price_', 'asks_3_volume_',
    'asks_4_price_', 'asks_4_volume_',
    'price_',
    'volume_',
    'foreignLd_aggressorSide_',
    'isTrade_'
))

OrderCountState = namedtuple('OrderCountState', ('bookId_',
                                                 'createdNanos_',
                                                 'received_',
                                                 'timestamp_', 'bids_0_count_',
                                                 'bids_0_max_volume_',
                                                 'asks_0_count_',
                                                 'asks_0_max_volume_',
                                                 'bids_1_count_',
                                                 'bids_1_max_volume_',
                                                 'asks_1_count_',
                                                 'asks_1_max_volume_',
                                                 'bids_2_count_',
                                                 'bids_2_max_volume_',
                                                 'asks_2_count_',
                                                 'asks_2_max_volume_',
                                                 'bids_3_count_',
                                                 'bids_3_max_volume_',
                                                 'asks_3_count_',
                                                 'asks_3_max_volume_',
                                                 'bids_4_count_',
                                                 'bids_4_max_volume_',
                                                 'asks_4_count_',
                                                 'asks_4_max_volume_',
                                                 'isTrade_'))

FeedStateAtJoin = namedtuple('FeedStateAtJoin', (
    'bookId_at_join_',
    'createdNanos_at_join_',
    'received_at_join_',
    'timestamp_at_join_',
    'bids_0_price_at_join_', 'bids_0_volume_at_join_',
    'asks_0_price_at_join_', 'asks_0_volume_at_join_',
))

FullFeedStateAtJoin = namedtuple('FullFeedStateAtJoin', (
    'bookId_at_join_', 'createdNanos_at_join_',
    'received_at_join_', 'timestamp_at_join_',
    'bids_0_price_at_join_', 'bids_0_volume_at_join_',
    'asks_0_price_at_join_', 'asks_0_volume_at_join_',
    'bids_1_price_at_join_', 'bids_1_volume_at_join_',
    'asks_1_price_at_join_', 'asks_1_volume_at_join_',
    'bids_2_price_at_join_', 'bids_2_volume_at_join_',
    'asks_2_price_at_join_', 'asks_2_volume_at_join_',
    'bids_3_price_at_join_', 'bids_3_volume_at_join_',
    'asks_3_price_at_join_', 'asks_3_volume_at_join_',
    'bids_4_price_at_join_', 'bids_4_volume_at_join_',
    'asks_4_price_at_join_', 'asks_4_volume_at_join_',
))

FullFeedStateAtEvent = namedtuple('FullFeedStateAtEvent', (
    'bookId_at_event_', 'createdNanos_at_event_',
    'received_at_event_', 'timestamp_at_event_',
    'bids_0_price_at_event_', 'bids_0_volume_at_event_',
    'asks_0_price_at_event_', 'asks_0_volume_at_event_',
    'bids_1_price_at_event_', 'bids_1_volume_at_event_',
    'asks_1_price_at_event_', 'asks_1_volume_at_event_',
    'bids_2_price_at_event_', 'bids_2_volume_at_event_',
    'asks_2_price_at_event_', 'asks_2_volume_at_event_',
    'bids_3_price_at_event_', 'bids_3_volume_at_event_',
    'asks_3_price_at_event_', 'asks_3_volume_at_event_',
    'bids_4_price_at_event_', 'bids_4_volume_at_event_',
    'asks_4_price_at_event_', 'asks_4_volume_at_event_',
))

AuctionMatch = namedtuple('AuctionMatch', (
    'bookId_',
    'createdNanos_',
    'received_',
    'timestamp_',
    'match_volume', 'match_price',
    'surplus_volume', 'surplus_volume_side',
    'causing_order_id', 'event_type',
    'optiver_order', 'order_side', 'order_volume'
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