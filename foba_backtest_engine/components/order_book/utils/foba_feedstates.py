from collections import defaultdict, namedtuple
from itertools import chain
from operator import attrgetter

from foba_backtest_engine.enrichment import enriches, id_dict, provides
from foba_backtest_engine.utils.base_utils import (
    ImmutableDict,
    priority_process_and_dispatch,
    sorted_multi_dict,
)

"""
FEEDSTATE ENRICHMENT

- FeedState contains bookId, received timestamp, createdNanos & [price, volume] x [0-4] levels

i) We use the EnrichmentHelper to combine a FeedState to each FobaEvent ... using priority process dispatch function
ii) priority_process_and_dispatch function sorts (FobaEvent) & (FeedState) by bookId_
    - then we sort by createdNanos_ for the feed_states & foba_events
    - we then try and match the most "recent" feed_state with the foba_events

"""


EnrichmentHelper = namedtuple(
    "EnrichmentHelper",
    (
        "original_key",
        "group_key",
        "sort_key",
        "payload",
    ),
)


class FeedStateEnricher:
    def __init__(self, foba_events, feed_states, foba_slippages, join_driver_created):
        self.foba_events = [
            EnrichmentHelper(
                event_id,
                event.book_id,
                (
                    event.join_driver_created
                    if join_driver_created
                    else event.event_driver_created
                ),
                event,
            )
            for event_id, event in foba_events.items()
        ]
        self.feed_states = [
            EnrichmentHelper(key, value.bookId_, value.createdNanos_, value)
            for key, value in feed_states.items()
        ]
        self.foba_slippages = [
            EnrichmentHelper(key, slippage.bookId_, slippage.createdNanos_, slippage)
            for key, slippage in foba_slippages.items()
        ]
        self.sorted_foba_events = sorted_multi_dict(
            self.foba_events,
            group_key=attrgetter("group_key"),
            sort_key=attrgetter("sort_key"),
        )
        self.sorted_feed_states = sorted_multi_dict(
            self.feed_states,
            group_key=attrgetter("group_key"),
            sort_key=attrgetter("sort_key"),
        )
        self.sorted_slippages = sorted_multi_dict(
            self.foba_slippages,
            group_key=attrgetter("group_key"),
            sort_key=attrgetter("sort_key"),
        )
        self.join_driver_created = join_driver_created
        self.previous_retail_state = defaultdict(list)
        self.previous_slippage_event = defaultdict(list)
        self.enriched_foba_events = {"feed_state": {}, "slippages": {}}
        self.run()

    def update_feed_state(self, enrichment_helper):
        if enrichment_helper.payload.isTrade_ == 0:
            self.previous_retail_state[enrichment_helper.group_key] = [
                enrichment_helper.payload
            ]

    def update_slippages(self, enrichment_helper):
        self.previous_slippage_event[enrichment_helper.group_key] = [
            enrichment_helper.payload
        ]

    def update_foba_event(self, enrichment_helper):
        if len(self.previous_retail_state[enrichment_helper.group_key]) > 0:
            self.enriched_foba_events["feed_state"][enrichment_helper.original_key] = (
                self.previous_retail_state[enrichment_helper.group_key][0]
            )
        else:
            self.enriched_foba_events["feed_state"][enrichment_helper.original_key] = (
                self.sorted_feed_states[enrichment_helper.group_key][0].payload
            )

        if self.join_driver_created:
            pass
        else:
            if len(self.previous_slippage_event[enrichment_helper.group_key]) > 0:
                self.enriched_foba_events["slippages"][
                    enrichment_helper.original_key
                ] = self.previous_slippage_event[enrichment_helper.group_key][0]
            else:
                self.enriched_foba_events["slippages"][
                    enrichment_helper.original_key
                ] = self.sorted_slippages[enrichment_helper.group_key][0].payload

    def run(self):
        if not self.join_driver_created:
            for book in self.sorted_foba_events:
                book_events = self.sorted_foba_events[book]
                book_feed_states = self.sorted_feed_states[book]
                book_slippages = self.sorted_slippages[book]
                priority_process_and_dispatch(
                    (iter(book_events), self.update_foba_event, attrgetter("sort_key")),
                    (
                        iter(book_feed_states),
                        self.update_feed_state,
                        attrgetter("sort_key"),
                    ),
                    (
                        iter(book_slippages),
                        self.update_slippages,
                        attrgetter("sort_key"),
                    ),
                )
        else:
            for book in self.sorted_foba_events:
                book_events = self.sorted_foba_events[book]
                book_feed_states = self.sorted_feed_states[book]
                priority_process_and_dispatch(
                    (iter(book_events), self.update_foba_event, attrgetter("sort_key")),
                    (
                        iter(book_feed_states),
                        self.update_feed_state,
                        attrgetter("sort_key"),
                    ),
                )


@provides("feed_states")
def fetch_feed_stats_from_book_builders(pybuilders):
    return id_dict(
        chain.from_iterable(builder.feed_states for builder in pybuilders.values())
    )


static_fields_event = (
    "bookId_",
    "createdNanos_",
    "received_",
    "timestamp_",
    "bids_0_price_",
    "bids_0_volume_",
    "asks_0_price_",
    "asks_0_volume_",
    "bids_1_price_",
    "bids_1_volume_",
    "asks_1_price_",
    "asks_1_volume_",
    "bids_2_price_",
    "bids_2_volume_",
    "asks_2_price_",
    "asks_2_volume_",
    "bids_3_price_",
    "bids_3_volume_",
    "asks_3_price_",
    "asks_3_volume_",
    "bids_4_price_",
    "bids_4_volume_",
    "asks_4_price_",
    "asks_4_volume_",
)

static_fields_join = (
    "bookId_",
    "createdNanos_",
    "received_",
    "timestamp_",
    "bids_0_price_at_join_",
    "bids_0_volume_at_join_",
    "asks_0_price_at_join_",
    "asks_0_volume_at_join_",
    "bids_1_price_at_join_",
    "bids_1_volume_at_join_",
    "asks_1_price_at_join_",
    "asks_1_volume_at_join_",
    "bids_2_price_at_join_",
    "bids_2_volume_at_join_",
    "asks_2_price_at_join_",
    "asks_2_volume_at_join_",
    "bids_3_price_at_join_",
    "bids_3_volume_at_join_",
    "asks_3_price_at_join_",
    "asks_3_volume_at_join_",
    "bids_4_price_at_join_",
    "bids_4_volume_at_join_",
    "asks_4_price_at_join_",
    "asks_4_volume_at_join_",
)


@provides("full_feed_state_enrichment")
@enriches("foba_events")
def full_feed_state_enrichment(
    foba_events,
    feed_states,
    foba_slippages,
    join_driver_created=False,
    pnl_slippage_times=[5, 15, 30, 60, 120, 240, 300, 600, 900, 1800, 3600, 7200],
):
    feed_state_enricher = FeedStateEnricher(
        foba_events,
        feed_states,
        foba_slippages,
        join_driver_created=join_driver_created,
    )
    dynamic_fields = [f"midspot_{x}" for x in pnl_slippage_times] + [
        f"rws_{x}" for x in pnl_slippage_times
    ]
    all_fields = (
        static_fields_event
        + ("raw_bbov", "smooth_bbov")
        + tuple(dynamic_fields)
        + tuple(["rws", "rws_skew", "optiver_xgb_val", "midspot"])
    )

    FullFeedStateAtEvent = namedtuple("FullFeedStateAtEvent", all_fields)

    def items():
        for event_id, event in foba_events.items():
            feed_state = feed_state_enricher.enriched_foba_events["feed_state"][
                event_id
            ]
            slippage = feed_state_enricher.enriched_foba_events["slippages"][event_id]

            static_values = [
                getattr(feed_state, x)
                for x in ["bookId_", "createdNanos_", "received_", "timestamp_"]
            ]
            feed_state_values = []
            for level in range(5):
                feed_state_values.extend(
                    [
                        getattr(feed_state, f"bids_{level}_price_"),
                        getattr(feed_state, f"bids_{level}_volume_"),
                        getattr(feed_state, f"asks_{level}_price_"),
                        getattr(feed_state, f"asks_{level}_volume_"),
                    ]
                )

            dynamic_midspot_values = [
                getattr(slippage, f"midspot_{x}") for x in pnl_slippage_times
            ]
            dynamic_rws_values = [
                getattr(slippage, f"rws_{x}") for x in pnl_slippage_times
            ]
            bbov_values = [slippage.raw_bbov, slippage.smooth_bbov]
            misc = [
                getattr(slippage, x)
                for x in ["rws", "rws_skew", "optiver_xgb_val", "midspot"]
            ]

            combined_values = (
                tuple(static_values)
                + tuple(feed_state_values)
                + tuple(bbov_values)
                + tuple(dynamic_midspot_values)
                + tuple(dynamic_rws_values)
                + tuple(misc)
            )
            output = FullFeedStateAtEvent(*combined_values)

            yield event_id, output

    return ImmutableDict(items())


@provides("feed_states_at_join")
@enriches("foba_events")
def feed_states_at_join(
    foba_events, feed_states, foba_slippages, join_driver_created=True
):
    feed_state_enricher = FeedStateEnricher(
        foba_events,
        feed_states,
        foba_slippages,
        join_driver_created=join_driver_created,
    )
    all_fields = static_fields_join
    FeedStateAtJoin = namedtuple("FeedStateAtJoin", all_fields)

    def items():
        for event_id, event in foba_events.items():
            feed_state = feed_state_enricher.enriched_foba_events["feed_state"][
                event_id
            ]
            static_values = [
                getattr(feed_state, x)
                for x in ["bookId_", "createdNanos_", "received_", "timestamp_"]
            ]
            feed_state_values = []
            for level in range(5):
                feed_state_values.extend(
                    [
                        getattr(feed_state, f"bids_{level}_price_"),
                        getattr(feed_state, f"bids_{level}_volume_"),
                        getattr(feed_state, f"asks_{level}_price_"),
                        getattr(feed_state, f"asks_{level}_volume_"),
                    ]
                )
            combined_values = tuple(static_values) + tuple(feed_state_values)
            output = FeedStateAtJoin(*combined_values)

            yield event_id, output

    return ImmutableDict(items())
