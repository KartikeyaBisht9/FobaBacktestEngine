from collections import namedtuple
from enum import Enum, unique

import pytz

from foba_backtest_engine.enrichment import enriches, provides
from foba_backtest_engine.utils.base_utils import ImmutableDict
from foba_backtest_engine.utils.time_utils import (
    _convert_unix_to_datetime,
    _convert_unix_to_fdate,
    _convert_unix_to_ftime,
    _time_from_start_of_day_in_seconds,
    to_nano_timestamp,
)


@unique
class Category(Enum):
    BOOV = 0
    EARLY = 1
    LATE = 2
    DEPTH = 3
    PRE_OPEN = 4


@unique
class JoinType(Enum):
    CROSS = 0
    DIME = 1
    JOIN = 2
    DEEPER_JOIN = 3


@unique
class Side(Enum):
    BID = 1
    UNKNOWN = 0
    ASK = -1


CategoryEnrichment = namedtuple("CategoryEnrichment", "category")
DerivedEnrichment = namedtuple(
    "DerivedEnrichment",
    (
        "join_type",
        "direction",
        "order_age_millis",
        "event_value",
        "turnover",
        "tick_size_bps",
        "date",
        "join_received_string",
        "event_received_string",
        "join_received_datetime",
        "event_received_datetime",
        "join_time_from_start_of_day_seconds",
        "event_time_from_start_of_day_seconds",
        "book_product",
    ),
)


@provides("category_enrichment")
@enriches("foba_events")
def category_enrichment(foba_events, early_cutoff_time, market_open=None):
    open = to_nano_timestamp(market_open) if market_open else None

    def items():
        for event_id, event in foba_events.items():
            if open and event.join_driver_received < open:
                category = Category.PRE_OPEN
            elif event.join_aggressive_volume > 0:
                category = Category.BOOV
            elif event.join_depth > 0:
                category = Category.DEPTH
            elif (
                event.join_driver_received
                < event.level_driver_received + early_cutoff_time
            ):
                category = Category.EARLY
            else:
                category = Category.LATE
            yield event_id, CategoryEnrichment(category=category)

    return ImmutableDict(items())


@provides("derived_enrichment")
@enriches("foba_events")
def derived_enrichment(
    foba_events, send_times, static_data_enrichment, currency_rate=1, time_zone=None
):
    time_zone = pytz.timezone(time_zone)

    def items():
        for event_id, event in foba_events.items():
            if event.join_aggressive_volume > 0:
                join_type = JoinType.CROSS
            elif event.join_depth < 0:
                join_type = JoinType.DIME
            elif event.join_depth == 0:
                join_type = JoinType.JOIN
            else:
                join_type = JoinType.DEEPER_JOIN

            direction = 1 if event.side == Side.BID else -1
            order_age_millis = (
                send_times[event_id].max_event_sent_time
                - send_times[event_id].max_join_sent_time
            ) / 1e6
            event_value = (
                static_data_enrichment[event_id].contract_size
                * event.event_volume
                * direction
                * event.event_price
                * currency_rate
            )
            tick_size_bps = (
                static_data_enrichment[event_id].tick_size / event.event_price * 10000
                if event.event_price != 0
                else None
            )
            date = _convert_unix_to_fdate(
                event.join_driver_received, time_zone=time_zone
            )
            join_received_string = _convert_unix_to_ftime(
                event.join_driver_received, time_zone=time_zone
            )
            event_received_string = _convert_unix_to_ftime(
                event.event_driver_received, time_zone=time_zone
            )
            join_received_datetime = _convert_unix_to_datetime(
                event.join_driver_received, time_zone=time_zone
            )
            event_received_datetime = _convert_unix_to_datetime(
                event.event_driver_received, time_zone=time_zone
            )
            join_time_from_start_of_day_seconds = _time_from_start_of_day_in_seconds(
                event.join_driver_received, time_zone=time_zone
            )
            event_time_from_start_of_day_seconds = _time_from_start_of_day_in_seconds(
                event.event_driver_received, time_zone=time_zone
            )
            symbol = (
                ""
                if static_data_enrichment[event_id].product_symbol is None
                else static_data_enrichment[event_id].product_symbol
            )
            book_product = symbol + " - " + event.book_id

            yield (
                event_id,
                DerivedEnrichment(
                    join_type=join_type,
                    direction=direction,
                    order_age_millis=order_age_millis,
                    event_value=event_value,
                    turnover=abs(event_value),
                    tick_size_bps=tick_size_bps,
                    date=date,
                    join_received_string=join_received_string,
                    event_received_string=event_received_string,
                    join_received_datetime=join_received_datetime,
                    event_received_datetime=event_received_datetime,
                    join_time_from_start_of_day_seconds=join_time_from_start_of_day_seconds,
                    event_time_from_start_of_day_seconds=event_time_from_start_of_day_seconds,
                    book_product=book_product,
                ),
            )

    return ImmutableDict(items())
