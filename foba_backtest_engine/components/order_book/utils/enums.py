from collections import namedtuple
from enum import Enum, unique

FobaEvent = namedtuple(
    "FobaEvent",
    (
        "id",
        "book_id",
        "order_number",
        "aggressor_order_number",
        "side",
        "level_id",
        "level_exchange_timestamp",
        "level_driver_received",
        "level_driver_created",
        "level_aggressive_volume",
        "join_exchange_timestamp",
        "join_driver_received",
        "join_driver_created",
        "join_driver_sequence_number",
        "join_volume",
        "join_aggressive_volume",
        "join_depth",
        "join_score",
        "volume_ahead_at_join",
        "level_cumulative_joined_volume_at_join",
        "count_ahead_at_join",
        "event_type",
        "event_exchange_timestamp",
        "event_driver_received",
        "event_driver_created",
        "event_driver_sequence_number",
        "event_price",
        "event_volume",
        "event_depth",
        "event_score",
        "aggressor_volume",
        "volume_behind_at_event",
        "count_behind_at_event",
        "volume_ahead_at_event",
        "count_ahead_at_event",
        "level_volume_at_event",
        "remaining_volume",
        "cumulative_pulled_volume",
        "order_type",
        "foreign_join_reason",
        "book_state",
        "volume_behind_after_join",
        "inplace_updates",
        "volume_reducing_updates",
        "inplace_updates_received",
        "inplace_updates_depth",
        "volume_reducing_updates_received",
        "next_best_level_price",
        "next_best_level_order_count_at_join",
        "next_best_level_volume_at_join",
        "best_level_times",
    ),
)

"""
ENUMS for STATIC DATA
    - tickSchedule
    - FeeSchedule
    - ProductClass
    - FeeRule
    - 
"""


@unique
class TickSchedule(Enum):
    OMDC_STOCKS = 0
    ETF = 1


@unique
class FeeSchedule(Enum):
    OMDC_STOCKS = 0
    ETF = 1


@unique
class ProductClass(Enum):
    BOND = "Bond"
    COMBO = "Combo"
    CURRENCY = "Currency"
    ETF = "ExchangeTradedFund"
    FUTURE = "Future"
    INDEX = "Index"
    OPTION = "Option"
    STOCK = "Stock"
    WARRANT = "Warrant"
    TURBO_WARRANT = "TurboWarrant"
    TAS = "TradeAtSettlement"
    NO_DEF = None


@unique
class FeeRule(Enum):
    STOCK_CLEARING = 0
    STOCK_SEHK_CLEARING = 1
    STOCK_TRANSACTION_LEVY_EXCHANGE = 2
    STOCK_MM_STAMP = 3
    STOCK_FULL_STAMP = 4
    FRC_TRANSACTION_LEVY = 5
    STOCK_FULL_STAMP_CBBC = 6
    ETF_CLEARING = 7
    ETF_HKEX_TARIFF = 8


@unique
class Side(Enum):
    BID = ord("B")
    ASK = ord("S")
    NONE = None


@unique
class Command(Enum):
    ADD = 0
    UPDATE = 1
    DELETE = 2
    CLEAR = 3


@unique
class Exchange(Enum):
    ASX = 0
    SFE = 1
    JAPANNEXT = 2
    OMDC = 3
    OSE = 4
    SGX = 5
    CHIX = 6
    OMDD = 7
    BROKERTEC = 8
    CHIX_New = 9
    JAPANNEXT_New = 10
    TSE = 11
    SBIX = 12


@unique
class EventType(Enum):
    TRADE = 0
    PULL = 1
    INPLACE_UPDATE = 2


@unique
class EventScore(Enum):
    MISMATCH = "MISMATCH"
    PARTIAL = "PARTIAL"
    MATCH = "MATCH"
    AUCTION = "AUCTION"


@unique
class JoinScore(Enum):
    DOESNTEXIST = "DOESNTEXIST"
    NULL = "NULL"
    PERFECT = "PERFECT"
    ADJUSTED = "ADJUSTED"
    AMBIGUOUS = "AMBIGUOUS"


@unique
class OrderType(Enum):
    NORMAL = "NORMAL"
    ICEBERG = "ICEBERG"


@unique
class QuoteType(Enum):
    CROSS = 0
    DIME = 1
    JOIN = 2
    DEEPER_JOIN = 3
    UNKNOWN = 4


@unique
class Category(Enum):
    BOOV = 0
    EARLY = 1
    LATE = 2
    DEPTH = 3


@unique
class OptiverOpportunityType(Enum):
    HIT = 0
    QUOTE = 1
    NONE = 2
    UNKNOWN = 3


@unique
class OrderValidity(Enum):
    day = 548
    good_till_cancelled = 549
    goot_till_date = 550
    immediate = 551


@unique
class PeakCategory(Enum):
    BEFORE_PEAK = 0
    AFTER_PEAK = 1
