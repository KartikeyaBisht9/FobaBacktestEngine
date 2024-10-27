from collections import namedtuple

import numpy as np
import pandas as pd

from foba_backtest_engine.components.order_book.utils import MyRow as MyRow
from foba_backtest_engine.components.order_book.utils.enums import (
    EventType,
    ProductClass,
    Side,
)
from foba_backtest_engine.enrichment import enriches, id_dict, provides
from foba_backtest_engine.utils.base_utils import ImmutableDict

VolumeAtSent = namedtuple("VolumeAtSent", ("volume_ahead_at_max_join_sent"))

OrderCountStateAtJoin = namedtuple(
    "OrderCountStateAtJoin",
    (
        "bids_0_count_at_join_",
        "bids_0_max_volume_at_join_",
        "asks_0_count_at_join_",
        "asks_0_max_volume_at_join_",
        "bids_1_count_at_join_",
        "bids_1_max_volume_at_join_",
        "asks_1_count_at_join_",
        "asks_1_max_volume_at_join_",
        "bids_2_count_at_join_",
        "bids_2_max_volume_at_join_",
        "asks_2_count_at_join_",
        "asks_2_max_volume_at_join_",
        "bids_3_count_at_join_",
        "bids_3_max_volume_at_join_",
        "asks_3_count_at_join_",
        "asks_3_max_volume_at_join_",
        "bids_4_count_at_join_",
        "bids_4_max_volume_at_join_",
        "asks_4_count_at_join_",
        "asks_4_max_volume_at_join_",
    ),
)

OrderCountStateAtEvent = namedtuple(
    "OrderCountStateAtEvent",
    (
        "bids_0_count_at_event_",
        "bids_0_max_volume_at_event_",
        "asks_0_count_at_event_",
        "asks_0_max_volume_at_event_",
        "bids_1_count_at_event_",
        "bids_1_max_volume_at_event_",
        "asks_1_count_at_event_",
        "asks_1_max_volume_at_event_",
        "bids_2_count_at_event_",
        "bids_2_max_volume_at_event_",
        "asks_2_count_at_event_",
        "asks_2_max_volume_at_event_",
        "bids_3_count_at_event_",
        "bids_3_max_volume_at_event_",
        "asks_3_count_at_event_",
        "asks_3_max_volume_at_event_",
        "bids_4_count_at_event_",
        "bids_4_max_volume_at_event_",
        "asks_4_count_at_event_",
        "asks_4_max_volume_at_event_",
    ),
)


EnrichmentHelper = namedtuple(
    "EnrichmentHelper",
    (
        "original_key",
        "group_key",
        "sort_key",
        "payload",
    ),
)


OptiverOrderMatch = namedtuple(
    "OptiverOrderMatch",
    (
        "optiver_order",
        "optiver_aggressor",
        "optiver_trigger_book_id",
        "optiver_trigger_book_type",
        "optiver_order_type",
        "optiver_request_status",
        "optiver_driver_sent",
        "optiver_exchange_time",
        "volume_ahead_at_optiver_join",
        "optiver_send_delay",
    ),
)

OptiverDeleteMatch = namedtuple(
    "OptiverDeleteMatch",
    (
        "optiver_delete",
        "optiver_trigger_book_id",
        "optiver_trigger_book_type",
        "optiver_order_type",
        "optiver_request_status",
        "optiver_driver_sent",
        "optiver_exchange_time",
        "volume_ahead_at_optiver_delete_sent",
        "optiver_send_delay",
    ),
)

OptiverTrade = namedtuple(
    "OptiverTrade",
    (
        "bookId_",
        "orderId_",
        "aggressorId_",
        "optiver_hit",
        "optiver_trade",
        "optiver_broker_id",
        "counterparty_broker_code",
        "aggressor_add_order_ts",
        "optiver_price",
        "optiver_side",
        "optiver_volume",
        "optiver_exchange_time",
        "optiver_portfolio",
        "optiver_AT_name",
        "pick_off",
        "trigger_response_time",
        "competitor_latency",
        "received_",
    ),
)


def get_private_feed_data(filter):
    df = pd.read_parquet(filter.private_feed_path)
    df = df[df["feedcode"].isin(filter.book_ids)].reset_index(drop=True)
    return df

def get_own_order_data(filter, table_name):
    if table_name == "order_insert":
        df = pd.read_parquet(filter.order_insert_path)
        df = df[df["feedcode"].isin(filter.book_ids)].reset_index(drop=True)
        return df
    elif table_name == "delete_operations":
        df = pd.read_parquet(filter.delete_operation_path)
        df = df[df["feedcode"].isin(filter.book_ids)].reset_index(drop=True)
        return df
    elif table_name == "private_trade":
        df = pd.read_parquet(filter.private_trade_path)
        df = df[df["feedcode"].isin(filter.book_ids)].reset_index(drop=True)
        return df
    else:
        raise ValueError("Wtf is this")

def get_merged_trades(filter, date_to_inspect, feedcodes, broker_number_to_broker_name):
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]
    private_trades = get_own_order_data(filter, table_name="private_trade")
    trade_confirms = get_private_feed_data(filter)

    combined = pd.merge(
        private_trades,
        trade_confirms,
        left_on=["private_trade_key"],
        right_on=["unique_trade_id"],
    )
    combined["counterparty_broker_id"] = combined["counterparty_broker_id"].astype(
        "int"
    )
    combined["submitting_broker_id"] = combined["submitting_broker_id"].astype("int")
    mapping = {
        "trade_time": "exchange_timestamp",
        "exchange_order_id": "orderId_",
        "feedcode_x": "bookId_",
        "feedcode_y":"feedcode",
        "log_time": "received_",
        "sending_time_epoch": "add_order_timestamp_",
        "counterparty_broker_id": "counterpartyId_",
        "exchange_trade_id": "exchangeTradeId_",
        "submitting_broker_id": "optiverBrokerId_",
        "price_x":"price",
        "volume_x": "volume",
    }
    combined = combined.rename(columns=mapping)
    combined = combined[
        [
            "exchange_timestamp",
            "orderId_",
            "bookId_",
            "feedcode",
            "username",
            "portfolio",
            "received_",
            "price",
            "volume",
            "optiver_side",
            "dd_group_key",
            "add_order_timestamp_",
            "counterpartyId_",
            "exchangeTradeId_",
            "optiverBrokerId_",
        ]
    ]
    combined["orderId_"] = combined["orderId_"].astype("Int64")

    addOrders = get_own_order_data(filter, table_name="order_insert")
    addOrders = addOrders.rename(
        columns={"exchange_order_id": "orderId_", "feedcode": "bookId_"}
    )
    addOrders["orderId_"] = pd.to_numeric(
        addOrders["orderId_"].replace(["", " "], np.nan), errors="coerce"
    ).astype("Int64")

    order_type_dict = dict(zip(addOrders["orderId_"], addOrders["lifespan"]))
    if (len(combined) == 0) | (len(order_type_dict) == 0):
        pass
    else:
        combined["order_type"] = combined["orderId_"].map(order_type_dict)
        combined["optiver_hit"] = np.where(combined["order_type"] == "FAK", True, False)

    return combined


@provides("optiver_order_numbers")
def order_numbers_filtered(filter, type="list"):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]
    addOrders = get_own_order_data(filter, table_name="order_insert")
    addOrders = addOrders[addOrders["order_result"] == "Accepted"]
    addOrders["exchange_order_id"] = addOrders["exchange_order_id"].astype("Int64")

    addOrders["datetime"] = pd.to_datetime(
        addOrders["exchange_timestamp"] + 8 * 60 * 60 * 1e9
    )
    addOrders["datetime"] = addOrders["datetime"].dt.tz_localize("Asia/Hong_Kong")
    addOrders = addOrders[addOrders["datetime"] <= filter.end_time.datetime]

    if len(addOrders) == 0:
        return set()
    else:
        return set(list(addOrders["exchange_order_id"].values))


def get_FAK_orders(filter):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]
    addOrders = get_own_order_data(filter, table_name="order_insert")
    addOrders = addOrders[addOrders["order_result"] == "Accepted"]
    addOrders["exchange_order_id"] = addOrders["exchange_order_id"].astype("Int64")
    addOrders["datetime"] = pd.to_datetime(
        addOrders["exchange_timestamp"] + 8 * 60 * 60 * 1e9
    )
    addOrders["datetime"] = addOrders["datetime"].dt.tz_localize("Asia/Hong_Kong")
    addOrders = addOrders[addOrders["datetime"] <= filter.end_time.datetime]

    if len(addOrders) == 0:
        return set()
    else:
        FAKs = set(
            list(addOrders[addOrders["lifespan"] == "FAK"]["exchange_order_id"].values)
        )
        return FAKs


def get_competitor_FAKS(filter, FAK_ids):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]

    private_trades = get_own_order_data(filter, table_name="private_trade")
    trade_confirms = get_private_feed_data(filter)


    combined = pd.merge(
        private_trades,
        trade_confirms[
            [
                "unique_trade_id",
                "order_id",
                "log_time",
                "sending_time_epoch",
                "counterparty_broker_id",
                "exchange_trade_id",
                "submitting_broker_id",
            ]
        ],
        left_on=["unique_trade_id"],
        right_on=["unique_trade_id"],
        how="inner",
    )
    combined["datetime"] = pd.to_datetime(
        combined["exchange_timestamp"] + 8 * 60 * 60 * 1e9
    )
    combined["datetime"] = combined["datetime"].dt.tz_localize("Asia/Hong_Kong")
    combined = combined[combined["datetime"] <= filter.end_time.datetime]

    if len(combined) == 0:
        order_id_to_broker_map = {}
    else:
        columns_to_convert_to_int = [
            "exchange_order_id",
            "feedcode",
            "order_id",
            "counterparty_broker_id",
            "exchange_trade_id",
            "submitting_broker_id",
        ]
        combined[columns_to_convert_to_int] = combined[
            columns_to_convert_to_int
        ].astype("Int64")
        optiver_quote_fills = combined[~(combined["exchange_order_id"].isin(FAK_ids))]
        order_id_to_broker_map = dict(
            zip(
                optiver_quote_fills["exchange_order_id"],
                optiver_quote_fills["counterparty_broker_id"],
            )
        )
    return order_id_to_broker_map


@provides("optiver_order_state_creates")
def order_state_creates(filter, type="list"):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]
    addOrders = get_own_order_data(filter, table_name="order_insert")
    addOrders = addOrders[addOrders["order_result"]=="Accepted"].sort_values("exchange_order_id").reset_index(drop=True)
    addOrders = addOrders[
        [
            "dd_group_key",
            "eml_key",
            "exchange_order_id",
            "order_result",
            "exchange_timestamp",
            "feedcode",
            "order_price",
            "order_volume",
            "is_bid",
            "lifespan",
            "t8",
            "error_class",
            "error_text",
        ]
    ]
    addOrders = addOrders.rename(
        columns={
            "exchange_order_id": "orderId_",
            "feedcode": "bookId_",
            "t8": "driver_sent_time",
            "is_bid": "side",
            "lifespan": "eventType",
            "requestStatus": "order_result",
        }
    )
    addOrders["orderId_"] = pd.to_numeric(
        addOrders["orderId_"].replace(["", " "], np.nan), errors="coerce"
    ).astype("Int64")
    if len(addOrders) == 0:
        pass
    else:
        addOrders["one_way_latency"] = (
            addOrders["exchange_timestamp"] - addOrders["driver_sent_time"]
        )
        addOrders["side"] = addOrders["side"].map({True: Side.BID, False: Side.ASK})
        addOrders["aggressive"] = addOrders["eventType"].map(
            {"FAK": True, "GFD": False}
        )

    addOrders["datetime"] = pd.to_datetime(
        addOrders["exchange_timestamp"] + 8 * 60 * 60 * 1e9
    )
    addOrders["datetime"] = addOrders["datetime"].dt.tz_localize("Asia/Hong_Kong")
    addOrders = addOrders[addOrders["datetime"] <= filter.end_time.datetime]

    order_list = []
    for _, row in addOrders.iterrows():
        row_dict = row.to_dict()
        order_list.append(MyRow(row_dict))

    return id_dict([c for c in order_list])


@provides("order_matches")
@enriches("foba_events")
def order_matches(foba_events, optiver_order_state_creates, feed_states_at_join):
    """
    FOBA events has ALL the trades that occur w/ order-number matching the PASSIVE orderId
    - So if an order-number matches a FAK ... the FAK was a BOOV
    """

    def matches():
        order_creates = {
            (order.bookId_, order.orderId_): order
            for event_id, order in optiver_order_state_creates.items()
        }

        for eventId, event in foba_events.items():
            order_key = event.book_id, event.order_number
            aggressor_key = event.book_id, event.aggressor_order_number
            feed_at_join = feed_states_at_join[eventId]

            if order_key in order_creates:
                order = order_creates[order_key]
                output = OptiverOrderMatch(
                    optiver_order=True,
                    optiver_aggressor=order.aggressive,
                    optiver_trigger_book_id=order.bookId_,
                    optiver_trigger_book_type=ProductClass.STOCK,
                    optiver_order_type=order.eventType,
                    optiver_request_status=order.order_result,
                    optiver_driver_sent=order.driver_sent_time,
                    optiver_exchange_time=order.exchange_timestamp,
                    volume_ahead_at_optiver_join=volume_ahead_join(
                        event, order, feed_at_join
                    ),
                    optiver_send_delay=order.one_way_latency,
                )
            elif aggressor_key in order_creates:
                order = order_creates[aggressor_key]
                output = OptiverOrderMatch(
                    optiver_order=False,
                    optiver_aggressor=True,
                    optiver_trigger_book_id=order.bookId_,
                    optiver_trigger_book_type=ProductClass.STOCK,
                    optiver_order_type=order.eventType,
                    optiver_request_status=order.order_result,
                    optiver_driver_sent=order.driver_sent_time,
                    optiver_exchange_time=order.exchange_timestamp,
                    volume_ahead_at_optiver_join=volume_ahead_join(
                        event, order, feed_at_join
                    ),
                    optiver_send_delay=order.one_way_latency,
                )
            else:
                output = OptiverOrderMatch(
                    optiver_order=False,
                    optiver_aggressor=False,
                    optiver_trigger_book_id="",
                    optiver_trigger_book_type="",
                    optiver_order_type="",
                    optiver_request_status="",
                    optiver_driver_sent="",
                    optiver_exchange_time="",
                    volume_ahead_at_optiver_join=0,
                    optiver_send_delay="",
                )
            yield eventId, output

    return ImmutableDict(matches())


def volume_ahead(foba_event, order, feed):
    """
    This function assumes orders will have:
        - bidPrice0, askPrice0, bidVolume0, askVolume0
        - price & volume
    """
    if foba_event.side is Side.BID:
        if feed.bids_0_volume_ is None:
            return 0
        best_price_at_send = feed.bids_0_price_
        best_volume_at_send = feed.bids_0_volume_
    else:
        if feed.asks_0_volume_ is None:
            return 0
        best_price_at_send = feed.asks_0_price_
        best_volume_at_send = -feed.asks_0_volume_

    if abs(order.price - best_price_at_send) < 1e-10:
        return best_volume_at_send

    if foba_event.side.value * (order.price - best_price_at_send) > 0:
        return 0

    return None


def volume_ahead_join(foba_event, order, join_feed):
    """
    This function assumes orders will have:
        - bidPrice0, askPrice0, bidVolume0, askVolume0
        - price & volume
    """
    if foba_event.side is Side.BID:
        if join_feed.bids_0_volume_at_join_ is None:
            return 0
        best_price_at_send = join_feed.bids_0_price_at_join_
        best_volume_at_send = join_feed.bids_0_volume_at_join_
    else:
        if join_feed.asks_0_volume_at_join_ is None:
            return 0
        best_price_at_send = join_feed.asks_0_price_at_join_
        best_volume_at_send = -join_feed.asks_0_volume_at_join_

    if abs(order.order_price - best_price_at_send) < 1e-10:
        return best_volume_at_send

    if foba_event.side.value * (order.order_price - best_price_at_send) > 0:
        return 0

    return None


@provides("optiver_order_deletes")
def order_deletes(filter, type="list"):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    feedcodes = [str(x) if isinstance(x, int) else x for x in feedcodes]
    deleteOrders = get_own_order_data(
        filter, table_name="delete_operations"
    )
    deleteOrders = deleteOrders[
        [
            "dd_group_key",
            "eml_key",
            "exchange_order_id",
            "operation_result",
            "exchange_timestamp",
            "feedcode",
            "price",
            "volume",
            "delete_side",
            "eeid_event_id",
            "error_class",
            "error_text",
            "t8",
        ]
    ]
    deleteOrders = deleteOrders.rename(
        columns={
            "exchange_order_id": "orderId_",
            "feedcode": "bookId_",
            "t8": "driver_sent_time",
            "operation_result": "requestStatus",
        }
    )
    deleteOrders["orderId_"] = pd.to_numeric(
        deleteOrders["orderId_"].replace(["", " "], np.nan), errors="coerce"
    ).astype("Int64")
    if len(deleteOrders) == 0:
        pass
    else:
        deleteOrders["one_way_latency"] = (
            deleteOrders["exchange_timestamp"] - deleteOrders["driver_sent_time"]
        )
        deleteOrders["side"] = deleteOrders["delete_side"].map(
            {"BID": Side.BID, "ASK": Side.ASK}
        )
    order_list = []
    for _, row in deleteOrders.iterrows():
        row_dict = row.to_dict()
        order_list.append(MyRow(row_dict))

    return id_dict([c for c in order_list])


@provides("order_delete_matches")
@enriches("foba_events")
def order_delete_matches(
    foba_events, optiver_order_deletes, full_feed_state_enrichment
):
    def matches():
        order_deletes = {
            (order.bookId_, order.orderId_): order
            for key, order in optiver_order_deletes.items()
        }

        for eventId, event in foba_events.items():
            order_key = event.book_id, event.order_number
            feed_at_delete = full_feed_state_enrichment[eventId]

            if order_key in order_deletes:
                order = order_deletes[order_key]
                output = OptiverDeleteMatch(
                    optiver_delete=True,
                    optiver_trigger_book_id=order.bookId_,
                    optiver_trigger_book_type=ProductClass.STOCK,
                    optiver_order_type=EventType.PULL,
                    optiver_request_status=order.requestStatus,
                    optiver_driver_sent=order.driver_sent_time,
                    optiver_exchange_time=order.exchange_timestamp,
                    volume_ahead_at_optiver_delete_sent=volume_ahead(
                        event, order, feed_at_delete
                    ),
                    optiver_send_delay=order.one_way_latency,
                )
            else:
                output = OptiverDeleteMatch(
                    optiver_delete=False,
                    optiver_trigger_book_id="",
                    optiver_trigger_book_type="",
                    optiver_order_type="",
                    optiver_request_status="",
                    optiver_driver_sent="",
                    optiver_exchange_time="",
                    volume_ahead_at_optiver_delete_sent=0,
                    optiver_send_delay="",
                )
            yield eventId, output

    return ImmutableDict(matches())


@provides("optiver_trades")
def get_optiver_trades(filter, broker_number_to_broker_name):
    date_to_inspect, feedcodes = filter.start_time.datetime, filter.book_ids
    optiver_trades = get_merged_trades(
        filter, date_to_inspect, feedcodes, broker_number_to_broker_name
    )

    trade_list = []
    for _, row in optiver_trades.iterrows():
        row_dict = row.to_dict()
        trade_list.append(MyRow(row_dict))

    return id_dict([c for c in trade_list])


@provides("optiver_hit_or_quote")
@enriches("foba_events")
def optiver_trade_and_quotes(filter, foba_events, optiver_trades, order_delete_matches):
    quotes = {
        (trade.bookId_, trade.orderId_): trade
        for key, trade in optiver_trades.items()
        if trade.optiver_hit == False
    }
    hits = {
        (trade.bookId_, trade.orderId_): trade
        for key, trade in optiver_trades.items()
        if trade.optiver_hit == True
    }

    pending_deletes = {
        order.orderId_
        for key, order in order_delete_matches.items()
        if order.optiver_order_type == "DELETE"
    }

    def matches():
        for event_id, event in foba_events.items():
            if event.event_type == EventType.TRADE:
                quote_key = (event.book_id, event.order_number)
                hit_key = (event.book_id, event.aggressor_order_number)

                hit, pickoff, add_delete_delay, competitor_speed = (
                        False,
                        False,
                        np.nan,
                        np.nan,
                    )
                if event.order_number in pending_deletes:
                    pickoff, pending_delete = (
                        True,
                        pending_deletes[event.order_number],
                    )
                    sentTime, tradeTime, addTime = (
                        pending_delete.optiver_driver_sent,
                        optiver_trade.exchange_timestamp,
                        optiver_trade.add_order_timestamp_,
                    )
                    add_delete_delay, competitor_speed = (
                        sentTime - addTime,
                        tradeTime - addTime,
                    )


                if quote_key in quotes:
                    """
                    This is an Optiver quote trade
                    """
                    quote_trade = quotes[quote_key]
                
                    optiver_trade = OptiverTrade(
                        bookId_=quote_key[0],
                        orderId_=quote_key[1],
                        aggressorId_=event.aggressor_order_number,
                        optiver_hit=quote_trade.optiver_hit,
                        optiver_trade=True,
                        optiver_broker_id=int(quote_trade.optiverBrokerId_),
                        counterparty_broker_code=int(quote_trade.counterpartyId_),
                        aggressor_add_order_ts=quote_trade.add_order_timestamp_,
                        optiver_price=quote_trade.price,
                        optiver_side=Side.BID if quote_trade.optiver_side == "Buy" else Side.ASK,
                        optiver_volume=quote_trade.volume,
                        optiver_exchange_time=quote_trade.exchange_timestamp,
                        optiver_portfolio=quote_trade.portfolio,
                        optiver_AT_name=quote_trade.username,
                        pick_off=pickoff,
                        trigger_response_time=add_delete_delay,
                        competitor_latency=competitor_speed,
                        received_=quote_trade.received_,
                    )

                elif hit_key in hits:
                    hit_trade = hits[hit_key]

                    optiver_trade = OptiverTrade(
                        bookId_=hit_key[0],
                        orderId_=event.order_number,
                        aggressorId_=hit_key[1],
                        optiver_hit=hit_trade.optiver_hit,
                        optiver_trade=True,
                        optiver_broker_id=int(hit_trade.optiverBrokerId_),
                        counterparty_broker_code=int(hit_trade.counterpartyId_),
                        aggressor_add_order_ts=hit_trade.add_order_timestamp_,
                        optiver_price=hit_trade.price,
                        optiver_side=Side.BID if hit_trade.optiver_side == "Buy" else Side.ASK,
                        optiver_volume=hit_trade.volume,
                        optiver_exchange_time=hit_trade.exchange_timestamp,
                        optiver_portfolio=hit_trade.portfolio,
                        optiver_AT_name=hit_trade.username,
                        pick_off=pickoff,
                        trigger_response_time=add_delete_delay,
                        competitor_latency=competitor_speed,
                        received_=hit_trade.received_,
                    )

                else:
                    optiver_trade = OptiverTrade(
                        bookId_=event.book_id,
                        orderId_=event.order_number,
                        aggressorId_=event.aggressor_order_number,
                        optiver_hit=False,
                        optiver_trade=False,
                        optiver_broker_id=np.nan,
                        counterparty_broker_code=np.nan,
                        aggressor_add_order_ts=np.nan,
                        optiver_price=np.nan,
                        optiver_side="",
                        optiver_volume=np.nan,
                        optiver_exchange_time=np.nan,
                        optiver_portfolio=np.nan,
                        optiver_AT_name=np.nan,
                        pick_off=np.nan,
                        trigger_response_time=np.nan,
                        competitor_latency=np.nan,
                        received_=np.nan,
                    )

            else:
                optiver_trade = OptiverTrade(
                    bookId_=event.book_id,
                    orderId_=event.order_number,
                    aggressorId_=event.aggressor_order_number,
                    optiver_hit=False,
                    optiver_trade=False,
                    optiver_broker_id=np.nan,
                    counterparty_broker_code=np.nan,
                    aggressor_add_order_ts=np.nan,
                    optiver_price=np.nan,
                    optiver_side="",
                    optiver_volume=np.nan,
                    optiver_exchange_time=np.nan,
                    optiver_portfolio=np.nan,
                    optiver_AT_name=np.nan,
                    pick_off=np.nan,
                    trigger_response_time=np.nan,
                    competitor_latency=np.nan,
                    received_=np.nan,
                )

            yield event_id, optiver_trade

    return ImmutableDict(matches())
