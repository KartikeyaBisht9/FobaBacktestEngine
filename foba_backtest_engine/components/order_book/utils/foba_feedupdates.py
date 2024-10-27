from collections import namedtuple

import numpy as np
import pandas as pd

from foba_backtest_engine.components.order_book.utils import enums

"""
-------------------
    FEED UPDATE
-------------------

This is a data normalizer ... it takes in data from ADD | MODIFY | DELETE | CLEAR dataframes 
and creates FeedUpdate objects that are all namedTuples .. have some general attributes.


Feed capture method expected
 - expect FPGA w/ line arbitration ... OMDC has multiple multi-cast channels that each delier different types of orders
    .. e.g Add orders (type = 30) are on one channel, deletes on another
    .. each channel has 2 lines (A/B) ... so arb these


Expected dataframe fields (ADD | DELETE | MODIFY)
    - received_         ... this is the exchange-Timestamp which we can infer or is disseminated
    - createdNanos_     ... this is the timestamp at which the FPGA registers the packet
    - timestampNanos_   ... this is the timestamp we extract from the packet header (ms precision for HKEX :( )
    - securityCode_     ... this is the corresponding securityCode_ that is extracted from the message byteArrays (e.g. 700 for TCH)
    - message extracted fields:      price | volume | orderId_ | sequenceNumber

    - end_              ... this tells us if the row is a MESSAGE that was the LAST MESSAGE in the packet (True or False)
    - changeReason_     ... this tells us the PURPOSE for which the message was disseminated by exchange:
            0 = this is occuring due to an ADD order
            1 = this is ocuring due to a DELETE order
            2 = this is a modify due to a MODIFY order
            3 = this is occuring due to a TRADE (i.e. could be a "corrective" modify or delete arriving)

Expected broker queue is explained in foba_backtest_engine.components.orderbook.utils.foba_omdc_broker_queue_processor.py

"""

FeedUpdate = namedtuple(
    "FeedUpdate",
    (
        "received",
        "created",
        "timestamp",
        "command",
        "side",
        "book",
        "order_number",
        "change_reason",
        "price",
        "volume",
        "inferred",
        "aggressor_order_number",
        "sequence_number",
    ),
)


def db_to_feed_update(row, exchange):
    if exchange == enums.Exchange.OMDC:
        trade_class = lambda x: x
    else:
        raise ValueError("Valid exchanges are OMDC ")

    return FeedUpdate(
        received=row["received_"],
        created=row["createdNanos_"],
        timestamp=row["timestampNanos_"],
        command=enums.Command(row["class_"]),
        side=enums.Side(row["side_"]),
        book=row["securityCode_"],
        order_number=row["orderId_"],
        change_reason=trade_class(row["changeReason_"]),
        price=row["price_"],
        volume=row["volume_"],
        inferred=row["end_"],
        aggressor_order_number=row["aggressorId_"],
        sequence_number=row["sequenceNumber_"],
    )


def get_feed_updates(exchange, filter=None):
    table_class_map = {
        "add_order": 0,
        "update_order": 1,
        "delete_order": 2,
        "clear_book": 3,
    }
    table_map = {
        "add_order": "AddOrder",
        "update_order": "ModifyOrder",
        "delete_order": "DeleteOrder",
        "clear_book": "ClearOrder",
    }

    dataframe = pd.read_parquet(filter.order_book_path)
    dataframe = dataframe[dataframe['securityCode_'].isin(filter.book_ids)]
    
    data_as_dict = (
        dataframe.sort_values(["sequenceNumber_", "createdNanos_", "class_"], ascending=[True, True, False])
        .replace({np.nan: None})
        .to_dict(orient="records")
    )
    for item in data_as_dict:
        if item["orderId_"]:
            item["orderId_"] = int(item["orderId_"])

    result = list(map(lambda fu: db_to_feed_update(fu, exchange), data_as_dict))
    del dataframe
    del data_as_dict

    return result
