from foba_backtest_engine.components.order_book.utils import apply_mapping as apply_mapping
from foba_backtest_engine.components.order_book.utils import MyRow as MyRow
from foba_backtest_engine.components.order_book.utils.enums import Side
from foba_backtest_engine.utils.time_utils import to_milli_timestamp
from foba_backtest_engine.enrichment import provides, id_dict
from collections import namedtuple, defaultdict
from enum import Enum, EnumMeta
from operator import attrgetter
from itertools import chain
import pandas as pd
import numpy as np


def get_trades(filter, book_ids):
    """
    This fetches all confirmed optiver trades with these attributes:
        - Time fields:      received_, timestampNanos_
        - Order fields:     orderId_, order_, price_, volume_
        - Broker fields:    foreign_counterParty_ & foreign_submittingBrokerId_
        book_, received_, price_, volume_
    """
    start_time, end_time, date_to_pull = to_milli_timestamp(filter.start_time) * 1e6, to_milli_timestamp(
        filter.end_time) * 1e6, filter.start_time.format('YYYYMMDD')

    final_list = []
    return final_list

@provides('optiver_trades')
def optiver_trades(filter, book_ids):
    return id_dict(trade for trade in get_trades(filter, book_ids))
