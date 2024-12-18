from operator import attrgetter

import pandas as pd

from foba_backtest_engine.components.order_book.utils import MyRow
from foba_backtest_engine.enrichment import provides
from foba_backtest_engine.utils.base_utils import (
    ImmutableDict,
    invert_dict_to_dict_list,
    multi_dict,
)
from foba_backtest_engine.utils.FeeTickScheduler.StaticDataInfo import StaticDataInfo

"""
STATIC DATA BOOK INFO 

This is a class that contains various static data such as fee schedule etc which is directly parsed from a dataframe

"""


@provides("static_data_info")
def static_data_info(filter, product_filter=None):
    """
    Provides an ImmutableDict of StaticDataInfo
    """
    feeInfo = pd.read_feather(filter.fee_info_path)
    tickSchedule = pd.read_feather(filter.tick_schedule_path)
    feeSchedule = pd.read_feather(filter.fee_schedule_path)

    # feeInfo = pd.read_feather(
    #     "/Users/kartikeyabisht/FobaBacktestEngine/temp_data/FeeInfo.feather"
    # )
    # tickSchedule = pd.read_feather(
    #     "/Users/kartikeyabisht/FobaBacktestEngine/temp_data/TickSchedule.feather"
    # )
    # feeSchedule = pd.read_feather(
    #     "/Users/kartikeyabisht/FobaBacktestEngine/temp_data/FeeSchedule.feather"
    # )

    books = []
    for _, row in feeInfo.iterrows():
        row_dict = row.to_dict()
        books.append(MyRow(row_dict))

    tick_schedules_list = []
    for _, row in tickSchedule.iterrows():
        row_dict = row.to_dict()
        tick_schedules_list.append(MyRow(row_dict))

    fee_schedules_list = []
    for _, row in feeSchedule.iterrows():
        row_dict = row.to_dict()
        fee_schedules_list.append(MyRow(row_dict))

    tick_schedules = multi_dict(tick_schedules_list, attrgetter("tick_schedule_id"))
    fee_schedules = multi_dict(fee_schedules_list, attrgetter("fee_schedule_id"))

    product_filter = (
        [b.product_symbol for b in books] if product_filter is None else product_filter
    )
    inverted_dict = invert_dict_to_dict_list(
        {b.product_symbol: b.book_id for b in books}
    )
    product_filter = [
        v[0]
        if len(v) == 1
        else [i for i in v if i in product_filter][0]
        if len([i for i in v if i in product_filter]) == 1
        else v[0]
        for k, v in inverted_dict.items()
    ]

    return ImmutableDict(
        (
            str(book.book_id),
            StaticDataInfo(
                book.book_id,
                book.product_symbol,
                book.product_class,
                book.contract_size,
                book.exchange,
                book.round_lot_size,
                tick_schedules.get(book.tick_schedule_id, ()),
                fee_schedules.get(book.fee_schedule_id, ()),
            ),
        )
        for book in books
        if book.product_symbol in product_filter
    )
