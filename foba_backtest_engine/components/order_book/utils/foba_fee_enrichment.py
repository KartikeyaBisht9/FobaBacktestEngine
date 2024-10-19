from foba_backtest_engine.utils.FeeTickScheduler.Calculator import default_static_data_info_enrichment, _calculate_tick_size, _calculate_fee
from foba_backtest_engine.utils.FeeTickScheduler.StaticDataInfo import StaticDataEnrichment
from foba_backtest_engine.components.order_book.utils.enums import ProductClass
from foba_backtest_engine.utils.base_utils import ImmutableDict
from foba_backtest_engine.enrichment import provides, enriches


@provides('static_data_enrichment')
@enriches('foba_events')
def static_data_enrichment(foba_events, static_data_info, excluded_fee_names=("Stock Full Stamp", "Stock Full Stamp - CBBC hedge"), currency_rate=1):
    """
    Provides an ImmutableDict of StaticDataEnrichment (see named tuple defn) which are used for FobaEvent enrichment
    :param foba_events: ImmutableDict of FobaEvents which will be enriched.
    :param static_data_info: ImmutableDict of StaticDataInfo.
    :param excluded_fee_names: configuration of fee names to exclude. may or may not be supplied.
    :param currency_rate: configuration of currency rate to use for fees. may or may not be supplied.
    """
    def items():
        for event_id, event in foba_events.items():
            if event.book_id not in static_data_info:
                yield event_id, default_static_data_info_enrichment()
                continue
            book_info = static_data_info[event.book_id]
            yield event_id, StaticDataEnrichment(exchange=book_info.exchange,
                                          product_symbol=book_info.product_symbol,
                                          product_class=ProductClass(book_info.product_class),
                                          contract_size=book_info.contract_size,
                                          round_lot_size=book_info.round_lot_size,
                                          tick_size=_calculate_tick_size(book_info.tick_schedule, event),
                                          fees=_calculate_fee(book_info, event, excluded_fee_names) * currency_rate,
                                          )

    return ImmutableDict(items())