import bisect

from foba_backtest_engine.utils.FeeTickScheduler.StaticDataInfo import (
    StaticDataEnrichment,
)


def default_static_data_info_enrichment():
    return StaticDataEnrichment(
        exchange=None,
        product_symbol=None,
        product_class=None,
        contract_size=1,
        round_lot_size=1,
        tick_size=0.01,
        fees=0,
    )


def _calculate_tick_size(tick_schedule, event):
    """
    :param tick_schedule: list of tick prices/increments
    :param event: a FobaEvent
    :return: tick size for the event
    """
    if not tick_schedule:
        return float("nan")
    i = bisect.bisect_right([x.min_price for x in tick_schedule], event.event_price)
    return tick_schedule[i - 1].increment


def _calculate_fee(book_info, event, excluded_fee_names=()):
    """
    Utility function to calculate a trade's fee
    :param book_info: StaticDataInfo
    :param event: a FobaEvent
    :param excluded_fee_names: List/tuple of excluded fee name
    :return: total fee
    """
    total_fee = 0
    for rule in book_info.fee_rules:
        if rule.name in excluded_fee_names:
            continue
        if rule.charged_unit == "VOLUME":
            fee = event.event_volume * rule.cost
        elif rule.charged_unit == "MARKET_VALUE":
            fee = (
                event.event_volume
                * book_info.contract_size
                * event.event_price
                * rule.cost
            ) / 10000
        elif rule.charged_unit == "TRADE":
            fee = rule.cost
        else:
            fee = 0
            print("WARNING Unsupported rule type: " + rule.chargedUnit)
        if rule.maximum:
            fee = min(fee, rule.maximum)
        if rule.minimum:
            fee = max(fee, rule.minimum)
        total_fee += fee
    return total_fee
