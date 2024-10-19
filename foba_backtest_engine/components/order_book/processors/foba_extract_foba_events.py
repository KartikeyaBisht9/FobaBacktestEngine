from foba_backtest_engine.components.order_book.utils.enums import FobaEvent, EventType, Side, EventScore, JoinScore, OrderType
from foba_backtest_engine.utils.time_utils import to_nano_timestamp
from foba_backtest_engine.enrichment import provides, id_dict
import logging

@provides('foba_events')
def extract_foba_events(
    pybuilders, 
    filter, 
    exclude_pulls=False, 
    include_only_optiver_pulls=False,
    optiver_order_numbers=(), 
    exclude_inplace_updates=True
    ):
    """
    :param pybuilders:
    :return: @provides('foba_events')
    """
    
    logger = logging.getLogger(__name__)
    start_time_ns = to_nano_timestamp(filter.start_time)
    end_time_ns = to_nano_timestamp(filter.end_time)
        
    events = []
    for book_id, builder in pybuilders.items():
        logger.info(f"Extracting FOBA for book_id: {book_id}")
        for trade_event in builder.trades:
            if start_time_ns < trade_event.trade_received < end_time_ns:
                events.append(trade_event)
            else:
                print(trade_event.trade_received)


        if not exclude_pulls:
            for pull_event in builder.pulls:
                if start_time_ns < trade_event.trade_received < end_time_ns:
                    pull_order_number = int(str(pull_event.order_number).split('_')[0])
                    if include_only_optiver_pulls:
                        if pull_order_number in optiver_order_numbers:
                            events.append(pull_event)
                    else:
                        events.append(pull_event)
                    
        if not exclude_inplace_updates:
            for event in builder.inplace_updates:
                events.append(event)
    logger.info(f"Total FOBA events = {len(events)}")
    
    return id_dict(
        FobaEvent(
            id=None,
            book_id=event.book,
            order_number=event.order_number,
            aggressor_order_number=event.aggressor_order_number,
            side=Side.BID if event.passive_side == Side.BID else Side.ASK,
            level_id=event.book + str(event.level_created),
            level_exchange_timestamp=event.level_timestamp,
            level_driver_received=event.level_received,
            level_driver_created=event.level_created,
            level_aggressive_volume=event.level_aggressive_volume,
            join_exchange_timestamp=event.join_timestamp,
            join_driver_received=event.join_received,
            join_driver_created=event.join_created,
            join_driver_sequence_number=event.join_sequence_number,
            join_volume=event.volume_at_join,
            join_aggressive_volume=event.aggressive_volume_at_join,
            join_depth=event.depth_at_join,
            join_score=JoinScore.PERFECT,
            volume_ahead_at_join=event.volume_ahead_at_join,
            level_cumulative_joined_volume_at_join=event.rank_at_join,
            count_ahead_at_join=event.count_ahead_at_join,
            event_type=EventType.TRADE if event.type == EventType.TRADE else (EventType.INPLACE_UPDATE if event.type == EventType.INPLACE_UPDATE else EventType.PULL),
            event_exchange_timestamp=event.trade_timestamp,
            event_driver_received=event.trade_received,
            event_driver_created=event.trade_created,
            event_driver_sequence_number=event.trade_sequence_number,
            event_price=event.trade_price,
            event_volume=event.trade_volume,
            event_depth=event.depth_at_trade,
            event_score=EventScore.MATCH,
            aggressor_volume=event.aggressor_volume,
            volume_behind_at_event=event.volume_behind_at_trade,
            count_behind_at_event=event.count_behind_at_trade,
            volume_ahead_at_event=event.volume_ahead_at_trade,
            count_ahead_at_event=event.count_ahead_at_trade,
            level_volume_at_event=event.volume_behind_at_trade + event.volume_ahead_at_trade + event.trade_volume + event.volume_remaining,
            remaining_volume=event.volume_remaining,
            cumulative_pulled_volume=event.volume_pulled,
            order_type=OrderType.NORMAL,
            foreign_join_reason=event.foreign_join_reason,
            book_state=event.book_state,
            volume_behind_after_join=event.volume_behind_after_join,
            inplace_updates=event.inplace_updates,
            volume_reducing_updates=event.volume_reducing_updates,
            inplace_updates_received=event.inplace_updates_received,
            inplace_updates_depth=event.inplace_updates_depth,
            volume_reducing_updates_received=event.volume_reducing_updates_received,
            next_best_level_price=event.next_best_level_price,
            next_best_level_order_count_at_join=event.next_best_level_order_count_at_join,
            next_best_level_volume_at_join=event.next_best_level_volume_at_join,
            best_level_times=event.best_level_times
        ) for event in events
    )