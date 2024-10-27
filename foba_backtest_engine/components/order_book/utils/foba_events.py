from foba_backtest_engine.components.order_book.utils import enums

"""
FOBAEVENT
- represents a event in the order-book .. trade or pull

"""

class FobaEvent:
    def __init__(self, message, orders, event_type=enums.EventType.TRADE, event_depth=0):
        self.order_number = message.order_number
        self.book = message.book
        self.type = event_type
        self.quote_type = orders[message.order_number].quote_type
        self.level_created = orders[message.order_number].level.start_created
        self.level_received = orders[message.order_number].level.start_received
        self.level_timestamp = orders[message.order_number].level.start_timestamp
        self.join_created = orders[message.order_number].created
        self.join_received = orders[message.order_number].received
        self.join_timestamp = orders[message.order_number].timestamp
        self.join_sequence_number = orders[message.order_number].sequence_number
        self.trade_created = message.created
        self.trade_received = message.received
        self.trade_timestamp = message.timestamp
        self.trade_sequence_number = message.sequence_number
        self.volume_remaining = 0 if message.volume is None else message.volume
        self.is_partial = True if message.command == enums.Command.UPDATE else False
        self.trade_price = orders[message.order_number].price
        self.trade_volume = orders[message.order_number].volume - int(self.is_partial) * self.volume_remaining
        self.aggressor_volume = self.trade_volume  # Must be updated by book builder if multi lds for single aggressor
        self.volume_at_join = orders[message.order_number].volume_at_join
        self.volume_pulled = orders[message.order_number].volume_pulled
        self.volume_ahead_at_join = orders[message.order_number].volume_ahead_at_join
        self.volume_ahead_at_trade = orders[message.order_number].volume_ahead
        self.count_ahead_at_join = orders[message.order_number].count_ahead_at_join
        self.count_ahead_at_trade = orders[message.order_number].count_ahead
        self.rank_at_join = orders[message.order_number].rank_at_join
        self.depth_at_join = orders[message.order_number].depth_at_join
        self.volume_behind_at_trade = orders[message.order_number].volume_behind
        self.count_behind_at_trade = orders[message.order_number].count_behind
        self.depth_at_trade = event_depth
        self.passive_side = message.side
        self.aggressive_at_join = orders[message.order_number].aggressive_at_join
        self.aggressive_volume_at_join = orders[message.order_number].aggressive_volume_at_join
        self.level_aggressive_volume = orders[message.order_number].level.aggressive_volume
        self.aggressor_order_number = -1 if message.aggressor_order_number is None else int(message.aggressor_order_number)

        self.foreign_join_reason = None
        if hasattr(orders[message.order_number], 'foreign_fields'):
            if hasattr(orders[message.order_number].foreign_fields, 'foreign_join_reason'):
                self.foreign_join_reason = orders[message.order_number].foreign_fields.foreign_join_reason

        self.book_state = None
        self.volume_behind_after_join = None
        self.inplace_updates = orders[message.order_number].inplace_updates
        self.volume_reducing_updates = orders[message.order_number].volume_reducing_updates
        self.inplace_updates_received = orders[message.order_number].inplace_updates_received
        self.inplace_updates_depth = orders[message.order_number].inplace_updates_depth if orders[message.order_number].inplace_updates_depth else None
        self.volume_reducing_updates_received = orders[message.order_number].volume_reducing_updates_received

        self.next_best_level_price = orders[message.order_number].next_best_level_price
        self.next_best_level_order_count_at_join = orders[message.order_number].count_on_next_best_level_at_join
        self.next_best_level_volume_at_join = orders[message.order_number].volume_on_next_best_level_at_join

        self.best_level_times = orders[message.order_number].best_level_times


class PtsFobaEvent(FobaEvent):
    def __init__(self, message, orders, event_type=enums.EventType.TRADE):
        super().__init__(message, orders, event_type)

        # MAST: japannext enrichers
        self.seconds_from_level_formed_to_join = (self.join_received - self.level_received) / 1e9
        self.is_imc_trade = False
        self.imc_action = 'imc_not_sent'
        # Dave added some more info that will be added down the line
        self.slipped_trade_received = None
        self.slipped_price = None
        self.slipped_profit_passive = None
        self.slipped_profit_aggressive = None
        self.slipped_trade_received_900 = None
        self.slipped_price_900 = None
        self.slipped_profit_passive_900 = None
        self.slipped_profit_aggressive_900 = None
        self.trade_date = None
        self.tse_trigger_delay_us = None
        self.puff_average_spread = None
        self.puff_bbo = None
        self.puff_average_spread_bkt = None
        self.join_created_datetime = None
        self.autotrader_name = None
        self.strategy_name = None
        self.volume_at_join_norm = None
        self.round_lot_size = None
        self.volume_at_join_round_lots = None
        self.volume_at_join_round_lots_bkt = None
        self.volume_ahead_at_join_norm = None
        self.is_setup = False
        self.total_turnover_tse = False
        self.can_borrow = None
        self.bbov_in_round_lots = None
        # Trigger info
        self.trigger_info = None
        self.passive_trigger_reason = None
        self.passive_trigger_credit_ticks = 0
        self.passive_trigger_rs_level_volume = None
        self.passive_trigger_rs = None
        self.passive_trigger_price = None
        self.passive_trigger_volume = None
        self.passive_trigger_hit_thru = False
        self.passive_trigger_time = None
        self.aggressive_trigger_reason = None
        self.aggressive_trigger_credit_ticks = 0
        self.aggressive_trigger_rs_level_volume = None
        self.aggressive_trigger_rs = None
        self.aggressive_trigger_price = None
        self.aggressive_trigger_volume = None
        self.aggressive_trigger_hit_thru = False
        self.aggressive_trigger_time = None
        self.tick_size_ratio_to_tse = None
        self.is_etf = None
        self.is_tse_trading = False
        self.self_triggers = {'passive': orders[message.order_number].passive_triggers, 'aggressive': []}
        self.self_passive_trigger_reason = orders[message.order_number].passive_trigger_reason
        self.self_aggressive_trigger_reason = 'no_trigger'
        self.trigger_book_bid_price = None
        self.trigger_book_bid_volume = None
        self.trigger_book_ask_price = None
        self.trigger_book_ask_volume = None
        # Some other variables that should be added to make sure code does not break
        self.sp_passive_early_imc = None
        self.sp_passive_late_imc = None
        self.sp_passive_early_mkt = None
        self.sp_passive_late_mkt = None
        self.sp_passive_imc = None
        self.sp_passive_mkt = None
        self.imc_is_trading = None
        self.imc_is_trading_agg = None
        self.bk_state = None
        self.bk_quoting_mode = None
        self.bk_side_enabled = None
        self.bk_output_volume = None      ## KOPA
        self.bk_side_check = None         ## KOPA
        self.bk_both_side_check = None    ## KOPA