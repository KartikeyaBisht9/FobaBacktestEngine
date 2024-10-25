from foba_backtest_engine.components.order_book.utils import enums

"""
FOBAEVENT
- represents a event in the order-book .. trade or pull

"""


class FobaEvent:
    def __init__(
        self, message, orders, event_type=enums.EventType.TRADE, event_depth=0
    ):
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
        self.trade_volume = (
            orders[message.order_number].volume
            - int(self.is_partial) * self.volume_remaining
        )
        self.aggressor_volume = (
            self.trade_volume
        )  # Must be updated by book builder if multi lds for single aggressor
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
        self.aggressive_volume_at_join = orders[
            message.order_number
        ].aggressive_volume_at_join
        self.level_aggressive_volume = orders[
            message.order_number
        ].level.aggressive_volume
        self.aggressor_order_number = (
            -1
            if message.aggressor_order_number is None
            else message.aggressor_order_number
        )

        self.foreign_join_reason = None
        if hasattr(orders[message.order_number], "foreign_fields"):
            if hasattr(
                orders[message.order_number].foreign_fields, "foreign_join_reason"
            ):
                self.foreign_join_reason = orders[
                    message.order_number
                ].foreign_fields.foreign_join_reason

        self.book_state = None
        self.volume_behind_after_join = None
        self.inplace_updates = orders[message.order_number].inplace_updates
        self.volume_reducing_updates = orders[
            message.order_number
        ].volume_reducing_updates
        self.inplace_updates_received = orders[
            message.order_number
        ].inplace_updates_received
        self.inplace_updates_depth = (
            orders[message.order_number].inplace_updates_depth
            if orders[message.order_number].inplace_updates_depth
            else None
        )
        self.volume_reducing_updates_received = orders[
            message.order_number
        ].volume_reducing_updates_received

        self.next_best_level_price = orders[message.order_number].next_best_level_price
        self.next_best_level_order_count_at_join = orders[
            message.order_number
        ].count_on_next_best_level_at_join
        self.next_best_level_volume_at_join = orders[
            message.order_number
        ].volume_on_next_best_level_at_join

        self.best_level_times = orders[message.order_number].best_level_times
