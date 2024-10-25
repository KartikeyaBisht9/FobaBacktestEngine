from foba_backtest_engine.utils.base_utils import ReprMixin
from foba_backtest_engine.components.order_book.utils import enums
from collections import namedtuple
from abc import ABCMeta, abstractmethod

"""
ORDER CLASS

High level class that stored all Add | Delete | Modify orders we receive.

Records
a) General fields - created, received, timestamp, sequence_number, price, volume, side
b) Aggressive Volume
    - Aggressive if the ADD order immediately matches
    - Has an aggressive volume (i.e volume that matches) & non-aggressive volume (posted)

c) Order statistics - these track the order's position ... rank_at_join, count_ahead_at_join etc

d) Quote Type
    - CROSS: If the order crosses the spread (aggressive volume).
    - JOIN: If the order matches the best price in the book.
    - DIME or DEEPER_JOIN: If the order improves or worsens the spread.

e) Best level information - stores when the order was on the best bid/ask 

"""

class Order(ReprMixin):
    def __init__(self,
                 message,
                 price,
                 triggers=None,
                 trigger_reason=None,
                 foreign_constructor=None,
                 aggressive_volume=0):
        self.created = message.created
        self.received = message.received
        self.timestamp = message.timestamp
        self.sequence_number = message.sequence_number
        self.price = price
        self.side = message.side
        self.volume = message.volume
        self.volume_at_join = message.volume + aggressive_volume
        self.aggressive_volume_at_join = aggressive_volume
        self.aggressive_at_join = True if aggressive_volume > 0 else False
        self.order_number = message.order_number
        self.volume_pulled = 0
        # The following order priority trackers are updated when order is assigned a 'level' object
        self.count_ahead_at_join = 0
        self.volume_ahead_at_join = 0
        self.rank_at_join = 0
        self.volume_ahead = 0
        self.count_ahead = 0
        self.volume_behind = 0
        self.count_behind = 0
        self.level = None
        self.depth_at_join = 0
        self.quote_type = None
        self.foreign_fields = None
        if foreign_constructor:
            self.foreign_fields = foreign_constructor(message)
        self.passive_triggers = triggers
        self.passive_trigger_reason = trigger_reason
        self.inplace_updates = 0
        self.volume_reducing_updates = 0
        self.inplace_updates_received = []
        self.volume_reducing_updates_received = []
        self.inplace_updates_depth = []
        self.next_best_level = None
        self.next_best_level_price = None
        self.count_on_next_best_level_at_join = 0
        self.volume_on_next_best_level_at_join = 0
        self.best_level_times = None

    def give_associated_level_object(self, level):
        if self.level is None:
            self.level = level

    def give_depth_at_join(self, distance):
        self.depth_at_join = distance
        self.quote_type = self.determine_quote_type()

    def give_dime_info_at_join(self, next_best_level):
        self.next_best_level = next_best_level
        self.next_best_level_price = next_best_level.price
        self.count_on_next_best_level_at_join = next_best_level.number_of_orders_on_level
        self.volume_on_next_best_level_at_join = next_best_level.volume_on_level

    def calculate_join_statistics(self):
        self.count_ahead_at_join = self.level.number_of_orders_on_level
        self.volume_ahead_at_join = self.level.volume_on_level
        self.rank_at_join = self.level.volume_through_level
        self.count_ahead = self.level.number_of_orders_on_level
        self.volume_ahead = self.level.volume_on_level

    def determine_quote_type(self):
        if self.price is None:
            return enums.QuoteType.UNKNOWN
        if self.aggressive_volume_at_join > 0:
            return enums.QuoteType.CROSS
        if self.depth_at_join < 0:
            return enums.QuoteType.DIME
        elif self.depth_at_join == 0:
            return enums.QuoteType.JOIN
        else:
            return enums.QuoteType.DEEPER_JOIN


"""
ORDER MAANGER

High level class that manages the collection of Order Objects

a) Add      ... we add an order based on its 'order_number'
b) Delete   ... deletes order from the orders dict
c) update   ... updates the order as well as the 'level'

"""

class OrderManager:
    def __init__(self):
        self.orders = {}

    def add(self, order_number, quote):
        self.orders[order_number] = quote

    def delete(self, order_number):
        del self.orders[order_number]

    def update_volume(self, message):
        volume_change = self.orders[message.order_number].volume - message.volume
        if volume_change < 0:
            raise ValueError("update_volume should iponly be called for volume decreases")
        self.orders[message.order_number].level.update_volume(self.orders[message.order_number], volume_change)
        self.orders[message.order_number].volume = message.volume

    def update_volume_pulled(self, message):
        volume = message.volume if message.volume is not None else 0
        volume_pulled = self.orders[message.order_number].volume - volume
        self.orders[message.order_number].volume_pulled += volume_pulled

    def increment_number_updates(self, message, depth):
        if message.volume < self.orders[message.order_number].volume:
            self.orders[message.order_number].volume_reducing_updates += 1
            self.orders[message.order_number].volume_reducing_updates_received.append(message.received)
        elif message.volume == self.orders[message.order_number].volume:
            self.orders[message.order_number].inplace_updates += 1
            self.orders[message.order_number].inplace_updates_received.append(message.received)
            self.orders[message.order_number].inplace_updates_depth.append(depth)


