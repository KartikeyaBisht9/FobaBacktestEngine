import operator
from collections import deque
import itertools
import bisect


class Level:
    def __init__(self, initializing_order):
        self.price = initializing_order.price
        self.side = initializing_order.side
        self.start_created = initializing_order.created
        self.start_received = initializing_order.received
        self.start_timestamp = initializing_order.timestamp
        self.end_created = None
        self.end_received = None
        self.end_timestamp = None
        self.orders = [initializing_order]
        self.last_dones = []
        self.pulls = []
        self.volume_on_level = initializing_order.volume
        self.volume_through_level = initializing_order.volume
        self.volume_traded_on_level = 0
        self.volume_pulled_on_level = 0
        self.aggressive_volume = initializing_order.aggressive_volume_at_join

    @property
    def number_of_orders_on_level(self):
        return len(self.orders)

    @property
    def largest_order_on_level(self):
        return max([abs(x.volume) for x in self.orders])

    def _adjust_volume_through_level(self, order_volume):
        self.volume_through_level += order_volume

    def add_order_to_level(self, order):
        order.calculate_join_statistics()
        self.volume_on_level += order.volume

        for o in self.orders:
            o.volume_behind += order.volume
            o.count_behind += 1

        self.orders.append(order)
        self._adjust_volume_through_level(order.volume)

    def remove_order_from_level(self, order):
        self.volume_on_level -= order.volume
        index = self.orders.index(order)

        if index != 0:
            for o in self.orders[:index]:
                o.volume_behind -= order.volume
                o.count_behind -= 1

        if index != len(self.orders) + 1:
            for o in self.orders[index + 1:]:
                o.volume_ahead -= order.volume
                o.count_ahead -= 1

        self.orders.remove(order)

    def update_volume(self, order, volume_to_remove):
        self.volume_on_level -= volume_to_remove
        index = self.orders.index(order)

        if index != 0:
            for o in self.orders[:index]:
                o.volume_behind -= volume_to_remove

        if index != len(self.orders) + 1:
            for o in self.orders[index + 1:]:
                o.volume_ahead -= volume_to_remove

    def close_level(self, closing_message):
        self.end_created = closing_message.created
        self.end_received = closing_message.received
        self.end_timestamp = closing_message.timestamp

    def append_last_done(self, trade):
        self.last_dones.append(trade)
        self.volume_traded_on_level += trade.trade_volume

    def append_pull(self, pull):
        self.pulls.append(pull)
        self.volume_pulled_on_level += pull.trade_volume

class LevelManager:
    def __init__(self, is_bid):
        self.closed_levels = []
        self.open_levels = {}
        self.unknown_last_dones = []
        self.unknown_pulls = []

        self.is_bid = is_bid
        self.best_price = None
        self.ordered_prices = deque()

        self._best = max if self.is_bid else min
        self._better = operator.gt if self.is_bid else operator.lt
        self._worse = operator.lt if self.is_bid else operator.gt
        self._worse_equal = operator.le if self.is_bid else operator.ge
        self.better_equal = operator.ge if self.is_bid else operator.le

    def _initialize_level(self, initializing_order):
        my_level = Level(initializing_order)
        self.open_levels[initializing_order.price] = my_level
        initializing_order.give_associated_level_object(my_level)

    def _close_levels(self, level, message):
        del self.open_levels[level.price]
        level.close_level(message)
        self.closed_levels.append(level)

    def _sorted_prices_append_middle(self, price):
        if self.is_bid:
            self.ordered_prices.reverse()
        pos = bisect.bisect_left(self.ordered_prices, price)
        self.ordered_prices.rotate(-pos)
        self.ordered_prices.appendleft(price)
        self.ordered_prices.rotate(pos)
        if self.is_bid:
            self.ordered_prices.reverse()

    def _sorted_prices_pop_middle(self, price):
        if self.is_bid:
            self.ordered_prices.reverse()
        del self.ordered_prices[bisect.bisect_left(self.ordered_prices, price)]
        if self.is_bid:
            self.ordered_prices.reverse()

    def process_add_message(self, order):
        if order.level is None:
            order.give_depth_at_join(self.get_distance(order.price))
            next_best = self.get_next_best_level(order.price)
            if next_best:
                order.give_dime_info_at_join(next_best)
        if order.price in self.open_levels.keys():
            order.give_associated_level_object(self.open_levels[order.price])
            self.open_levels[order.price].add_order_to_level(order)
        else:
            self._initialize_level(order)
            if order.price is not None:
                if len(self.ordered_prices) == 0 or self._better(order.price, self.ordered_prices[0]):
                    self.ordered_prices.appendleft(order.price)
                    self.best_price = self.ordered_prices[0]
                else:
                    self._sorted_prices_append_middle(order.price)

    def process_delete_message(self, message, orders, implied_price=False):
        price = orders[message.order_number].price if not implied_price else implied_price
        my_level = self.open_levels[price]
        my_level.remove_order_from_level(orders[message.order_number])
        if len(my_level.orders) == 0:
            self._close_levels(my_level, message)
            if price == self.ordered_prices[0]:
                self.ordered_prices.popleft()
                self.best_price = None if len(self.ordered_prices) == 0 else self.ordered_prices[0]
            else:
                self._sorted_prices_pop_middle(price)

    def enrich_open_levels_with_last_done(self, last_done, order_manager):
        if last_done.trade_volume > 0:
            level_of_trade = order_manager.orders[last_done.order_number].level
            if level_of_trade is self.open_levels[last_done.trade_price]:
                level_of_trade.append_last_done(last_done)
            else:
                self.unknown_last_dones.append(last_done)

    def enrich_open_levels_with_pull(self, pull, order_manager):
        if pull.trade_volume > 0:
            level_of_trade = order_manager.orders[pull.order_number].level
            if level_of_trade is self.open_levels[pull.trade_price]:
                level_of_trade.append_pull(pull)
            else:
                self.unknown_pulls.append(pull)

    def is_aggressive_against(self, price):
        if self.best_price is None:
            return False
        if price is None:
            return True
        return True if self._worse_equal(price, self.best_price) else False

    def get_levels(self, num):
        prices = list(itertools.islice(self.ordered_prices, 0, num))
        volumes = [self.open_levels[p].volume_on_level for p in prices]

        num_empty = num - len(prices)

        return list(zip(prices, volumes)) + [(None, None)] * num_empty

    def get_non_empty_levels(self, num):
        prices = list(itertools.islice(self.ordered_prices, 0, num))
        volumes = [self.open_levels[p].volume_on_level for p in prices]

        return list(zip(prices, volumes))

    def get_count_on_levels(self, num):
        prices = list(itertools.islice(self.ordered_prices, 0, num))
        count = [self.open_levels[p].number_of_orders_on_level for p in prices]
        largest_order = [self.open_levels[p].largest_order_on_level for p in prices]

        num_empty = num - len(prices)

        return list(zip(count, largest_order)) + [(None, None)] * num_empty

    def get_distance(self, price):
        if price is None \
                or not self.ordered_prices \
                or self._better(price, self.ordered_prices[0]):
            return -1
        else:
            num_in_front = 0
            while len(self.ordered_prices) > num_in_front \
                    and self._better(self.ordered_prices[num_in_front], price):
                num_in_front += 1
            return num_in_front

    def get_next_best_level(self, price):
        if price is None or not self.ordered_prices:
            return None
        else:
            try:
                return next(self.open_levels[level_price] for level_price in self.ordered_prices
                            if self._worse(level_price, price))
            except StopIteration:
                return None

    def update_order_best_level_times_for_add(self, order):
        order.best_level_times = []

        distance = self.get_distance(order.price)
        if distance <= 0:
            order.best_level_times.append([order.created, None])

        if distance < 0 and self.ordered_prices:
            for o in self.open_levels[self.ordered_prices[0]].orders:
                o.best_level_times[-1][-1] = order.created

    def update_order_best_level_times_for_delete(self, message, orders, implied_price=False):
        price = orders[message.order_number].price if not implied_price else implied_price

        if price == self.best_price:
            orders[message.order_number].best_level_times[-1][-1] = message.created
            if len(self.open_levels[price].orders) == 1 and len(self.ordered_prices) > 1 and not message.volume:
                for o in self.open_levels[self.ordered_prices[1]].orders:
                    o.best_level_times.append([message.created, None])
