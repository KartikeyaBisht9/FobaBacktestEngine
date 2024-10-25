import warnings

from foba_backtest_engine.components.order_book.utils import enums
from foba_backtest_engine.components.order_book.utils.foba_events import FobaEvent
from foba_backtest_engine.components.order_book.utils.foba_levels import (
    LevelManager,
)
from foba_backtest_engine.components.order_book.utils.foba_order import (
    Order,
    OrderManager,
)
from foba_backtest_engine.components.order_book.utils.foba_states import (
    FeedState,
    OrderCountState,
)
from foba_backtest_engine.components.order_book.utils.order_queue import OrderQueue

"""
OMDC Book Builder

Central class that does all the order management, broker queue management etc

a) Structure
    - bids & asks are handled by the OrderManager ... these stores orders in a hash-map {order_number : Order (obj)}
    
    - bid_levels & ask_levels are handled by the LevelManager  
        ... the level prices are stored in a deque (sorted)
        ... each price maps to a Level object which stored our orders in a list maintained according to FIFO
        ... the orders are stored as Order objects w/ various attributes

    - order_queues ... are handled by the OrderQueue class
        ... we have separate bid & ask queues
        ... for each queue ... the strcture is like so:

            QueueLevel = List<Price, List<OrderNumber(s)>>
            Queue = Pair <createdNanos_, sortedList<QueueLevel>>

                    e.g. the orderbook at time = 1728565192000000000 w/ 2 orders at P = 101.5 & 1 at P = 101.0 is:
                        [1010, [[101.5, [1, 3]], [101.0, [2]]]]]

b) Methods/execution flow
    - We take raw feed separated in 4 dataframes:   ADD | MODIFY | DELETE | BROKER_QUEUE
    - We store rows from these dataframes as FeedUpdate_ objects & use this as a "standardized" input into our OmdcBookBuilder engine

    We have the update method that parses FeedUpdate_ rows & routes it to further processors that deal w/ different event types
    
"""


class OmdcBookBuilder:
    def __init__(self, book_id, **kwargs):
        self.book = book_id
        self.bids = OrderManager()
        self.asks = OrderManager()
        self.bid_levels = LevelManager(is_bid=True)
        self.ask_levels = LevelManager(is_bid=False)

        self.persist_order_queue = True
        self.bid_order_queue = OrderQueue(bid_side=True)
        self.ask_order_queue = OrderQueue(bid_side=False)

        self.trades = []
        self.pulls = []

        self.feed_states = []
        self.order_count_states = []
        self.event_trades = []

    def total_traded_volume(self):
        return sum([t.trade_volume for t in self.trades])

    def update(self, message):
        """
        Gets called for each FeedUpdate_ row.  From here it's passed to ADD, DELETE, UPDATE accordingly.
        :param message: The FeedUpdate row.  See db_to_feed_update.
        :return:
        """
        is_bid = message.side == enums.Side.BID
        order_manager = self.bids if is_bid else self.asks
        level_manager = self.bid_levels if is_bid else self.ask_levels
        is_trade = message.change_reason == 3
        is_multi_trade_end = message.inferred == 1 and len(self.event_trades) > 0

        if message.command == enums.Command.ADD:
            self.update_add(message, order_manager, level_manager, is_multi_trade_end)
        elif message.command == enums.Command.DELETE:
            self.update_delete(message, order_manager, level_manager, is_trade)
        elif message.command == enums.Command.UPDATE:
            if message.order_number not in order_manager.orders:
                self.update_add(message, order_manager, level_manager, False)
            self.update_update(message, order_manager, level_manager, is_trade)
        else:
            return

        if is_multi_trade_end:
            self.update_event_trades()

        if message.inferred == 1:
            self.event_trades = []

        self.append_feed_state(message, is_trade)
        self.append_order_count_state(message)

    def update_add(self, message, order_manager, level_manager, is_boov):
        """
        This processes an order ADD message.
        :param message: The FeedUpdate row.  See db_to_feed_update.
        :return:
        """

        aggressive_volume = 0
        if is_boov:
            aggressive_volume = sum(x.trade_volume for x in self.event_trades)
        price = message.price if message.price else level_manager.best_price
        order = Order(message, price, aggressive_volume=aggressive_volume)
        order_manager.add(message.order_number, order)
        level_manager.update_order_best_level_times_for_add(order)
        level_manager.process_add_message(order)
        if self.persist_order_queue:
            if message.side == enums.Side.BID:
                self.bid_order_queue.add(
                    message.timestamp, price, int(message.order_number)
                )
            else:
                self.ask_order_queue.add(
                    message.timestamp, price, int(message.order_number)
                )

    def update_delete(self, message, order_manager, level_manager, is_trade):
        """
        This processes an order DELETE message.
        :param message: The FeedUpdate row.  See db_to_feed_update.
        :return:
        """
        if message.order_number not in order_manager.orders:
            return
        if is_trade:
            self.process_trade(message, order_manager, level_manager)
        else:
            order_manager.update_volume_pulled(message)
            self.process_pull(message, order_manager, level_manager)
        price = order_manager.orders[message.order_number].price
        level_manager.process_delete_message(message, order_manager.orders)
        order_manager.delete(message.order_number)
        if self.persist_order_queue:
            if message.side == enums.Side.BID:
                self.bid_order_queue.delete(
                    message.timestamp, price, message.order_number
                )
            else:
                self.ask_order_queue.delete(
                    message.timestamp, price, message.order_number
                )

    def update_update(self, message, order_manager, level_manager, is_trade):
        """
        This processes an order UPDATE message.
        :param message: The FeedUpdate row.  See db_to_feed_update.
        :return:
        """
        if is_trade:
            self.process_trade(message, order_manager, level_manager)

        if message.volume > order_manager.orders[message.order_number].volume:
            if is_trade:
                raise ValueError("Cannot increase volume on last done!")
            implied_price = order_manager.orders[message.order_number].price
            level_manager.process_delete_message(
                message, order_manager.orders, implied_price
            )
            order_manager.delete(message.order_number)
            new_order = Order(message, implied_price)
            order_manager.add(message.order_number, new_order)
            level_manager.process_add_message(new_order)
            if self.persist_order_queue:
                if message.side == enums.Side.BID:
                    self.bid_order_queue.update(
                        message.timestamp,
                        implied_price,
                        message.order_number,
                        volume_decrease=False,
                    )
                else:
                    self.ask_order_queue.update(
                        message.timestamp,
                        implied_price,
                        message.order_number,
                        volume_decrease=False,
                    )
        elif message.volume <= order_manager.orders[message.order_number].volume:
            if not is_trade:
                order_manager.update_volume_pulled(message)
                self.process_pull(message, order_manager, level_manager)
            order_manager.update_volume(message)
            price = order_manager.orders[message.order_number].price
            if self.persist_order_queue:
                if message.side == enums.Side.BID:
                    self.bid_order_queue.update(
                        message.timestamp,
                        price,
                        message.order_number,
                        volume_decrease=True,
                    )
                else:
                    self.ask_order_queue.update(
                        message.timestamp,
                        price,
                        message.order_number,
                        volume_decrease=True,
                    )

    def process_trade(self, message, order_manager, level_manager):
        """
        Called when an UPDATE or DELETE is a result of a trade.
        A trade is created and appended to both the builder and the level within the builder.

        :param message: The FeedUpdate row.  See db_to_feed_update.  In this case an UPDATE or DELETE on a passive side.
        :param order_manager: The side order manager.
        :param level_manager: The side level manager.
        :return:
        """
        level_manager.update_order_best_level_times_for_delete(
            message, order_manager.orders
        )
        trade = FobaEvent(message, order_manager.orders)
        self.trades.append(trade)
        self.event_trades.append(trade)
        level_manager.enrich_open_levels_with_last_done(trade, order_manager)

    def process_pull(self, message, order_manager, level_manager):
        """
        Called when an UPDATE or DELETE is a result of a pull.
        A pull is created and appended to both the builder and the level within the builder.

        :param message: The FeedUpdate row.  See db_to_feed_update.  In this case an UPDATE or DELETE on a passive side.
        :param order_manager: The side order manager.
        :param level_manager: The side level manager.
        :return:
        """
        level_manager.update_order_best_level_times_for_delete(
            message, order_manager.orders
        )
        pull = FobaEvent(
            message,
            order_manager.orders,
            event_type=enums.EventType.PULL,
            event_depth=level_manager.get_distance(
                order_manager.orders[message.order_number].price
            ),
        )
        self.pulls.append(pull)
        level_manager.enrich_open_levels_with_pull(pull, order_manager)

    def append_feed_state(self, message, is_trade):
        retail_bids = self.bid_levels.get_levels(5)
        retail_asks = self.ask_levels.get_levels(5)
        if is_trade:
            price_ = self.trades[-1].trade_price
            volume_ = self.trades[-1].trade_volume
            foreignLd_aggressorSide_ = "S" if message.side is enums.Side.BID else "B"
            isTrade_ = 1
        else:
            price_ = None
            volume_ = None
            foreignLd_aggressorSide_ = " "
            isTrade_ = 0

        feed_state = FeedState(
            bookId_=message.book,
            createdNanos_=message.created,
            received_=message.received,
            timestamp_=message.timestamp,
            bids_0_price_=retail_bids[0][0],
            bids_0_volume_=retail_bids[0][1],
            bids_1_price_=retail_bids[1][0],
            bids_1_volume_=retail_bids[1][1],
            bids_2_price_=retail_bids[2][0],
            bids_2_volume_=retail_bids[2][1],
            bids_3_price_=retail_bids[3][0],
            bids_3_volume_=retail_bids[3][1],
            bids_4_price_=retail_bids[4][0],
            bids_4_volume_=retail_bids[4][1],
            asks_0_price_=retail_asks[0][0],
            asks_0_volume_=retail_asks[0][1],
            asks_1_price_=retail_asks[1][0],
            asks_1_volume_=retail_asks[1][1],
            asks_2_price_=retail_asks[2][0],
            asks_2_volume_=retail_asks[2][1],
            asks_3_price_=retail_asks[3][0],
            asks_3_volume_=retail_asks[3][1],
            asks_4_price_=retail_asks[4][0],
            asks_4_volume_=retail_asks[4][1],
            price_=price_,
            volume_=volume_,
            foreignLd_aggressorSide_=foreignLd_aggressorSide_,
            isTrade_=isTrade_,
        )

        self.feed_states.append(feed_state)

    def append_order_count_state(self, message):
        retail_bids = self.bid_levels.get_count_on_levels(5)
        retail_asks = self.ask_levels.get_count_on_levels(5)
        order_count_state = OrderCountState(
            bookId_=message.book,
            createdNanos_=message.created,
            received_=message.received,
            timestamp_=message.timestamp,
            bids_0_count_=retail_bids[0][0],
            bids_0_max_volume_=retail_bids[0][1],
            bids_1_count_=retail_bids[1][0],
            bids_1_max_volume_=retail_bids[1][1],
            bids_2_count_=retail_bids[2][0],
            bids_2_max_volume_=retail_bids[2][1],
            bids_3_count_=retail_bids[3][0],
            bids_3_max_volume_=retail_bids[3][1],
            bids_4_count_=retail_bids[4][0],
            bids_4_max_volume_=retail_bids[4][1],
            asks_0_count_=retail_asks[0][0],
            asks_0_max_volume_=retail_asks[0][1],
            asks_1_count_=retail_asks[1][0],
            asks_1_max_volume_=retail_asks[1][1],
            asks_2_count_=retail_asks[2][0],
            asks_2_max_volume_=retail_asks[2][1],
            asks_3_count_=retail_asks[3][0],
            asks_3_max_volume_=retail_asks[3][1],
            asks_4_count_=retail_asks[4][0],
            asks_4_max_volume_=retail_asks[4][1],
            isTrade_=0,
        )
        self.order_count_states.append(order_count_state)

    def update_event_trades(self):
        aggressor_volume = sum([t.trade_volume for t in self.event_trades])
        count_from_trade = 0
        volume_from_trade = 0
        for trade in self.event_trades:
            trade.aggressor_volume = aggressor_volume
            trade.count_ahead_at_trade += count_from_trade
            trade.volume_ahead_at_trade += volume_from_trade
            count_from_trade += 1
            volume_from_trade += trade.trade_volume
