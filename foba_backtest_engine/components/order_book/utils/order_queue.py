import copy

"""
ORDER QUEUE
- This maintains a LIST of orders arranged in time-sequence form. Each entry is a "snapshot" of the orderBook at some time
- Each snapshot has a *timestamp* & a [price, [order_number]] pair where order_number = list of orders at the price
- order_numbers List is dealt w/ in FIFO manner but also note delets dont follow this rule


e.g.
order_queue.add(time=1000, price=101.5, order_number=1)  # Add order 1 at price 101.5 at time 1000
order_queue.add(time=1005, price=101.0, order_number=2)  # Add order 2 at price 101.0 at time 1005
order_queue.add(time=1010, price=101.5, order_number=3)

would result in ...
    [[1000, [[101.5, [1]]]], 
     [1005, [[101.5, [1]], [101.0, [2]]]], 
     [1010, [[101.5, [1, 3]], [101.0, [2]]]]]


Main use is in the OMDC BrokerQueue matching algorithm I have made
"""


class OrderQueue:
    def __init__(self, bid_side=None):
        self.order_queue = []
        self.prices = set()
        if bid_side is None:
            raise ValueError("Argument bid_side must be either True or False.")
        self.bid_side = bid_side

    def _better(self, new_price, existing_price):
        if self.bid_side:
            return new_price > existing_price
        return new_price < existing_price

    def add(self, time, price, order_number, inplace=False):
        if len(self.order_queue) < 1:
            curr_queue = [time, [[price, [order_number]]]]
            self.prices.add(price)
            self.order_queue.append(curr_queue)
            return

        if inplace:
            curr_queue = self.order_queue[-1]
        else:
            curr_queue = copy.deepcopy(self.order_queue[-1])
        curr_queue[0] = time

        if price not in self.prices:
            self.prices.add(price)
            if len(curr_queue[1]) < 1 or not self._better(price, curr_queue[1][-1][0]):
                curr_queue[1].append([price, [order_number]])
            else:
                for idx, entry in enumerate(curr_queue[1]):
                    if self._better(price, entry[0]):
                        curr_queue[1].insert(idx, [price, [order_number]])
                        break
        else:
            for entry in curr_queue[1]:
                if entry[0] == price:
                    entry[1].append(order_number)
                    break
        if not inplace:
            self.order_queue.append(curr_queue)

    def delete(self, time, price, order_number, remove_empty=True):
        if len(self.order_queue) < 1 or price not in self.prices:
            return

        curr_queue = copy.deepcopy(self.order_queue[-1])
        curr_queue[0] = time
        for idx, entry in enumerate(curr_queue[1]):
            if entry[0] == price:
                if order_number in entry[1]:
                    entry[1].remove(order_number)
                if len(entry[1]) == 0 and remove_empty:
                    self.prices.remove(price)
                    del curr_queue[1][idx]
                break
        self.order_queue.append(curr_queue)

    def update(self, time, price, order_number, volume_decrease):
        if volume_decrease:
            curr_queue = copy.deepcopy(self.order_queue[-1])
            curr_queue[0] = time
            self.order_queue.append(curr_queue)
            return
        self.delete(time, price, order_number, remove_empty=False)
        self.add(time, price, order_number, inplace=True)
