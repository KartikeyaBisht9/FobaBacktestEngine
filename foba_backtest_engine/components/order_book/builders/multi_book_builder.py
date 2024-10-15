import traceback
import numpy as np
import pandas as pd
from enum import unique, Enum
from collections import defaultdict
from foba_backtest_engine.utils import futures
from foba_backtest_engine.utils.base_utils import get_logger
from foba_backtest_engine.components.order_book.utils import enums
from foba_backtest_engine.components.order_book.builders.OMDC import OmdcBookBuilder
from foba_backtest_engine.components.order_book.utils.foba_feedupdates import get_feed_updates

"""
MULTIBOOKBUILDER CLASS

This is a core class that manages execution of multiple OmdcBookBuilders for different securityCode_ (books)
i)  We can pass in feedUpdate objects (using @Provide) or this will pull in feed
ii) If parallel = True ... we will split the feed above based on securityCode_ across various workers
iii) once the books are built - we can extract trades, pull etc into pandas dataframes

"""



def process_book_events(book, events, book_id):
    for fu in events:
        book.update(fu)
    return book_id, book


class MultiBookBuilder:
    def __init__(self, exchange, books, start, end, optiver_only, optiver_order_numbers, logger=None,
                 log_triggers=False, feed_updates=None, filter=None):
        self.exchange = exchange
        self.books_string = books
        self.start = start
        self.end = end
        self.books = {}
        if logger is not None:
            self.logger = logger
        else:
            self.logger = get_logger(__name__)
        self.log_triggers = log_triggers
        self.filter = filter

        self.feed_updates_final = []
        self.unconsolidated_lds = []
        self.aggressive_ose_volumes = None
        self.get_feed_updates_final(feed_updates)

        self.optiver_only = optiver_only
        self.optiver_order_numbers = set(optiver_order_numbers)

    def get_feed_updates_final(self, feed_updates):
        """
        Needs to be run to get all the feed updates for a given set of books. Run by default on creation.
        :return:
        """
        if not feed_updates:
            self.feed_updates_final = get_feed_updates(self.exchange, filter=self.filter)

        else:
            self.feed_updates_final = feed_updates


    def build_books(self, parallel=False, max_workers=5):
        log_triggers = self.log_triggers
        trade_change_reason = False
        if self.exchange == enums.Exchange.OMDC:
            book_builder = OmdcBookBuilder
        else:
            raise ValueError("Could not find a book builder for the exchange: {}".format(self.exchange))

        self.logger.debug('build_books using ' + book_builder.__name__ + ': Started')
        self.logger.debug('build_books using ' + book_builder.__name__ + ': Timerange - ' + self.start + '-' + self.end)

        feed_update_count = len(self.feed_updates_final)
        self.logger.debug('build_books using ' + book_builder.__name__ + ': feed_update_count - ' + repr(feed_update_count))

        progress_percent = 0.1
        processed_count = 0

        if not parallel:
            for fu in self.feed_updates_final:
                if ~self.optiver_only or fu.order_number in self.optiver_order_numbers:
                    try:
                        if fu.book not in self.books.keys():
                            self.books[fu.book] = book_builder(fu.book, log_triggers=log_triggers,
                                                                   trade_change_reason=trade_change_reason)
                        self.books[fu.book].update(fu)
                    except Exception as exception:
                        traceback.print_exception(type(exception), exception, exception.__traceback__)
                        raise Exception('Exception while processing feed update: {}'.format(fu)) from exception

                    processed_count += 1
                    if processed_count / feed_update_count >= progress_percent:
                        self.logger.debug('build_books using ' + book_builder.__name__ + ': Percent Complete - ' +
                                          str(round(progress_percent, 1)))
                        progress_percent += 0.1
        else:
            self.logger.debug('BUILDING BOOK IN PARALLEL')
            book_processed_count = 0
            per_book_feed_updates = defaultdict(list)
            for fu in self.feed_updates_final:
                if ~self.optiver_only or fu.order_number in self.optiver_order_numbers:
                    per_book_feed_updates[fu.book].append(fu)
            num_books = len(per_book_feed_updates)

            exchange = self.exchange
            with futures.make_executor(parallel=parallel, max_workers=max_workers) as executor:
                def book_event_futures():
                    for book_id, events in per_book_feed_updates.items():
                        book = book_builder(book_id, log_triggers=log_triggers,
                                                trade_change_reason=trade_change_reason)
                        yield executor.submit(process_book_events, book, events, book_id)

                for future in futures.as_completed(book_event_futures()):
                    book_processed_count += 1
                    if book_processed_count / num_books >= progress_percent:
                        self.logger.debug('build_books using ' + book_builder.__name__ + ': Percent Complete - ' +
                                          str(round(progress_percent, 1)))
                        progress_percent += 0.1
                    book_id, book = future.result()
                    self.books[book_id] = book
        self.logger.debug('build_books using ' + book_builder.__name__ + ': Completed')

    def get_result_as_df(self, include_etfs=None, include_pulls=False):
        all_events = []
        for b in self.books:
            if (include_etfs is None) or include_etfs:
                all_events = all_events + [vars(t) for t in self.books[b].trades]
                if include_pulls:
                    all_events = all_events + [vars(p) for p in self.books[b].pulls]
            else:
                for t in self.books[b].trades:
                    if not t.is_etf:
                        all_events.append(vars(t))
                if include_pulls:
                    for p in self.books[b].pulls:
                        if not p.is_etf:
                            all_events.append(vars(p))
        df = pd.DataFrame(all_events)
        if len(df) > 0:
            my_fields = list(all_events[0].keys())
            my_enum_fields = [f for f in my_fields if isinstance(all_events[0][f], Enum)]
            for f in my_enum_fields:
                df[f] = df[f].apply(lambda x: x.name)
            df = df.sort(columns=['trade_created'], ascending=True)

        return df