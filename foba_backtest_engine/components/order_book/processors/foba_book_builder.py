from foba_backtest_engine.components.order_book.builders.multi_book_builder import (
    MultiBookBuilder,
)
from foba_backtest_engine.enrichment import provides
from foba_backtest_engine.utils.base_utils import ImmutableDict


@provides("pybuilders")
def pybuilders(
    pybuilder_exchange,
    filter,
    book_build_parallel=False,
    max_workers=5,
    optiver_only=False,
    optiver_order_numbers=(),
):
    """
    Pulls data from FeedUpdates_ and builds the books using the python book builder/
    The pybuilders contain foba data and feed states, among other properties.
    :param pybuilder_exchange: foba_backtest_engine.components.order_book.utils.enums.Exchange
    :param filter: Requires: book_ids, start_time, end_time
    :return: @provides('pybuilders')
    """

    book_builder = MultiBookBuilder(
        exchange=pybuilder_exchange,
        books=filter.book_ids,
        start=filter.start_time.format("YYYY-MM-DD HH:mm:SS"),
        end=filter.end_time.format("YYYY-MM-DD HH:mm:SS"),
        optiver_only=optiver_only,
        optiver_order_numbers=optiver_order_numbers,
        filter=filter,
    )
    book_builder.build_books(book_build_parallel, max_workers)

    return ImmutableDict(
        (book_id, builder) for book_id, builder in book_builder.books.items()
    )
