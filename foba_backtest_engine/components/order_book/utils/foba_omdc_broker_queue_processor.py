from foba_backtest_engine.data.S3.S3OptiverResearchActions import OPTIVER_BUCKET_ACTIONS
from foba_backtest_engine.components.order_book.utils import MyRow as MyRow
from foba_backtest_engine.utils.time_utils import to_milli_timestamp
from foba_backtest_engine.utils.base_utils import ImmutableDict
from foba_backtest_engine.enrichment import provides
from collections import namedtuple

"""
Expected BrokerQueue (for OMDC only)
    - Broker queue arrives in a conflated manner & is sided ... separate for ask and bid
    
    e.g. Ask Broker Queue example

    || PRICE  || ORDERS
       100.20  |  [1000, 2000, 1300, 5550, 1000]      (volume)
               |  [9481, 9910, 4123, 5441, 2221]      (brokerId)

       100.00  |  [4000, 5500]                        (volume)
               |  [9481, 2221]                        (brokerId)

    
               Entry:    1    2    3    4   5   6   7   8   
        BQ ->  Items:  9481 9910 4123 5441 2221 1 9498 2221
                Type:    B    B    B    B   B   S   B   B
    
    - On the FPGA .. as the BQ arrives in a byte array stream the BEST way to store the queue is item-wise
    - each row should have:
        - securityCode  (from 4 byte Uint32 in message)
        - priority_ this is a number from [0, 40] ... i.e. priority from front of BEST LEVEL (this just increments on every "B" item)
        - brokerNumber_ (extracted from the item)
        - level_    this defaults to 0 unless we receive "S" items .. then we increment by (1)

"""

OmdcBrokerQueue = namedtuple('OmdcBrokerQueue', (
    'createdNanos_',
    'received_',
    'seq_',
    'securityCode_',
    'side_',
    'priority_',
    'brokerNumber_',
    'level_',
))

def get_omdc_broker_queue(filter, security_codes):
    start_time, end_time, date_to_pull, date_filter = to_milli_timestamp(filter.start_time) * 1e6, to_milli_timestamp(
        filter.end_time) * 1e6, filter.start_time.format('YYYY-MM-DD'), int(filter.start_time.format('YYYYMMDD'))
    omdc = OPTIVER_BUCKET_ACTIONS.get_feather(path = f'OMDC/ConflatedBrokerQueue/{date_to_pull}.feather')
    omdc["securityCode_"] = omdc["securityCode_"].apply(lambda x:str(x) if isinstance(x, int) else x)
    omdc = omdc[omdc['securityCode_'].isin(security_codes)]
    omdc_filter = omdc[omdc["date_id"]==date_filter].reset_index(drop = True)
    omdc_filter_time = omdc_filter[(omdc_filter['createdNanos_'] >= start_time) & (omdc_filter['createdNanos_'] <= end_time)]
    omdc_final = omdc_filter_time.sort_values(by=['timestampNanos_', 'side_', 'priority_'])
    omdc_final_list = []
    for _, row in omdc_final.iterrows():
        row_dict = row.to_dict()
        omdc_final_list.append(MyRow(row_dict))
    return omdc_final_list


@provides('omdc_broker_queue')
def omdc_broker_queue(filter, foba_events):
    security_codes = ImmutableDict((event.book_id, event.book_id) for event in foba_events.values())
    result_dict = dict()

    books = []
    codes = []
    for book, code in security_codes.items():
        books.append(book)
        codes.append(code)

    start = 0
    step = 50
    while start < len(books):
        end = min(len(books), start + step)
        data = get_omdc_broker_queue(filter, codes[start:end])
        for book, code in zip(books[start:end], codes[start:end]):
            result_dict[book] = [e for e in data if e.securityCode_ == code]
        start = end

    return ImmutableDict((book, broker_queue) for book, broker_queue in result_dict.items())
