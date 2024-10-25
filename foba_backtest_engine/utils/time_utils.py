import datetime

import arrow

# import pandas_market_calendars as mcal

NANOS_IN_SECOND = int(1e9)
MILLIS_IN_SECOND = int(1e3)

# hkex = mcal.get_calendar('HKEX')
# szse = mcal.get_calendar('SSE')

# def business_day_range(start_date, end_date, type = 'datetime'):
#     if type == 'datetime':
#         return [c.to_pydatetime().date() for c in hkex.valid_days(start_date=start_date, end_date=end_date)]
#     elif type == 'timestamp':
#         return [c.timestamp() for c in hkex.valid_days(start_date=start_date, end_date=end_date)]
#     else:
#         return hkex.valid_days(start_date=start_date, end_date=end_date)

# def get_previous_business_day(date=datetime.date.today(), type = 'datetime'):
#     if type == 'datetime':
#         return hkex.previous_close(date).to_pydatetime().date()
#     elif type == 'timestamp':
#         return hkex.previous_close(date).timestamp()
#     else:
#         return hkex.previous_close(date)

# def get_previous_business_day_szse(date=datetime.date.today(), type = 'datetime'):
#     if type == 'datetime':
#         return szse.previous_close(date).to_pydatetime().date()
#     elif type == 'timestamp':
#         return szse.previous_close(date).timestamp()
#     else:
#         return szse.previous_close(date)


# def get_previous_china_and_hk_business_day(date):
#     test_dates = sorted(business_day_range(start_date=date-Timedelta(days = 20), end_date=date))
#     condition = False
#     found_date = None
#     while not condition:
#         test_date = test_dates[-1]
#         prev_hk = get_previous_business_day(test_date)
#         prev_szse = get_previous_business_day_szse(test_date)
#         if len(test_dates) == 0:
#             raise ValueError('No previous business day found')
#         if prev_hk == prev_szse:
#             condition = True
#             found_date = prev_hk
#         else:
#             test_dates.pop()
#     return found_date


def start_end_time(
    start_hour,
    start_minute,
    end_hour,
    end_minute,
    days_ago=0,
    time_zone=None,
    skip_weekend=False,
    end_date=None,
):
    if end_date:
        now = arrow.get(end_date, time_zone).replace(second=0, microsecond=0)
    else:
        now = (
            arrow.now(time_zone).replace(second=0, microsecond=0).shift(days=-days_ago)
        )
    start_time = now.replace(hour=start_hour, minute=start_minute)
    end_time = now.replace(hour=end_hour, minute=end_minute)
    if end_time < start_time:
        start_time = start_time.replace(days=-1)

    if skip_weekend:
        days_past_friday = start_time.weekday() - 4
        if days_past_friday > 0:
            start_time = start_time.replace(days=-days_past_friday)
            end_time = end_time.replace(days=-days_past_friday)

    return start_time, end_time


def to_nano_timestamp(arrow_time):
    try:
        return arrow_time.timestamp * NANOS_IN_SECOND
    except TypeError:
        return arrow_time.int_timestamp * NANOS_IN_SECOND


def to_milli_timestamp(arrow_time):
    try:
        return arrow_time.timestamp * MILLIS_IN_SECOND
    except TypeError:
        return arrow_time.int_timestamp * MILLIS_IN_SECOND


def _convert_unix_to_datetime(unix_time, time_zone=None):
    return datetime.datetime.fromtimestamp(unix_time / 1e9, tz=time_zone).replace(
        tzinfo=None
    )


def _convert_unix_to_ftime(unix_time, time_zone=None):
    return (
        _convert_unix_to_datetime(unix_time, time_zone=time_zone).strftime("%H:%M:%S")
        + "."
        + str(unix_time)[-9:]
    )


def _convert_unix_to_fdate(unix_time, time_zone=None):
    return _convert_unix_to_datetime(unix_time, time_zone=time_zone).strftime(
        "%Y-%m-%d"
    )


def _time_from_start_of_day_in_seconds(unix_time, time_zone=None):
    return (
        datetime.datetime.fromtimestamp(unix_time / 1e9, tz=time_zone)
        - datetime.datetime.fromtimestamp(unix_time / 1e9, tz=time_zone).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    ).total_seconds()
