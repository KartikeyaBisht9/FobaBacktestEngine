from foba_backtest_engine.utils.base_utils import ImmutableRecord, ImmutableDict
from foba_backtest_engine.enrichment import enriches, provides
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from operator import attrgetter

"""
TIME CLASSES

HFT precise classes for calculating order SEND tims and order RECEIVE times

- one-way delay     = line_latency + jitter                                 ... i.e. time for a message to go from EX --> Optiver or Optiver --> EX      
- round-trip delay  = 2 x (line_latency) + matching_engine_delay + jitter   ... i.e. time for message to be send to exchange and event to be recieved in public/private feed


a) MinSentTime = Uses the minimum possible delays to calculate the earliest time the order could have been sent.
b) MaxSendTime = Uses the maximum possible delays to calculate the latest time the order could have been sent.
c) AverageSendTime ... you get the idea
"""

class SentTime(metaclass=ABCMeta):
    def __init__(self, time_profile, profile_name=None, times=('level', 'join', 'event')):
        self.time_profile = time_profile

        self.name = self._kind()
        if profile_name:
            self.name = profile_name + '_' + self.name

        self.time_names = times

    @abstractmethod
    def calculate(self, exchange_timestamp, driver_received):
        pass

    @classmethod
    @abstractmethod
    def _kind(cls):
        pass


class MinSentTime(SentTime):
    def calculate(self, exchange_timestamp, driver_received):
        return min_order_sent_time(exchange_timestamp, driver_received, self.time_profile)

    @classmethod
    def _kind(cls):
        return 'min'


class MaxSentTime(SentTime):
    def calculate(self, exchange_timestamp, driver_received):
        return max_order_sent_time(exchange_timestamp, driver_received, self.time_profile)

    @classmethod
    def _kind(cls):
        return 'max'


class AverageSentTime(SentTime):
    def calculate(self, exchange_timestamp, driver_received):
        return 0.5 * (
                max_order_sent_time(exchange_timestamp, driver_received, self.time_profile) +
                min_order_sent_time(exchange_timestamp, driver_received, self.time_profile)
        )

    @classmethod
    def _kind(cls):
        return 'avg'

TimeProfile = namedtuple('TimeProfile', ('min_one_way_delay', 'min_round_trip_delay', 'max_one_way_delay', 'max_round_trip_delay'))

# TODO ... need to check these numbers for optiver. However the round trip delays were ~ 360 micros and max time was 800 micros

omdc_profile = TimeProfile(min_one_way_delay=-float('inf'),
                                          min_round_trip_delay=float(0.35e6),
                                          max_one_way_delay=float('inf'),
                                          max_round_trip_delay=float(0.8e6))



def max_order_sent_time(exchange_published, driver_received, time_profile):
    return min(exchange_published - time_profile.min_one_way_delay, driver_received - time_profile.min_round_trip_delay)


def min_order_sent_time(exchange_published, driver_received, time_profile):
    return max(exchange_published - time_profile.max_one_way_delay, driver_received - time_profile.max_round_trip_delay)


class SentTimeCalculator:
    def __init__(self, sent_time, time_name):
        self.sent_field = '_'.join((sent_time.name, time_name, 'sent_time'))
        self._exchange_timestamp = attrgetter(time_name + '_exchange_timestamp')
        self._driver_received = attrgetter(time_name + '_driver_received')
        self._calculate = sent_time.calculate

    def calculate(self, event):
        return self._calculate(self._exchange_timestamp(event), self._driver_received(event))
    
@provides('send_times')
@enriches('foba_events')
def send_times(foba_events, sent_times):
    calculators = [SentTimeCalculator(sent_time, time_name)
                   for sent_time in sent_times
                   for time_name in sent_time.time_names]

    def sent_times(event):
        kwargs = {calculator.sent_field: calculator.calculate(event) for calculator in calculators}
        return ImmutableRecord(**kwargs)

    return ImmutableDict((event_id, sent_times(event)) for event_id, event in foba_events.items())