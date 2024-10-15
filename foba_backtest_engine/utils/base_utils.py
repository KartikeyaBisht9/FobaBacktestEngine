from collections.abc import Mapping, Iterator
from collections import defaultdict
from heapq import heappush, heappop
from functools import total_ordering
import logging

class ReprMixin:
    """Mixin to define __repr__ so objects are printed nicer (e.g. for debugging).
    """

    def __repr__(self):
        # TODO does __dict__ work with __slots__?
        return _repr(self, self.__dict__)


def _repr(self, attrs):
    items = ('{attr}={value!r}'.format(attr=attr, value=getattr(self, attr)) for attr in attrs)
    return '{cls}({items})'.format(cls=self.__class__.__name__, items=', '.join(items))


class Record(ReprMixin):
    def __init__(self, **kwargs):
        """Create a new record object with fields and values as given in kwargs.
        """
        self.__dict__.update(kwargs)

    def _asdict(self):
        """Return the object as a dict.

        This functions the same as the namedtuple _asdict method.
        """
        return self.__dict__.copy()

    def _replace(self, **kwargs):
        """Return a new record with new or updated fields as given in kwargs.

        This is similar to the namedtuple _replace method, except new fields can be added.
        """
        data = self._asdict()
        data.update(kwargs)
        return type(self)(**data)

    def _set_missing(self, **kwargs):
        """Return a new record with missing fields filled as given in kwargs.

        A field counts as missing if it is absent or is None.
        """
        data = self._asdict()
        for key, value in kwargs.items():
            if data.get(key) is None:
                data[key] = value
        return type(self)(**data)

    def __eq__(self, other):
        return self._asdict() == other._asdict()


class ImmutableDict(Mapping):
    """
    This behaves the same as dict except instances are immutable.
    """

    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)

    @classmethod
    def from_multi_dict(cls, d):
        return cls((key, tuple(values)) for key, values in d.items())

    def __getitem__(self, item):
        return self._dict[item]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)


def get_logger(module_name, level=logging.DEBUG):
    """
    A generic function to get a logger.

    Example:
        logger = get_logger(__name__)
        ...
        logger.warn('Disk is nearly full')

    :param module_name:
    :return:
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class ImmutableRecord(Record):
    """Immutable record type.

    This has a similar interface to namedtuple except:

    - can't construct with positional arguments
    - can't index
    - can add new fields in _replace
    - instances can always pickle (dynamically created namedtuple types can't pickle)
    """

    def __init__(self, **kwargs):
        for field in kwargs:
            if field[0] == '_':
                raise ValueError('Field names cannot start with an underscore: {!r}'.format(field))

        super().__init__(**kwargs)
        self.__dict__['_fields'] = tuple(self.__dict__)

    def _asdict(self):
        data = super()._asdict()
        del data['_fields']
        return data

    def __setattr__(self, key, value):
        raise TypeError('Attribute assignment is not supported')


def multi_dict(values, key):
    """Create a dict that groups values together which have the same key.

    The relative ordering of the values within each key is preserved.

    :param values: iterable of values
    :param key: key function
    :return: dict mapping each key to a list of all values that have that key

    >>> multi_dict([1, 2, 1], lambda x: x) == {1: [1, 1], 2: [2]}
    True
    """
    d = defaultdict(list)
    for value in values:
        d[key(value)].append(value)
    return d

def sorted_multi_dict(values, *, group_key, sort_key):
    """Create a dict that groups values together which have the same group_key, and sorts each group by the sort_key.

    :param values: iterable of values
    :param group_key: group key function
    :param sort_key: sort key function
    :return: dict mapping each group key to a sorted list of all values that have that key

    # >>> sorted_multi_dict([1, 2, 3, 4], group_key=lambda x: x % 2, sort_key=lambda x: -x) == {0: [4, 2], 1: [3, 1]}
    True
    """
    d = multi_dict(values, group_key)
    for group_values in d.values():
        group_values.sort(key=sort_key)
    return d

def invert_dict_to_dict_list(dict_to_invert):
    """
    Inverts a dictionary with common values to a new dictionary where the values are the keys,
    and the original keys are in a list for the new dictionary values.
    input_dict = {A: 1, B: 1, C: 2, D: 2}
    output_dict = {1: [A, B] , 2: [C, D]}
    """
    inverted_dict = {}
    for k, v in dict_to_invert.items():
        inverted_dict[v] = inverted_dict.get(v, [])
        inverted_dict[v].append(k)
    return inverted_dict

def common_key_items(*dicts):
    """Iterate through common keys and their values from a number of dicts.

    >>> sorted(common_key_items({'a': 1, 'b': 2, 'c': 3}, {'b': 20, 'c': 30, 'd': 40}))
    [('b', 2, 20), ('c', 3, 30)]

    Yield tuples. The first element is a common key.
    Following elements are the values from each dict, in the order the dicts were passed.

    Do not yield keys that are not common to all dicts.
    """
    common_keys = set(dicts[0]).intersection(*dicts[1:])
    for key in common_keys:
        yield (key,) + tuple(d[key] for d in dicts)

def sorted_iterators(*sequences):
    for sequence, dispatch, key in sequences:
        sequence.sort(key=key)
        yield iter(sequence), dispatch, key

def nan_if_none(value):
    if value is None:
        return float('nan')
    return value


class StreamQueue:
    def __init__(self):
        self.streams = []

    def push(self, stream):
        if stream.update():
            heappush(self.streams, stream)

    def pop(self):
        return heappop(self.streams)

    def __bool__(self):
        return bool(self.streams)


@total_ordering
class Stream:
    def __init__(self, iterator, map_function, key, rank):
        self.iterator = iterator
        self.map_function = map_function
        self.key = key
        self.rank = rank
        self.value = None
        self.priority = None

    def mapped(self):
        return self.map_function(self.value)

    def update(self):
        try:
            self.value = next(self.iterator)
            self.priority = self.key(self.value)
        except StopIteration:
            self.value = None
            self.priority = None
            return False
        return True

    def __eq__(self, other):
        return (self.priority, self.rank) == (other.priority, other.rank)

    def __lt__(self, other):
        return (self.priority, self.rank) < (other.priority, other.rank)
    

def process_by_priority(*iterators):
    """
    Combines multiple lists/streams & PROCESSES them in priority order
        - we look at all lists and pick the next item with smallest/highest priority
        - we process this item & then repeat.
    """
    queue = StreamQueue()
    for rank, (iterator, map_function, key) in enumerate(iterators):
        stream = Stream(iterator, map_function, key, rank)
        queue.push(stream)
    while queue:
        stream = queue.pop()
        yield stream.mapped()
        queue.push(stream)


def priority_process_and_dispatch(*iterators):
    """
    Dispatch the object with the smallest priority from multiple iterators (lists) to a handler function.

    Each iterator should be provided as a tuple in the form:
        (iterator, process_function, priority_key)

    - The `iterator` yields objects in ascending order of priority.
    - The `process_function` is called on the object that has the smallest priority among all iterators.
    - If multiple iterators have objects with the same priority, they are processed in the order they were passed to `priority_process_and_dispatch`.
    - The `priority_key` function takes an object from the iterator and returns its priority value.

    We do this efficiently using heaps

    """
    for _ in process_by_priority(*iterators):
        pass