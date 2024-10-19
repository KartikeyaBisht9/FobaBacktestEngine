"""
----------
Processors
----------

A processor is a callable object (e.g. a function) that has been decorated with @provides.
When called with appropriate arguments, it will produce an output that will be available to other processors under the
name(s) given in the @provides decoration.

Conceptually, there are two types of processors:

loader
    A loader simply provides a collection of objects for other processors to use.
    It may depend on resources provided by other processors.

enricher
    An enricher provides a special type of resource: it enriches each object from the output of a loader.
    So it must depend on at least that loader.

(a)     Loaders
        ~~~~~~~

        To create a loader:

        * Define a function with parameter names the same as resources provided by other processors or configuration.
        * Decorate the function with @provides('name of loaded resource')
        (Multiple names can be given; the same resource will be available under different names.)
        * Return a map (e.g. an ImmutableDict) with the loaded objects as values.
        The loaded objects should be namedtuples (or, a similar type with the _fields attribute, e.g. ImmutableRecord).
        The keys of the map are arbitrary ids for the values.
        The id_dict function can construct these keys easily.

(b)     Enrichers
        ~~~~~~~~~

        To create an enricher:

        * Define a function and decorate it with @provides, as with a loader.
        * Decorate the function with @enriches('name of enriched resource')
        * Return a map (e.g. an ImmutableDict) with the enrichments as values.
        The enrichments should be namedtuples (or, a similar type with the _fields attribute, e.g. ImmutableRecord).
        The keys of the map should be the ids of the resource named in @enriches.

Configuration
-------------

A processor may have inputs that aren't provided by any other processor.
The inputs can be set by using configuration:

* set it in the configuration dict that is passed to the Enrichment constructor; or,
* set it using the "configure" function: configure(processor, parameter_name=parameter_value)

----------
Enrichment
----------

To run an enrichment, you need to provide the Enrichment constructor with an iterable of processors, and a map of configuration.

The order of the processors is important:
each processor can only depend on resources provided by other processors earlier in the list.

Once the Enrichment is constructed, you can join all the enrichments for a particular loaded resource
by calling joined_enrichments

"""



from collections import defaultdict, namedtuple
from functools import partial
from inspect import signature, Parameter
from warnings import warn
import time
import datetime
import pickle
import os
import pandas as pd
from collections import deque
from foba_backtest_engine.utils.base_utils import ImmutableDict, get_logger, ImmutableRecord
import copy


logger = get_logger(__name__)


class Enrichment:
    def __init__(self, processors, configuration, auto_persist_on_fail=False, test_mode=False, test_processor=None,
                 test_name=None, persist_dropped=False, auto_cleanup_resources=False, keep_resources_on_disk=False,
                 on_disk_location='/data/tmp/foba/', processor_resource_look_forward=3):
        """Run the enrichment with the given processors and configuration.
        """
        self.resources = dict()
        self.configuration = configuration.copy()
        self.enrichments = defaultdict(list)
        self.enriches = {}
        self.auto_persist = auto_persist_on_fail
        self.persist_dropped = persist_dropped
        self.keep_resources_on_disk = keep_resources_on_disk
        self.on_disk_location = on_disk_location
        self.successful_processors = []
        self.pending_processors = deque(processors)

        self.expired_resources = {}
        self.processor_resource_look_forward = processor_resource_look_forward
        self.auto_cleanup_resources = auto_cleanup_resources
        self.required_memory_resources = {}

        self.check_processors_requirements(processors, configuration)
        self.accumulate_in_memory_processors(processors, configuration)

        self.run_processors(processors, test_mode, test_name, test_processor)

    def run_processors(self, processors, test_mode, test_name, test_processor):
        if test_mode:
            self.run_test_mode(test_name, test_processor, processors)
        else:
            profile_dict = dict()
            for processor in processors:
                start = time.time()
                self._process(processor)
                end = time.time()
                profile_dict[processor.__name__] = {'TimeTaken': end - start}
            print(pd.DataFrame(profile_dict).T.sort_values('TimeTaken', ascending=False))

    def run_test_mode(self, test_name, test_processor, processors):
        if test_processor and test_name:
            resources_from_file = self._load_persited_resources(test_name)
            self.resources.update(resources_from_file)
            pre_test_processors = processors[:processors.index(test_processor)]
            post_test_processors = processors[processors.index(test_processor):]
            if not resources_from_file:
                for processor in pre_test_processors:
                    self._process(processor)
                self._persist_required_resources(test_name)
            else:
                for processor in pre_test_processors:
                    provides = processor._provides
                    enriches = getattr(processor, '_enriches', None)
                    for provide in provides:
                        if enriches:
                            output = self.resources[provide]
                            self._record_enrichment(provides, enriches, output)

            for processor in post_test_processors:
                self._process(processor)
        else:
            raise ValueError('Cannot run in test_mode without test_processor')

    def check_processors_requirements(self, processors, configuration):
        provided = self.accumulate_and_check_provides(processors, configuration)
        required = self.accumulate_required_enriches(processors)
        config = set(configuration.keys())

        for process, provide, require in zip(processors, provided, required):
            expired_provides = provide.difference(require)
            if hasattr(process, '__name__'):
                self.expired_resources[process.__name__] = expired_provides - config
                self.expired_resources[process.__name__] -= set(['foba_events'])

    def accumulate_in_memory_processors(self, processors, configuration):
        current_enrichers = {}
        for i, process in enumerate(reversed(processors)):
            requires = [parameter.name for parameter in process._requires.values()]
            for resource in requires:
                if resource not in configuration:
                    current_enrichers[resource] = i

            to_delete = []
            for resource, index in current_enrichers.items():
                if index < i - self.processor_resource_look_forward:
                    to_delete.append(resource)

            for resource in to_delete:
                del current_enrichers[resource]
            self.required_memory_resources[process.__name__] = set(current_enrichers.keys())

    def accumulate_required_enriches(self, processors):
        required_enriches = set()
        still_required = []
        for processor in reversed(processors):
            if not isinstance(processor, DeleteResources):
                required_enriches.update(set(parameter.name for parameter in processor._requires.values()))
                if hasattr(processor, '_enriches'):
                    required_enriches.add(processor._enriches)
            still_required.append(copy.deepcopy(required_enriches))
        still_required.reverse()
        return still_required

    def accumulate_and_check_provides(self, processors, configuration):
        provided = set(configuration.keys())
        provided_since_start = []
        for processor in processors:
            if isinstance(processor, DeleteResources):
                for resource in processor.resource_names:
                    provided.remove(resource)
            else:
                # required_args = set(parameter.name for parameter in processor._requires.values()
                #                     if parameter.default is Parameter.empty
                #                     and parameter.name is not 'kwargs')
                required_args = set(parameter.name for parameter in processor._requires.values()
                                    if parameter.default is Parameter.empty
                                    and parameter.name != 'kwargs')
                provided.update(processor._provides)
                if not provided.issuperset(required_args):
                    raise ValueError('Processor {} does not have required resource {}. Check whether providing process ' \
                                     'exists or if resource has been deleted'.format(processor.__name__,
                                                                                     required_args - provided))
            provided_since_start.append(copy.deepcopy(provided))
        return provided_since_start

    def auto_remove_processes_from_memory(self, processor):
        not_required = set(self.resources.keys()) - self.required_memory_resources[processor.__name__]
        not_required -= set(self.configuration.keys())
        for name in not_required:
            logger.debug('Deleting resource from memory: {name}'.format(name=name))
            del self.resources[name]

    def _process(self, processor):
        if self.auto_cleanup_resources:
            if hasattr(processor, '__name__'):
                self._delete(self.expired_resources[processor.__name__])
                if self.keep_resources_on_disk:
                    self.auto_remove_processes_from_memory(processor)
        else:
            if hasattr(processor, '__name__') and self.expired_resources[processor.__name__]:
                logger.debug('Could auto-cleanup the following resources {}. Consider enabling as kwarg in enricher'
                             .format(self.expired_resources[processor.__name__]))
            if isinstance(processor, DeleteResources):
                self._delete(processor.resource_names)
                return

        provides = processor._provides
        self._check_duplicate_providers(provides)

        kwargs = dict(self._processor_kwargs(processor))

        enriches = getattr(processor, '_enriches', None)
        if enriches:
            _check_enricher_parameters(processor, kwargs)

        processor_name = processor.__name__
        logger.debug('Running processor: ' + processor_name)
        try:
            output = processor(**kwargs)
        except:
            if self.auto_persist:
                self._persist_on_failure(processor)
            raise
        logger.debug('{processor} produced {num_results} results'
                     .format(processor=processor_name, num_results=len(output)))

        for name in provides:
            if self.keep_resources_on_disk:
                self.save_resource_to_disk(output, name)
            self.resources[name] = output

        if enriches:
            self._record_enrichment(provides, enriches, output)

        self.successful_processors.append(processor)
        self.pending_processors.popleft()

    def _delete(self, resource_names):
        for name in resource_names:
            if name in self.resources:
                logger.debug('Deleting resource from memory: {name}'.format(name=name))
                del self.resources[name]
            if self.keep_resources_on_disk:
                if os.path.exists(self.on_disk_location + name):
                    logger.debug('Deleting resource from disk: {name}'.format(name=name))
                    os.remove(self.on_disk_location + name)

    def _record_enrichment(self, provides, enriches, output):
        for name in provides:
            self.enriches[name] = enriches

        while True:
            self.enrichments[enriches].append(output)
            if enriches not in self.enriches:
                break
            enriches = self.enriches[enriches]

    def _processor_kwargs(self, processor):
        for parameter in processor._requires.values():
            if parameter.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
                continue
            try:
                if parameter.name in self.configuration:
                    resource = self.configuration[parameter.name]
                elif self.keep_resources_on_disk and not (parameter.name in self.resources):
                    self.resources[parameter.name] = self.load_resource(parameter.name)
                    resource = self.resources[parameter.name]
                else:
                    resource = self.resources[parameter.name]
            except KeyError:
                if parameter.default is Parameter.empty:
                    print('raised again')
                    raise
                resource = parameter.default
            yield parameter.name, resource

    def _check_duplicate_providers(self, provides):
        for name in provides:
            if name in self.resources:
                raise Exception(('Same resource {name!r} provided by multiple processors;' +
                                 ' second processor provides {provides}').format(name=name, provides=provides))

    def joined_enrichments(self, name):
        """Yield namedtuples, containing the loaded data with the given resource and all of its enrichments.
        """
        all_event_dicts = self._joined_dicts(name)
        first_event_dict = next(all_event_dicts)

        yield ImmutableRecord(**first_event_dict)
        for event_dict in all_event_dicts:
            yield ImmutableRecord(**event_dict)

    def _joined_dicts(self, name):
        if self.keep_resources_on_disk:
            resources = self.load_resource(name)
        else:
            resources = self.resources[name]
        enrichments = self.enrichments[name]
        common_ids = set(resources).intersection(*enrichments)
        non_common_ids = set(resources).difference(common_ids)

        dropped_events = []
        for id in non_common_ids:
            event = resources[id]._asdict()
            for enrichment in enrichments:
                if id in enrichment:
                    event.update(enrichment[id]._asdict())
            dropped_events.append(event)

        if self.persist_dropped:
            self.persist_dropped_events(dropped_events)

        num_missing_events = len(resources) - len(common_ids)
        if num_missing_events != 0:
            warn('dropping {num_missing_events} events because they are missing enrichments'
                 .format(num_missing_events=num_missing_events))

        for event_id in common_ids:
            event_dict = resources[event_id]._asdict()
            for enrichment in enrichments:
                event_dict.update(enrichment[event_id]._asdict())
            yield event_dict

    def _persist_on_failure(self, processor):
        name = 'failed_run_{}_{}'.format(processor.__name__, datetime.date.today())
        self._persist_required_resources(name)

    def _persist_required_resources(self, test_name):
        persist_resources ={}
        for key, value in self.resources.items():
            try:
                with open('tmp', 'wb') as file:
                    pickle.dump(value, file)
                persist_resources[key] = value
            except AttributeError:
                continue
        with open(Enrichment._persist_path(test_name), 'wb') as file:
            pickle.dump(persist_resources, file)

    @staticmethod
    def _load_persited_resources(test_name):
        if os.path.isfile(Enrichment._persist_path(test_name)):
            logger.debug('Retrieving saved provides from file')
            with open(Enrichment._persist_path(test_name), 'rb') as file:
                provides = pickle.load(file)
            return provides
        else:
            logger.debug('No file {}. Running all processors'.format(test_name))
            return {}

    @staticmethod
    def _persist_path(test_name):
        return '{}_persisted_provides.pckl'.format(test_name)

    def persist_dropped_events(self, events):
        with open('dropped_events_{}'.format(datetime.date.today()), 'wb') as file:
            pickle.dump({'events':events, 'enrichments':self.enrichments}, file)

    def load_resource(self, resource_name):
        if os.path.exists(self.on_disk_location + resource_name):
            logger.debug('Loading {} from file'.format(resource_name))
            with open(self.on_disk_location +resource_name, 'rb') as file:
                resource = pickle.load(file)
        else:
            raise KeyError
        return resource

    def save_resource_to_disk(self, resource, resource_name):
        if not os.path.isdir(self.on_disk_location):
            os.makedirs(self.on_disk_location, exist_ok=True)
        logger.debug('Saving {} to file'.format(resource_name))
        with open(self.on_disk_location + resource_name, 'wb') as file:
            pickle.dump(resource,file)

def _check_enricher_parameters(processor, kwargs):
    if processor._enriches not in kwargs:
        raise Exception('Processor {processor} claims to enrich {enriches} but that is not a parameter'
                        .format(processor=processor, enriches=processor._enriches))


def _camel_case(snake_case):
    return ''.join(_camel_case_characters(snake_case))


def _camel_case_characters(snake_case):
    snake_case = iter(snake_case)
    yield next(snake_case).upper()
    for character in snake_case:
        if character == '_':
            yield next(snake_case).upper()
        else:
            yield character


def provides(*names):
    """Mark a callable as providing a resource under the given name(s).

    The callable provides just one resource, but it can be known under many names.

    The function is modified in place.
    """
    def wrapper(function):
        function._provides = names
        function._requires = dict(signature(function).parameters)
        return function
    return wrapper


def enriches(name):
    """Mark a callable as enriching a resource with the given name.
    """
    def wrapper(function):
        function._enriches = name
        return function
    return wrapper


def configure(processor, **kwargs):
    """Configure the processor with the given keyword arguments.
    """
    configured_function = partial(processor, **kwargs)
    configured_function.__name__ = processor.__name__
    configured_function._provides = processor._provides
    configured_function._requires = {key: value for key, value in processor._requires.items() if key not in kwargs}

    if hasattr(processor, '_enriches'):
        configured_function._enriches = processor._enriches

    return configured_function


DeleteResources = namedtuple('DeleteResources', 'resource_names')


def delete_resources(*names):
    return DeleteResources(names)


def id_dict(iterable):
    """Create a unique id for each element in the iterable, and return an ImmutableDict mapping each id to the element.
    """
    return ImmutableDict((id(x), x) for x in iterable)