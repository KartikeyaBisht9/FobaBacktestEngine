from concurrent import futures
import pickle
from functools import wraps

"""
A suite of executors/functions for asynchronous OR synchronous function execution

1) make_executor() - use this to decide if we execute functions in a synchronous | asynchronous manner
2) check_pickles() 
3) check_on_shutdown() - tracks all submitted futures & when executor.shutdown() is called - it checks if any execptions were raised and raises these ... for debugging

"""

def make_executor(parallel, *, safer=True, max_workers=None, executor_cls=futures.ProcessPoolExecutor):
    if not parallel:
        return SynchronousExecutor()

    executor = executor_cls(max_workers)
    if safer:
        executor = check_on_shutdown(check_pickles(executor))
    return executor

def check_pickles(executor):
    original_submit = executor.submit

    @wraps(original_submit)
    def submit(fn, *args, **kwargs):
        pickle.dumps(fn)  # call to ensure fn can be pickled
        pickled_args = pickle.dumps(args)
        pickled_kwargs = pickle.dumps(kwargs)

        return original_submit(UnpicklingFunction(fn), pickled_args, pickled_kwargs)

    executor.submit = submit

    return executor

def check_on_shutdown(executor):
    submitted = []

    original_submit = executor.submit

    @wraps(original_submit)
    def submit(fn, *args, **kwargs):
        future = original_submit(fn, *args, **kwargs)
        submitted.append(future)
        return future

    executor.submit = submit

    original_shutdown = executor.shutdown

    @wraps(original_shutdown)
    def shutdown(wait=True):
        if not wait:
            raise ValueError('only wait=True is supported')

        original_shutdown(wait=True)

        for future in submitted:
            exception = future.exception()
            if exception:
                raise exception

    executor.shutdown = shutdown

    return executor

# export this for convenience
as_completed = futures.as_completed

"""
HELPERS
"""


class UnpicklingFunction:
    def __init__(self, function):
        self._function = function

    def __call__(self, pickled_args, pickled_kwargs):
        args = pickle.loads(pickled_args)
        kwargs = pickle.loads(pickled_kwargs)
        return self._function(*args, **kwargs)


class SynchronousExecutor(futures.Executor):
    """Futures executor that runs submitted functions synchronously.

    Useful for writing code that easily adapts to parallel or serial execution.
    """

    def submit(self, func, *args, **kwargs):
        """Submit a function to be executed synchronously.

        If the function raises an exception then so will the call to submit.
        This contrasts with ProcessPoolExecutor, which only raises the exception when getting the future's result.
        """
        future = futures.Future()
        future.set_result(func(*args, **kwargs))
        return future


class DelayedExecutor(futures.Executor):
    """Futures executor that runs submitted functions when the future result is requested.

    Useful for writing code that easily adapts to parallel or serial execution.
    """

    def submit(self, function, *args, **kwargs):
        """Submit a function to be executed when the future's result is requested.
        """
        return DelayedFuture(function, args, kwargs)


class DelayedFuture:
    def __init__(self, function, args, kwargs):
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def result(self):
        return self.function(*self.args, **self.kwargs)
