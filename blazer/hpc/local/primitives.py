import logging
from threading import Thread
from typing import Any, Callable, List


def parallel(defers: List, *args):
    """This will use the master node 0 scheduler to scatter/gather results"""

    # threads = [executor.submit(partial(defer, *args)) for defer in defers]
    # return [thread.result() for thread in threads]

    threads = [Thread(target=defer, args=args) for defer in defers]
    logging.info("Starting parallel threads")
    _ = [thread.start() for thread in threads]  # type: ignore[func-returns-value]
    logging.info("Waiting on parallel threads")
    _ = [thread.join() for thread in threads]  # type: ignore[func-returns-value]


def scatter(data: Any, func: Callable):
    pass


def pipeline(defers: List):
    """This will use the master node 0 scheduler to orchestrate results"""
    last_result = None

    for defer in defers:
        if last_result is not None:
            last_result = defer(last_result)
        else:
            last_result = defer()

    return last_result
