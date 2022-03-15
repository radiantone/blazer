import logging
from typing import Any, Callable


class JobClass:
    def __init__(self, thenclass, func: Callable):
        self.thenclass = thenclass
        self.func = func
        self.job = self.func()

    def login(self):
        self.job.login()
        return self

    def __call__(self, data, *args, **kwargs):

        try:
            logging.debug("JobClass acquiring lock")
            self.thenclass.lock.acquire()
            logging.debug("Calling func %s %s %s", self.func, args, kwargs)
            self.thenclass.result = self.job(data)
            return self.thenclass
        finally:
            logging.debug("JobClass releasing lock")
            self.thenclass.lock.release()


class ThenClass:
    result: Any = None

    def __init__(self, func):
        from multiprocessing import Condition

        self.func = func
        self.lock = Condition()

    def __call__(self, *args, **kwargs):
        from functools import partial

        try:
            logging.debug("Then acquiring lock")
            self.lock.acquire()
            logging.info("%s %s %s", self.func, args, kwargs)
            p = partial(self.func, *args, **kwargs)
            jobclass = JobClass(self, p)

            return jobclass
        finally:
            self.lock.release()
            logging.debug("then: released lock")

    def then(self, next: Callable):
        try:
            logging.info("then: waiting on lock")
            self.lock.acquire()
            result = self.result
            if isinstance(self.result, ThenClass):
                result = self.result.result
            logging.info("then: Invoking next %s with result %s", next, result)
            self.result = next(result)
            return self
        finally:
            self.lock.release()
            logging.info("then: released lock")


def Then(func, *args, **kwargs):
    tc = ThenClass(func)
    return tc
