import logging
from typing import List, Any, Callable
from threading import Thread
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from functools import partial

def parallel(defers: List, *args):
    """ This will use the master node 0 scheduler to scatter/gather results """

    #threads = [executor.submit(partial(defer, *args)) for defer in defers]
    #return [thread.result() for thread in threads]

    threads = [Thread(target=defer,args=args) for defer in defers]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]    

def scatter(data: Any, func: Callable):
    pass

def pipeline(defers : List):
    """ This will use the master node 0 scheduler to orchestrate results """
    last_result = None
    
    for defer in defers:
        if last_result is not None:
            last_result = defer(last_result)
        else:
            last_result = defer()
            
    return last_result