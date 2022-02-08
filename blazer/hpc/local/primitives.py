from typing import List, Any, Callable
from threading import Thread
from dask import delayed, compute

def parallel(defers: List, *args):
    """ This will use the master node 0 scheduler to scatter/gather results """
    funcs = [delayed(defer)(args) for defer in defers]
    return compute(*funcs)

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