""" MPI implementation of compute primitives 
Will use special scheduler running on rank 0 to orchestrate
the needed behavior """
from typing import List, Any, Callable
from mpi4py import MPI
from functools import partial
from threading import Thread
from numpy import iterable
from pydash import flatten

from contextlib import contextmanager
from contextlib import nullcontext as skip

import dill
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
host= MPI.Get_processor_name()

global loop
loop = True

MASTER = rank == 0

logging.debug(f"MY {host} RANK {rank} and size {size}")


@contextmanager
def begin(*args, **kwds):
    try:
        logging.debug("Yielding comm")
        yield comm
    finally:
        logging.debug("Invoking stop(%s)",rank)
        if rank == 0:
            logging.debug("kwds %s", kwds)
            if 'stop' in kwds and kwds['stop']:
                stop()
            elif 'stop' not in kwds:
                stop()


def mprint(*args):
    if rank == 0:
        print(*args)


def stop():
    """ Stop all workers """
    global loop

    logging.debug("Stopping %s, %s", rank,host)
    if rank == 0:
        logging.debug("Sending break to all ranks")
        for i in range(1, size):
           comm.send("break", dest=i)
        logging.debug("Waiting on barrier")
        comm.Barrier()
        logging.debug("Barrier complete")


if rank != 0:
    def run():
        while loop:
            logging.debug("thread rank %s waiting on defer", rank)
            defer = comm.recv(source=0)
            logging.debug("thread rank %s got data %s", rank, defer)
            if type(defer) is str and defer == "break":
                logging.debug("Rank %s stopping", rank)
                break
            defer = dill.loads(defer)
            logging.debug("thread rank %s got defer", defer)
            result = defer()
            logging.debug("thread rank %s sending result %s", rank, result)
            comm.send(result, dest=0)
        logging.debug(f"Rank {rank} notifying Barrier")
        comm.Barrier()


    thread = Thread(target=run)
    thread.start()


def parallel(defers: List, *args):
    """ This will use the master node 0 scheduler to scatter/gather results """
    _rank = 1
    logging.debug("parallel rank %s %s", rank, args)
    l = len(defers)

    if rank == 0:
        dest_rank = 1

        if len(args) > 0:
            last_result = args
        else:
            last_result = None

        # Send all functions to other ranks to execute in parallel
        for defer in defers:
            if last_result is None:
                logging.debug("parallel Sending defer %s to rank %s NO ARGS", defer, dest_rank)
                comm.send(dill.dumps(defer), dest=dest_rank)
            else:
                logging.debug("parallel Sending defer %s to rank %s %s", defer, dest_rank, last_result)
                fname = None
                if type(defer) is partial:
                    fname = defer.func.__name__
                elif hasattr(defer, '__name__'):
                    fname = defer.__name__
                else:
                    # We have object data and need to pass it along
                    _defer = defer

                    def get_value(*args):
                        return _defer

                    defer = get_value
                    fname = 'get_value'

                logging.debug("FNAME %s", fname)
                if fname in ['parallel', 'pipeline']:
                    logging.debug("PARALLEL EXECUTING %s %s", defer, last_result)
                    # Just execute the parallel since I am already at rank 0
                    last_result = defer(*last_result)
                    l = l - 1
                    logging.debug("PARALLEL %s EXECUTING DONE %s", fname, last_result)
                    continue
                else:
                    comm.send(dill.dumps(partial(defer, *last_result)), dest=dest_rank)

            # Cycle destination rank over available nodes
            dest_rank += 1
            if dest_rank >= size:
                dest_rank = 1

        # After all functions are sent and being executed, get results over MPI comm
        results = []
        for i in range(0, l):
            logging.debug("parallel Master Waiting on result")
            last_result = comm.recv()
            logging.debug("parallel Master Got result %s", last_result)
            results += [last_result]

        logging.debug("parallel return results %s", results)
        return results


def enumrate(gen):
    i = 0
    for a in gen:
        yield i, a
        i += 1


def mapreduce(_map: Callable, _reduce: Callable, data: Any, require_list=False):
    """ Use scatter for map/reduce in one call """
    import numpy as np

    results = []
    if rank == 0:
        _chunks = np.array_split(data, size)
    else:
        _chunks = None
    data = comm.scatter(_chunks, root=0)

    listdata = data.tolist()
    logging.info("MAP: %s",listdata)
    _data = _map(listdata)
    newData = comm.gather(_data, root=0)
    results += [newData]
    _flattened = flatten(results)
    if None not in _flattened:
        logging.info("REDUCE: %s",_flattened)
        _data = _reduce(_flattened)
        if require_list and type(_data) is list:
            mapreduce(_map, _reduce, _data)
        return _data

def map(func: Callable, data: Any):
    _funcs = []
    for arg in data:
        if iterable(arg):
            _funcs += [partial(func, *arg)]
        else:
            _funcs += [partial(func, arg)]

    if MASTER:
        return parallel(_funcs)
    else:
        return None


def reduce(func: Callable, data: Any):
    _funcs = []
    if data is None:
        return None
    for arg in data:
        if iterable(arg):
            _funcs += [partial(func, *arg)]
        else:
            _funcs += [partial(func, arg)]

    logging.debug("FUNCS %s", _funcs)
    if MASTER:
        return pipeline(_funcs)
    else:
        return None


def scatter(data: Any, func: Callable):
    """ This will create a generator to chunk the incoming data (which itself can be a generator)
    Each chunk (which can itself be a list of data) will then be scattered with the function to all
    ranks. """
    
    def chunker(generator, chunksize):
        chunk = []
        for i, c in enumrate(generator):
            chunk += [c]
            if len(chunk) == chunksize:
                yield chunk
                chunk = []
        if len(chunk) > 0:
            yield chunk

    
    chunked_data = chunker(data, size)
    results = []
    for i, chunk in enumrate(chunked_data):
        if len(chunk) < size:
            chunk += [None for i in range(len(chunk), size)]

        data = comm.scatter(chunk, root=0)
        _data = func(data)
        logging.debug("scatter[%s, %s]: Chunk %s %s, Func is %s Data is %s Result is %s", rank,host,i, chunk, func, data, _data)
        newData = comm.gather(_data, root=0)
        results += [newData]

    return flatten(results)

def pipeline(defers: List, *args):
    """ This will use the master node 0 scheduler to orchestrate results """
    logging.debug("pipeline rank %s %s", rank, args)
    if rank == 0:
        dest_rank = 1

        if len(args) > 0:
            last_result = args
        else:
            last_result = None

        for defer in defers:
            if last_result is None:
                logging.debug("Pipeline Sending defer to rank %s NO ARGS", dest_rank)
                comm.send(dill.dumps(defer), dest=dest_rank)
            else:
                logging.debug("Pipeline Sending defer %s to rank %s %s", defer, dest_rank, last_result)
                fname = None
                if type(defer) is partial:
                    fname = defer.func.__name__
                elif hasattr(defer, '__name__'):
                    fname = defer.__name__
                else:
                    _defer = defer

                    def get_value(*args):
                        return _defer

                    defer = get_value
                    fname = 'get_value'

                if fname in ['parallel', 'pipeline']:
                    logging.debug("PIPELINE EXECUTING %s %s", defer, last_result)
                    # Just execute the parallel since I am already at rank 0
                    last_result = defer(last_result)
                    continue
                else:
                    comm.send(dill.dumps(partial(defer, last_result)), dest=dest_rank)

            dest_rank += 1

            if dest_rank >= size:
                dest_rank = 1

            logging.debug("Pipeline Master Waiting on result")
            last_result = comm.recv()
            logging.debug("Pipeline Master Got result %s", last_result)

        return last_result
