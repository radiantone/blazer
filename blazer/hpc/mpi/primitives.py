""" MPI implementation of compute primitives 
Will use special scheduler running on rank 0 to orchestrate
the needed behavior """
import logging
from typing import Generator, List, Any, Callable
from mpi4py import MPI
from functools import partial
from threading import Thread
from numpy import iterable
from pydash import flatten

from contextlib import contextmanager
from contextlib import nullcontext as skip

import dill

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
host = MPI.Get_processor_name()

MASTER = rank == 0

logging.debug(f"ME: {host} RANK {rank} and procs {size}")

class begin: 
    
    def __init__(self, *args, **kwargs): 
        logging.debug("[%s][%s] Context init %s",host,rank,kwargs)
        self.kwargs = kwargs
            
    def __enter__(self, *args, **kwargs): 
        logging.debug("[%s][%s] Context enter",host,rank)
        try:
            yield comm
        finally:
            pass
            

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        logging.debug("[%s][%s] Context exiting %s",host,rank,self.kwargs)

        if rank == 0:
            logging.debug("[%s][%s] Master STOPPING",host,rank)
            stop()

def mprint(*args):
    """ Print output if on master node """
    if rank == 0:
        print(*args)


def stop(barrier=True):
    """ Stop all workers from master """

    logging.debug("Stopping %s, %s", rank,host)
    if rank == 0:
        logging.debug("Sending break to all ranks")
        # This will cause each worker rank to hit the barrier
        for i in range(1, size):
            logging.debug(f"Master sending break to rank {i}")
            comm.send("break", tag=0, dest=i)

        if barrier:
            logging.debug("Master Waiting on barrier")
            comm.Barrier() # Master should barrier until all the workers being stopped barrier too
            logging.debug("Barrier complete")

        logging.debug("Master STOP complete!")

if rank != 0:
    """ Monitor thread for (worker processes) receiving function tasks to execute """
    def run():
        while True:
            logging.debug("[%s] thread rank %s waiting on defer",host,  rank)
            logging.debug("[%s][%s] RECV BEFORE TAG=0",host,rank)

            # Listen for tasks and "break" commands from Master
            defer = comm.recv(source=0, tag=0)

            logging.debug("[%s][%s] RECV AFTER TAG=0",host,rank)
            logging.debug("[%s] thread rank %s got defer",host,  rank)
            logging.debug("[%s] thread rank %s got data %s", host, rank, defer)

            if type(defer) is str and defer == "break":
                logging.debug("[%s] thread rank %s breaking", host, rank)
                break

            defer = dill.loads(defer)
            logging.debug("[%s] thread rank %s got defer",host,  defer)
            result = defer()
            logging.debug("[%s] thread rank %s sending result %s",host,  rank, result)
            comm.send(result, tag=0, dest=0)

        logging.debug(f"{host} Rank {rank} notifying Barrier")
        comm.Barrier()

        logging.debug("[%s] thread rank [%s] message loop ending", host, rank)

    thread = Thread(target=run)
    thread.start()


def parallel(defers: List, *args):
    """ Run list of tasks in parallel across compute fabric """
    logging.debug("[%s] parallel rank %s %s",host, rank, args)
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
                logging.debug("[%s] parallel Sending defer %s to rank %s NO ARGS",host,  defer, dest_rank)
                comm.send(dill.dumps(defer), tag=0, dest=dest_rank)
            else:
                logging.debug("[%s] parallel Sending defer %s to rank %s %s",host,  defer, dest_rank, last_result)
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
                    logging.debug("[%s] PARALLEL EXECUTING %s %s",host,  defer, last_result)
                    # Just execute the parallel since I am already at rank 0
                    last_result = defer(*last_result)
                    l = l - 1
                    logging.debug("[%s] PARALLEL %s EXECUTING DONE %s",host,  fname, last_result)
                    continue
                else:
                    comm.send(dill.dumps(partial(defer, *last_result)), tag=0, dest=dest_rank)

            # Cycle destination rank over available nodes
            dest_rank += 1
            if dest_rank >= size:
                dest_rank = 1

        # After all functions are sent and being executed, get results over MPI comm
        results = []
        for i in range(0, l):
            logging.debug("[%s] parallel Master Waiting on result", host)
            logging.debug("[%s][%s] RECV BEFORE TAG=0",host,rank)
            last_result = comm.recv(tag=0)
            logging.debug("[%s][%s] RECV AFTER TAG=0",host,rank)
            logging.debug("[%s] parallel Master Got result %s", host, last_result)
            results += [last_result]

        logging.debug("[%s] parallel return results %s", host, results)
        return results


def enumrate(gen):
    """ Alternate enumerate as a generator """
    i = 0
    for a in gen:
        yield i, a
        i += 1


def stream(data: Generator, func: Callable):
    """ Iterate over generator until you have collected enough data to farm out to ranks 
    then run them in parallel and get the results, yield generator"""
    chunk = []
    for datum in data:
        chunk += [partial(func, datum)]
        if len(chunk) == size:
            yield parallel(chunk)
            chunk = []


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
    logging.debug("MAP: %s",listdata)
    _data = _map(listdata)
    newData = comm.gather(_data, root=0)
    results += [newData]
    _flattened = flatten(results)
    if None not in _flattened:
        logging.debug("[%s] REDUCE: %s",host, _flattened)
        _data = _reduce(_flattened)
        if require_list and type(_data) is list:
            mapreduce(_map, _reduce, _data)
        return _data

def map(func: Callable, data: Any):
    """ Apply map function over data elements """
    """ Runs in parallel over data """
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
    """ Apply reduce function over data elements """
    """ Results cascade to next reduce function """
    """ Runs in sequence over data """
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

        # Pad data for rank size
        extra_chunks = 0
        if len(chunk) < size:
            chunk_list = [None for i in range(len(chunk), size)]
            extra_chunks = len(chunk_list)
            chunk += chunk_list

        data = comm.scatter(chunk, root=0)
        _data = func(data)
        logging.debug("[%s] scatter[%s, %s]: Chunk %s %s, Func is %s Data is %s Result is %s", host, rank,host,i, chunk, func, data, _data)
        newData = comm.gather(_data, root=0)

        # Unpad data
        if newData and extra_chunks > 0:
            newData = newData[:-extra_chunks]

        results += [newData]

    return flatten(results)

def pipeline(defers: List, *args):
    """ Run list of functions in ordered sequence, passing intermediate results on to next task """
    logging.debug("pipeline rank %s %s", rank, args)
    if rank == 0:
        dest_rank = 1

        if len(args) > 0:
            last_result = args
        else:
            last_result = None

        for defer in defers:
            if last_result is None:
                logging.debug("[%s] Pipeline Sending defer to rank %s NO ARGS", host, dest_rank)
                comm.send(dill.dumps(defer), tag=0, dest=dest_rank)
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
                    comm.send(dill.dumps(partial(defer, last_result)), tag=0, dest=dest_rank)

            dest_rank += 1

            if dest_rank >= size:
                dest_rank = 1

            logging.debug("Pipeline Master Waiting on result")
            last_result = comm.recv(tag=0)
            logging.debug("Pipeline Master Got result %s", last_result)

        return last_result
