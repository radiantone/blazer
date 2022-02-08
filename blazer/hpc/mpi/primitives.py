""" MPI implementation of compute primitives 
Will use special scheduler running on rank 0 to orchestrate
the needed behavior """
from typing import List, Any, Callable
from mpi4py import MPI
from functools import partial
from threading import Thread

import dill
import logging
logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


from contextlib import contextmanager

@contextmanager
def begin(*args, **kwds):
    try:
        yield comm
    finally:
        stop()


def mprint(*args):
    if rank == 0:
        print(*args)

def stop():
    for i in range(1,size):
        comm.send("break", dest=i)

if rank != 0:
    def run():
        while True:
            logging.debug("thread rank %s waiting on defer",rank)
            defer = comm.recv(source=0)
            logging.debug("thread got data")
            if type(defer) is str and defer == "break":
                logging.debug("Rank %s stopping", rank)
                break
            defer = dill.loads(defer)
            logging.debug("thread rank %s got defer",defer)
            result = defer()
            logging.debug("thread rank %s sending result %s",rank,result)
            comm.send(result, dest=0)

    thread = Thread(target=run)
    thread.start()
    

def parallel(defers: List, *args):
    """ This will use the master node 0 scheduler to scatter/gather results """
    _rank = 1
    logging.debug("parallel rank %s %s",rank, args)
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
                logging.debug("parallel Sending defer %s to rank %s NO ARGS",defer, dest_rank)
                comm.send(dill.dumps(defer), dest=dest_rank)
            else:
                logging.debug("parallel Sending defer %s to rank %s %s",defer, dest_rank, last_result)
                if type(defer) is partial:
                    fname = defer.func.__name__
                else:
                    fname = defer.__name__
                
                logging.debug("FNAME %s", fname)
                if fname in ['parallel','pipeline']:
                    logging.debug("PARALLEL EXECUTING %s %s",defer,last_result)
                    # Just execute the parallel since I am already at rank 0
                    last_result = defer(*last_result)
                    l = l - 1
                    logging.debug("PARALLEL %s EXECUTING DONE %s",fname,last_result)
                    continue
                else:
                    comm.send(dill.dumps(partial(defer, *last_result)), dest=dest_rank)

            # Cycle destination rank over available nodes
            dest_rank += 1
            if dest_rank >= size:
                dest_rank = 1

        # After all functions are sent and being executed, get results over MPI comm
        results = []
        for i in range(0,l):
            logging.debug("parallel Master Waiting on result")
            last_result = comm.recv()
            logging.debug("parallel Master Got result %s",last_result)
            results += [last_result]

        logging.debug("parallel return results %s",results)
        return results

def scatter(data: Any, func: Callable):
    pass

def pipeline(defers : List, *args):
    """ This will use the master node 0 scheduler to orchestrate results """
    logging.debug("pipeline rank %s %s",rank, args)
    if rank == 0:
        dest_rank = 1
        
        if len(args) > 0:
            last_result = args
        else:
            last_result = None

        for defer in defers:
            if last_result is None:
                logging.debug("Pipeline Sending defer to rank %s NO ARGS",dest_rank)
                comm.send(dill.dumps(defer), dest=dest_rank)
            else:
                logging.debug("Pipeline Sending defer %s to rank %s %s",defer,dest_rank,last_result)
                if type(defer) is partial:
                    fname = defer.func.__name__
                else:
                    fname = defer.__name__
                
                logging.debug("FNAME %s", fname)
                if fname in ['parallel','pipeline']:
                    logging.debug("PIPELINE EXECUTING %s %s",defer,last_result)
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
            logging.debug("Pipeline Master Got result %s",last_result)

        return last_result
