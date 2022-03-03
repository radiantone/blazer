import logging
from contextlib import contextmanager
from ..mpi.primitives import comm, rank, stop, host, comm


if rank == 0:
    comm.send("gpu:0", dest=1, tag=1)

@contextmanager
def gpu(*args, **kwds):
    try:
        logging.debug("[%s] Waiting on gpu from master", host)
        while True:
            lock = comm.recv(source=0, tag=1)
            if lock.find("gpu") == 0:
                gpu = lock.split(":")[1]
                logging.debug("[%s] Allocating GPU[%s]",host, gpu)
                yield gpu
    finally:
        logging.debug("[%s] Returning from gpu context",host)
