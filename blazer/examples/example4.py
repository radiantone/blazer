import logging
import blazer
import numpy as np

from blazer.hpc.mpi.primitives import host, rank
from numba import vectorize
from timeit import default_timer as timer

def dovectors():

    @vectorize(['float32(float32, float32)'], target='cuda')
    def dopow(a, b):
        return a ** b

    vec_size = 100

    a = b = np.array(np.random.sample(vec_size), dtype=np.float32)
    c = np.zeros(vec_size, dtype=np.float32)

    start = timer()
    dopow(a, b)
    duration = timer() - start
    return duration

with blazer.begin(gpu=True):  # on-fabric MPI scheduler
    # Behind the scenes, the blazer on-fabric scheduler will
    # ensure the gpu context below blocks until a gpu is free.
    # It will allow gpu contexts to run as others release the gpu
    logging.info(f"[{host}][{rank}] Waiting on GPU context")
    with blazer.gpu() as gpu:  # on-metal GPU scheduler
        logging.info(f"[{host}][{rank}] Got GPU context")
        if gpu:
            logging.info(f"[{host}][{rank}] Got GPU: {gpu}")
            #print(dovectors())


