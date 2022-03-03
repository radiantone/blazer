import logging
from contextlib import contextmanager
from ..mpi.primitives import comm, rank, size, stop, host, comm
from .utils import main
from threading import Thread
from queue import SimpleQueue
from multiprocessing import Condition


def handle_request(gpu_queue, requests, gpu_request):
    logging.info("[%s][%s] Got gpu request: %s", host, rank, gpu_request)

    if type(gpu_request) is dict:
        destination = gpu_request['rank']
    else:
        parts = gpu_request.split(':')
        destination = parts[2]

    if destination:
        logging.info("Master sending GPU allocation to rank[%s] queue size %s",int(destination), gpu_queue.qsize())
        try:
            gpu = gpu_queue.get(block=False)
            if gpu:
                logging.info("Master Got GPU from QUEUE %s", gpu)
                comm.send(gpu, dest=int(destination), tag=1)
        except:
            logging.info("Master storing GPU request for rank[%s] queue size %s",int(destination), gpu_queue.qsize())
            requests.put(gpu_request)


class gpu: 
    
    using_gpu = None

    gpus = main()
    free_gpus = [gpus]
    gpu_queue = SimpleQueue()
    requests = SimpleQueue()
    lock = Condition()
    total_released = 0

    def __init__(self, *args, **kwargs): 
        logging.info("[%s][%s] GPU Context init %s",host,rank,kwargs)
        self.kwargs = kwargs

        for gpu in self.gpus:
            self.gpu_queue.put(gpu)

        
    def __enter__(self, *args, **kwargs): 
        logging.info("[%s][%s] GPU Context enter",host,rank)
        while True:
            if rank == 0:
                logging.info("[%s][%s] Master waiting on gpu request from rank", host, rank)
                logging.info("Master requests %s", self.requests.qsize())
                logging.info("total_released %s size %s",self.total_released, size-1)
                
                if self.total_released == size-1:
                    logging.info("MASTER IS BREAKING")
                    break

                gpu_request = comm.recv(tag=1)
                logging.info("[%s][%s] Master got request from rank %s", host, rank, gpu_request)
                destination = None
                
                if type(gpu_request) is dict and 'release' in gpu_request:
                    del gpu_request['release']
                    try:
                        self.lock.acquire()
                        self.total_released += 1
                    finally:
                        self.lock.release()

                    logging.info("[%s][%s] Release GPU %s", host, rank,gpu_request)
                    try:
                        request = self.requests.get(block=False)
                        logging.info("Got REQUEST %s",request)
                    except:
                        request = None

                    self.gpu_queue.put(gpu_request)
                    if request:
                        handle_request(self.gpu_queue,  self.requests, request)
                else:
                    if gpu_request == "break":
                        logging.info("MASTER IS BREAKING")
                        break

                    handle_request(self.gpu_queue, self.requests, gpu_request)
                
            else:
                logging.info("[%s][%s] Sending gpu request",host,rank)
                comm.send(f"gpu:{host}:{rank}", dest=0, tag=1)
                logging.info("[%s][%s] Waiting for gpu",host,rank)
                self.using_gpu = gpu = comm.recv(source=0, tag=1)
                logging.info("[%s][%s] Allocating GPU[%s]",host,rank, gpu)
                return gpu


    def __exit__(self, exc_type, exc_value, exc_traceback): 
        # notify master of releasing this gpu
        logging.info("[%s][%s] GPU Context exit",host,rank)
        if rank != 0:
            self.using_gpu['release'] = True
            self.using_gpu['rank'] = rank
            comm.send(self.using_gpu, dest=0, tag=1)
            logging.info("[%s][%s] GPU Context exit: released GPU %s",host,rank, self.using_gpu)
        #else:
        #    comm.send("break", dest=0, tag=1)
