import logging
import os
from contextlib import contextmanager
from ..mpi.primitives import comm, rank, size, stop, host, comm
from .utils import main
from threading import Thread
from queue import SimpleQueue
from multiprocessing import Condition
from numba import cuda

GPUS = []

if os.path.exists(f'/var/tmp/blazer-{host}-gpulist.txt'):
    with open(f'/var/tmp/blazer-{host}-gpulist.txt') as gpufile:
        gpu_lines = gpufile.readlines()
        gpus = [None] * len(gpu_lines)
        for line in gpu_lines:
            _gpu = {}
            parts = line.split(' ')
            _gpu['host'] = parts[0]
            _gpu['uuid'] = parts[-1].replace(')','')
            _gpu['id'] = int(parts[2].replace(':',''))
            _gpu['name'] = parts[3]

            gpus[_gpu['id']] = _gpu
        GPUS = gpus

if rank == 0:
    """ Monitor thread for Master, controlling user code context handlers """
    def run():
        while True:
            logging.debug("Master waiting on context or break")
            context = comm.recv(tag=2)
            logging.debug("Master got context message %s",context)

            if context.find("context") == 0:
                parts = context.split(":")
                logging.debug("[%s] Master ending context for %s",rank, parts[2])
                if int(parts[2]) == 0:
                    stop()
                    break
                else:
                    comm.send("context:end", dest=int(parts[2]))
                    #stop()

            if context == "break2":
                logging.debug("Master breaking")
                break
            
        logging.debug("Master monitor loop ended")

    thread = Thread(target=run)
    thread.start()


def handle_request(gpu_queue, requests, gpu_request):
    logging.debug("[%s][%s] Got gpu request: %s", host, rank, gpu_request)

    if type(gpu_request) is dict:
        destination = gpu_request['rank']
    else:
        parts = gpu_request.split(':')
        destination = parts[2]

    if destination:
        logging.debug("Master sending GPU allocation to rank[%s] queue size %s",int(destination), gpu_queue.qsize())
        try:
            gpu = gpu_queue.get(block=False)
            if gpu:
                logging.debug("Master Got GPU from QUEUE %s", gpu)
                comm.send(gpu, dest=int(destination), tag=1)
        except:
            logging.debug("Master storing GPU request for rank[%s] queue size %s",int(destination), gpu_queue.qsize())
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
        logging.debug("[%s][%s] GPU Context init %s",host,rank,kwargs)
        self.kwargs = kwargs

        for gpu in self.gpus:
            self.gpu_queue.put(gpu)

        
    def __enter__(self, *args, **kwargs): 
        logging.debug("[%s][%s] GPU Context enter",host,rank)

        while True:

            if rank == 0:
                logging.debug("[%s][%s] Master waiting on gpu request from rank", host, rank)
                logging.debug("Master requests %s", self.requests.qsize())
                logging.debug("total_released %s size %s",self.total_released, size-1)
                
                if self.total_released == size-1:
                    logging.debug("MASTER IS STOPPING")
                    stop()
                    break

                gpu_request = comm.recv(tag=1)
                logging.debug("[%s][%s] Master got request from rank %s", host, rank, gpu_request)
                
                if type(gpu_request) is dict and 'release' in gpu_request:
                    del gpu_request['release']
                    try:
                        logging.debug("[%s][%s] Master acquiring lock", host, rank)
                        self.lock.acquire()
                        self.total_released += 1
                    finally:
                        logging.debug("[%s][%s] Master releasing lock", host, rank)
                        self.lock.release()

                    logging.debug("[%s][%s] Release GPU %s", host, rank,gpu_request)
                    try:
                        request = self.requests.get(block=False)
                        logging.debug("Got REQUEST %s",request)
                    except:
                        request = None

                    self.gpu_queue.put(gpu_request)
                    if request:
                        handle_request(self.gpu_queue,  self.requests, request)
                else:
                    #if gpu_request == "break":
                    #    logging.debug("MASTER IS BREAKING")
                    #    break

                    handle_request(self.gpu_queue, self.requests, gpu_request)
            else:
                logging.debug("[%s][%s] Sending gpu request",host,rank)
                comm.send(f"gpu:{host}:{rank}", dest=0, tag=1)
                logging.debug("[%s][%s] Waiting for gpu",host,rank)
                self.using_gpu = gpu = comm.recv(source=0, tag=1)
                logging.debug("[%s][%s] Allocating GPU[%s]",host,rank, gpu)
                cuda.select_device(gpu['id'])
                return gpu

        logging.debug("[%s][%s] Exiting GPU context",host,rank)

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        # notify master of releasing this gpu
        logging.debug("[%s][%s] GPU Context exit",host,rank)
        cuda.close()

        if rank != 0:
            self.using_gpu['release'] = True
            self.using_gpu['rank'] = rank
            comm.send(self.using_gpu, dest=0, tag=1)
            logging.debug("[%s][%s] GPU Context exit: released GPU %s",host,rank, self.using_gpu)
