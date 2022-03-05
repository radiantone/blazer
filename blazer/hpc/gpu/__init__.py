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

try:
    if os.path.exists(f'/home/darren/git/blazer/blazer-{host}-gpulist.txt'):
        with open(f'/home/darren/git/blazer/blazer-{host}-gpulist.txt') as gpufile:
            gpu_lines = gpufile.readlines()
            gpus = [None] * len(gpu_lines)
            for line in gpu_lines:
                _gpu = {}
                parts = line.split(' ')
                _gpu['host'] = parts[0]
                _gpu['uuid'] = parts[-1].replace(')','').strip()
                _gpu['id'] = int(parts[2].replace(':',''))
                _gpu['name'] = parts[3]

                gpus[_gpu['id']] = _gpu
            GPUS = gpus
except:
    pass

if rank == 0:
    """ Monitor thread for Master, controlling user code context handlers """
    def run():
        while True:
            logging.debug("[%s][%s] RECV BEFORE: Master waiting on context or break",host,rank)
            context = comm.recv(tag=2)
            logging.debug("[%s][%s] RECV AFTER: Master got context message %s",host,rank,context)

            if context.find("context") == 0:
                parts = context.split(":")
                logging.debug("[%s] Master ending context for %s",rank, parts[2])
                if int(parts[2]) == 0:
                    stop()
                    break
                else:
                    comm.send("context:end", dest=int(parts[2]))
                    #stop()

            if context == "break:barrier":
                logging.debug("BREAKING:BARRIER: Master waiting on barrier")
                comm.Barrier()
                logging.debug("BREAKING:BARRIER: stop(barrier=False)")
                stop()
                logging.debug("BREAKING:BARRIER: Master post barrier breaking")
                break

            if context == "break":
                logging.debug("Master breaking")
                #stop(barrier=False)
                break
            
        logging.debug("Master monitor loop ended")

        # TODO: False works with non-gpu programs
        # Notify workers to break and exit

    thread = Thread(target=run)
    thread.start()


def handle_request(gpu_queue, host_queues, requests, gpu_request):

    if type(gpu_request) is dict:
        destination = gpu_request['rank']
        host = gpu_request['host']
    else:
        parts = gpu_request.split(':')
        host = parts[1]
        destination = parts[2]

    logging.debug("[%s][%s] Got gpu request: %s", host, rank, gpu_request)
    gpu_queue = host_queues[host]
    
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


def get_gpus():
    gpus = main()
    _gpus = {}
    for i, gpu in enumerate(gpus):
        gpu.update(GPUS[i])
        if gpu['host'] not in _gpus:
            _gpus[gpu['host']] = []
        _gpus[gpu['host']] += [gpu]

    return _gpus

if rank == 0:
    print(get_gpus())


class gpu: 
    
    using_gpu = None

    gpus = main()
    free_gpus = [gpus]
    gpu_queue = SimpleQueue()
    requests = SimpleQueue()

    host_queues : dict = {}
    lock = Condition()
    total_released = 0

    def __init__(self, *args, **kwargs): 
        logging.debug("[%s][%s] GPU Context init %s",host,rank,kwargs)
        self.kwargs = kwargs

        for gpu in get_gpus():
            if gpu['host'] not in self.host_queues:
                self.host_queues[gpu['host']] = SimpleQueue()

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
                    #stop()
                    #break
                    
                logging.debug("[%s][%s] RECV BEFORE tag=1",host,rank)
                gpu_request = comm.recv(tag=1)
                logging.debug("[%s][%s] RECV AFTER tag=1",host,rank)
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
                        handle_request(self.gpu_queue,  self.host_queues, self.requests, request)
                else:
                    if gpu_request == "break":
                        logging.debug("[%s][%s] MASTER IS BREAKING",host,rank)
                        #comm.send("break:barrier", dest=0, tag=2)
                        logging.debug("[%s][%s] MASTER sent break:barrier",host,rank)
                        #comm.send("break", dest=0, tag=1)
                        break

                    handle_request(self.gpu_queue, self.host_queues, self.requests, gpu_request)
            else:
                logging.debug("[%s][%s] Sending gpu request",host,rank)
                comm.send(f"gpu:{host}:{rank}", dest=0, tag=1)
                logging.debug("[%s][%s] Waiting for gpu",host,rank)
                logging.debug("[%s][%s] RECV BEFORE TAG=1,a",host,rank)
                self.using_gpu = gpu = comm.recv(source=0, tag=1)
                logging.debug("[%s][%s] RECV AFTER TAG=1,a",host,rank)
                logging.debug("[%s][%s] Allocating GPU[%s]",host,rank, gpu)
                cuda.select_device(gpu['id'])
                return gpu

        logging.debug("[%s][%s] Exiting GPU context: ",host,rank)

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        # notify master of releasing this gpu
        logging.debug("[%s][%s] GPU Context exit",host,rank)
        cuda.close()

        if rank != 0:
            self.using_gpu['release'] = True
            self.using_gpu['rank'] = rank
            logging.debug("[%s][%s] GPU Context exit: sending gpu back to master GPU %s",host,rank, self.using_gpu)
            comm.send(self.using_gpu, dest=0, tag=1)
            logging.debug("[%s][%s] GPU Context exit: released GPU %s",host,rank, self.using_gpu)
        else:
            logging.debug("[%s][%s] MASTER GPU Context exit",host,rank)
