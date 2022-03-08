import logging
import os
from contextlib import contextmanager
from ..mpi.primitives import comm, rank, size, stop, host, comm
from .utils import main
from threading import Thread
from queue import SimpleQueue
from multiprocessing import Condition
from numba import cuda


def handle_request(host_queues, requests, gpu_request):

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


class gpu: 
    
    using_gpu = None

    gpu_queue = SimpleQueue()
    requests = SimpleQueue()

    host_queues : dict = {}
    lock = Condition()
    total_released = 0

    def __init__(self, *args, **kwargs): 
        logging.debug("[%s][%s] GPU Context init %s",host,rank,kwargs)

        self.kwargs = kwargs
        GPUS = []

        # Master node reads list of gpus for each node in fabric
        # Then it places those GPU definitions on individual queues for each
        # host
        def load_gpus():
            #try:
            #if rank == 0:
            logging.debug(f"Reading GPUs from /home/darren/git/blazer/blazer-{host}-gpulist.txt")
            if os.path.exists(f'/home/darren/git/blazer/blazer-{host}-gpulist.txt'):
                logging.debug("Loading GPU file")
                with open(f'/home/darren/git/blazer/blazer-{host}-gpulist.txt') as gpufile:
                    gpu_lines = gpufile.readlines()
                    gpus = [None] * len(gpu_lines)
                    for line in gpu_lines:
                        line = line.strip()
                        _gpu = {}
                        parts = line.split(' ')
                        _gpu['host'] = parts[0]
                        if _gpu['host'] not in self.host_queues:
                            self.host_queues[_gpu['host']] = SimpleQueue()
                        _gpu['uuid'] = parts[-1].replace(')','').strip()
                        _gpu['id'] = int(parts[2].replace(':',''))
                        _gpu['name'] = parts[3]
                        self.host_queues[_gpu['host']].put(_gpu)
                        gpus[_gpu['id']] = _gpu
                    logging.debug("Returning from GPU file")
                    return gpus
            #except Exception as ex:
            #    logging.error(ex)
            logging.warn("Returning empty list for GPUS")
            return []

        self.GPUS = []

        try:
            GPUS = load_gpus()
            print("GPUS",GPUS)
            gpus = main()
            _gpus = {}
            for i, gpu in enumerate(gpus):                
                print(i,gpu)
                gpu.update(GPUS[i])
                if gpu['host'] not in _gpus:
                    _gpus[gpu['host']] = []
                _gpus[gpu['host']] += [gpu]

            self.GPUS = _gpus
        except:
            import traceback
            print(traceback.format_exc())
            logging.warn("No GPUS found")


    def __enter__(self, *args, **kwargs): 
        logging.debug("[%s][%s] GPU Context enter",host,rank)

        # TODO: Rework this
        while True:

            if rank == 0:
                logging.debug("[%s][%s] Master waiting on gpu request from rank", host, rank)
                logging.debug("Master requests %s", self.requests.qsize())
                logging.debug("total_released %s size %s",self.total_released, size-1)
                
                if self.total_released == size-1:
                    logging.debug("MASTER FINISHED: total_released = %s",self.total_released)
                    break
                    
                # Wait for GPU requests on tag 1. Block until we get a message
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

                    # Put this GPU back on the queue for that host
                    self.host_queues[gpu_request['host']].put(gpu_request)

                    # Are there any pending GPU requests on the requests queue?
                    if request:
                        handle_request(self.host_queues, self.requests, request)
                else:
                    if gpu_request == "break":
                        logging.debug("[%s][%s] MASTER IS BREAKING",host,rank)
                        break

                    # Handle the message
                    handle_request(self.host_queues, self.requests, gpu_request)
            else:
                logging.debug("[%s][%s] Sending gpu request",host,rank)

                # Request a GPU from master node. 
                comm.send(f"gpu:{host}:{rank}", dest=0, tag=1)
                
                # Block until we get one: NOTE: What if server finishes and this receive is never fulfilled?
                logging.debug("[%s][%s] Waiting for gpu",host,rank)
                self.using_gpu = gpu = comm.recv(source=0, tag=1)

                logging.debug("[%s][%s] Received GPU[%s]",host,rank, gpu)
                cuda.select_device(gpu['id'])

                # Resume context in app code
                return gpu

        logging.debug("[%s][%s] Exiting GPU context: ",host,rank)

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        # notify master of releasing this gpu
        logging.debug("[%s][%s] GPU Context exit",host,rank)
        cuda.close()

        # Worker context is exiting, therefore, send our GPU definition back to the head
        # node and exit the context
        if rank != 0:
            self.using_gpu['release'] = True
            self.using_gpu['rank'] = rank
            logging.debug("[%s][%s] GPU Context exit: sending gpu back to master GPU %s",host,rank, self.using_gpu)
            comm.send(self.using_gpu, dest=0, tag=1)
            logging.debug("[%s][%s] GPU Context exit: released GPU %s",host,rank, self.using_gpu)
            
        else:
            # Master node exiting GPU context. Receive any stuck messages?
            logging.debug("[%s][%s] MASTER GPU Context exit",host,rank)
