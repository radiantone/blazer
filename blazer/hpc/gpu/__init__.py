import logging
import os
from contextlib import contextmanager
from ..mpi.primitives import comm, rank, size, stop, host, comm
from .utils import main
from threading import Thread
from queue import SimpleQueue
from multiprocessing import Condition
from numba import cuda
from typing import Any

ranks_exit_request = SimpleQueue()

def handle_request(host_queues, requests, gpu_request):

    if type(gpu_request) is dict:
        destination = gpu_request['rank']
        host = gpu_request['host']
    else:
        parts = gpu_request.split(':')
        if len(parts) != 3:
            logging.warn("Invalid message %s", gpu_request)
            return

        host = parts[1]
        destination = parts[2]

    logging.debug("[%s][%s] Got gpu request: %s", host, rank, gpu_request)
    logging.debug("[%s][%s] Got gpu request[host_queues]: %s", host, rank,host_queues)
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
    has_cuda = False

    def __init__(self, *args, **kwargs): 
        logging.debug("[%s][%s] GPU Context init %s",host,rank,kwargs)

        self.kwargs = kwargs
        # Master node reads list of gpus for each node in fabric
        # Then it places those GPU definitions on individual queues for each
        # host

        def load_gpus():
            from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter, where, select, filter, rank, size

            def get_gpus():
                from blazer.hpc.mpi import rank
                import platform
                logging.debug("GETTING GPU for rank[%s]",rank)
                try:
                    gpus = main()
                    for gpu in gpus:
                        gpu['host'] = platform.node()
                        gpu['rank'] = rank
                    return gpus
                except:
                    return []

            gpu_calls = [p(get_gpus) for i in range(1,size)]
            gpus = parallel(gpu_calls)
            if not gpus:
                gpus = []
            gpulist = [gpu for gpu in gpus if len(gpu) > 0]

            logging.debug("GOT GPUs for rank[%s] %s",rank, gpulist)
            return gpulist
            

        self.GPUS = []

        try:
            from pydash import flatten
            if rank == 0:
                self.GPUS = load_gpus()
                self.GPUS = flatten(self.GPUS)
                self.gpuranks = len(self.GPUS)
                for gpu in self.GPUS:
                    if gpu['host'] not in self.host_queues:
                        self.host_queues[gpu['host']] = SimpleQueue()
                    self.host_queues[gpu['host']].put(gpu)

                #comm.Barrier()
            
            else:
                import platform
                #print("WAITING FOR MASTER TO GATHER GPU DATA")
                #comm.Barrier()
                logging.info("[%s][%s] GPU context init. Checking my GPUS", host,rank)
                try:
                    gpus = main()
                    for gpu in gpus:
                        gpu['host'] = platform.node()
                        gpu['rank'] = rank
                    self.GPUS = flatten(gpus)
                except Exception as ex:
                    logging.error(ex)
                    self.GPUS = []

                logging.info("GOT GPUs for [%s][%s] %s",host,rank, self.GPUS)
            
        except:
            import traceback
            print(traceback.format_exc())
            logging.warn("No GPUS found")
        finally:
            return None

    def __enter__(self, *args, **kwargs) -> Any: 
        logging.debug("[%s][%s] GPU Context enter",host,rank)

        while True:

            if rank == 0:
                logging.debug("[%s][%s] Master waiting on gpu request from rank", host, rank)
                logging.debug("Master requests %s", self.requests.qsize())
                logging.debug("size is %s and ranks_exit_request.qsize()+1 is %s", size, ranks_exit_request.qsize()+1)

                logging.debug("total_released %s size %s",self.total_released, size-1)
                logging.debug("gpuranks is %s and ranks_exit_request.qsize()+1 is %s", self.gpuranks, ranks_exit_request.qsize()+1)
                # If the # of rank exist requests + 1 (master) equals the total number of ranks
                # Then master node can exit. All ranks have reported in
                if self.gpuranks == ranks_exit_request.qsize()+1:
                    logging.debug("MASTER FINISHED: total_released = %s",self.total_released)
                    break
                    
                # Wait for GPU requests on tag 1. Block until we get a message
                logging.debug("MASTER recv")
                gpu_request = comm.recv(tag=1)
                logging.debug("MASTER got gpu_request")

                logging.debug("[%s][%s] Master got request from rank %s", host, rank, gpu_request)
                        
                if type(gpu_request) is dict and 'release' in gpu_request:
                    del gpu_request['release']

                    try:
                        logging.debug("[%s][%s] Master acquiring lock", host, rank)
                        self.lock.acquire()
                        self.total_released += 1
                        ranks_exit_request.put("exit")
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
                if len(self.GPUS) == 0:
                    logging.debug("[%s][%s] I don't have any GPUS so just exiting",host,rank)
                    break

                logging.debug("[%s][%s] Sending gpu request",host,rank)

                # Request a GPU from master node. 
                comm.send(f"gpu:{host}:{rank}", dest=0, tag=1)
                
                # Block until we get one: NOTE: What if server finishes and this receive is never fulfilled?
                logging.debug("[%s][%s] RECV Waiting for gpu",host,rank)
                self.using_gpu = gpu = comm.recv(source=0, tag=1)

                logging.debug("[%s][%s] RECV Received GPU[%s]",host,rank, gpu)
                cuda.select_device(gpu['id'])
                self.has_cuda = True
                # Resume context in app code
                return gpu

        logging.debug("[%s][%s] Exiting GPU context: ",host,rank)

    def __exit__(self, exc_type, exc_value, exc_traceback) -> Any:
        # notify master of releasing this gpu

        logging.debug("[%s][%s] GPU Context exit",host,rank)

        if self.has_cuda:
            cuda.close()

            # Worker context is exiting, therefore, send our GPU definition back to the head
            # node and exit the context
            if rank != 0:
                self.using_gpu['release'] = True
                self.using_gpu['rank'] = rank
                logging.debug("[%s][%s] GPU Context exit: sending gpu back to master GPU %s",host,rank, self.using_gpu)
                comm.send(self.using_gpu, dest=0, tag=1)
                logging.debug("[%s][%s] GPU Context exit: released GPU %s",host,rank, self.using_gpu)
                #ranks_exit_request.put("exit")
            else:
                # Master node exiting GPU context. Receive any stuck messages?
                logging.debug("[%s][%s] MASTER GPU Context exit",host,rank)
        else:
            comm.send("pass", dest=0, tag=1)