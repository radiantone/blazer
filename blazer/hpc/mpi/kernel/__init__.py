from queue import SimpleQueue
from typing import Any, Dict, List

from blazer.hpc.mpi import mpi
from blazer.logs import logging


class MPIKernelModule:
    name = "module"


class MPIKernelMessageHandler:
    pass


class MPIKernelListener:
    pass


class MPIKernelPlugin:
    def start(self):
        pass

    def stop(self):
        pass


class Primitive(MPIKernelPlugin):
    pass


class Parallel(Primitive):
    def start(self):
        logging.info("Parallel plugin start")

    def stop(self):
        logging.info("Parallel plugin stop")


class MPIKernel:

    _modules: List[MPIKernelModule] = []
    _plugins: List[MPIKernelPlugin] = []
    _handlers: List[MPIKernelMessageHandler] = []
    _queues: Dict[str, SimpleQueue] = {}

    class begin:
        def __init__(self, *args, **kwargs):
            logging.info("Kernel.BEGIN INIT")

        def __enter__(self, *args, **kwargs):
            logging.info("Kernel.BEGIN ENTER")

        def __exit__(self, exc_type, exc_value, exc_traceback):
            from blazer.hpc.mpi.primitives import stop

            logging.info("Kernel.BEGIN EXIT")
            stop()

    def __init__(self, plugins: List[MPIKernelPlugin]):
        self._plugins += plugins
        logging.info("Kernel plugins %s", self._plugins)

    def setup(self):
        logging.info("Kernel setup")

    def boot(self):
        logging.info("Kernel booting...")
        self.setup()
        self.startup()
        self.run()

    def add_handler(self, handler: MPIKernelMessageHandler):
        self._handlers += [handler]

    def add_module(self, module: MPIKernelModule):
        """Set up a queue to receive messages and send them to the module"""
        self._queues[module.name + ".out"] = SimpleQueue()
        self._queues[module.name + ".in"] = SimpleQueue()
        self._modules.append(module)

    def get_modules(self) -> List[MPIKernelModule]:
        return self._modules

    def add_listener(self, listener: MPIKernelListener):
        pass

    def kernel_message(self, message: Any):
        pass

    def broadcast_message(self, message: Any):
        """Send a message to any registered handlers. A module may receive a message on its
        MPI tag and then broadcast it to the kernel for other modules to receive via the kernel."""
        pass

    def run(self):
        """Main kernel loop.Will initiate module run methods that
        may set up threads to listen on various MPI tags to process messages.

        Also run thread to monitor queues for each module to forward messages to."""
        logging.info("Kernel running")

    def startup(self):
        logging.info("Kernel startup")

        [plugin.start() for plugin in self._plugins]

    def shutdown(self):

        logging.info("Kernel shutdown")
        [plugin.stop() for plugin in self._plugins]
        mpi.Disconnect()


plugins: List[MPIKernelPlugin] = [Parallel()]
kernel = MPIKernel(plugins)
kernel.boot()
