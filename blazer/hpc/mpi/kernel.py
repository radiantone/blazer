from queue import SimpleQueue
from typing import Any, Dict, List


class MPIKernelModule:
    name = "module"


class MPIKernelMessageHandler:
    pass


class MPIKernelListener:
    pass


class MPIKernelPlugin:
    pass


class MPIKernel:

    _modules: List[MPIKernelModule]
    _plugins: List[MPIKernelPlugin]
    _handlers: List[MPIKernelMessageHandler]
    _queues: Dict[str, SimpleQueue]

    def __init__(self, plugins: List[MPIKernelPlugin]):
        self._plugins += plugins

    def setup(self):
        pass

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

    def _run(self):
        """Main kernel loop.Will initiate module run methods that
        may set up threads to listen on various MPI tags to process messages.

        Also run thread to monitor queues for each module to forward messages to."""
        pass

    def startup(self):
        pass

    def shutdown(self):
        pass
