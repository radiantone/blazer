import logging

from .hpc.gpu import gpu
from .hpc.mpi.primitives import MASTER as ROOT
from .hpc.mpi.primitives import begin, environment
from .hpc.mpi.primitives import mprint as print
from .hpc.mpi.primitives import stop

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)

__all__ = ("stop", "gpu", "begin", "environment", "ROOT", "print")
