import logging

from .hpc.gpu import gpu
from .hpc.mpi.primitives import MASTER as ROOT
from .hpc.mpi.primitives import begin
from .hpc.mpi.primitives import mprint as print
from .hpc.mpi.primitives import stop, variable

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)

__all__ = ("stop", "gpu", "begin", "variable", "ROOT", "print")
