import logging

from .hpc.mpi.primitives import stop, begin, MASTER as ROOT, mprint as print
from .hpc.gpu import gpu

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)

__all__ = (
    'stop',
    'gpu',
    'begin',
    'skip',
    'ROOT',
    'print'
)
