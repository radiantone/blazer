import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)
from .hpc.mpi.primitives import stop, begin, begin2, skip, MASTER as ROOT, mprint as print
from .hpc.gpu import gpu

__all__ = (
    'stop',
    'gpu',
    'begin',
    'begin2',
    'skip',
    'ROOT',
    'print'
)