import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG
)
from .hpc.mpi.primitives import stop, begin, skip, MASTER as ROOT, mprint as print

__all__ = (
    'stop',
    'begin',
    'skip',
    'ROOT',
    'print'
)