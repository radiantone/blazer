from .hpc.mpi.primitives import stop, begin, skip, MASTER as ROOT, mprint as print

__all__ = (
    'stop',
    'begin',
    'skip',
    'ROOT',
    'print'
)