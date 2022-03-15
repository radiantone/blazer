import blazer
from blazer.hpc.mpi import parallel
from blazer.hpc.mpi import partial as p

from .funcs import calc_stuff


def test_parallel():
    with blazer.begin():
        tasks = [
            p(calc_stuff, 1),
            p(calc_stuff, 2),
            p(calc_stuff, 3),
            p(calc_stuff, 4),
            p(calc_stuff, 5),
        ]
        result = parallel(tasks)

        if blazer.ROOT:
            assert len(result) == len(tasks)
