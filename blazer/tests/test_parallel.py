import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p
from .funcs import calc_more_stuff, calc_some, calc_stuff, add_date

def test_parallel():
    with blazer.begin():
        tasks = [
            p(calc_stuff, 1),
            p(calc_stuff, 2),
            p(calc_stuff, 3),
            p(calc_stuff, 4),
            p(calc_stuff, 5)
        ]
        result = parallel(tasks)
        
        if blazer.ROOT:
            assert len(result) == len(tasks)
