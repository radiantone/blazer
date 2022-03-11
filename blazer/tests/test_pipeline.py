import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter
from .funcs import calc_more_stuff, calc_some, calc_stuff, add_date

def test_pipeline():
    with blazer.begin():
        r = pipeline([
            p(calc_stuff, 'DATA'),
            p(pipeline, [
                calc_some,
                add_date
            ]),
            calc_stuff
        ])

        if blazer.ROOT:
            _true = 'this' in r

            assert _true 
