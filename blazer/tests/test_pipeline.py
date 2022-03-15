import blazer
from blazer.hpc.mpi import partial as p
from blazer.hpc.mpi import pipeline

from .funcs import add_date, calc_some, calc_stuff


def test_pipeline():
    with blazer.begin():
        r = pipeline(
            [p(calc_stuff, "DATA"), p(pipeline, [calc_some, add_date]), calc_stuff]
        )

        if blazer.ROOT:
            _true = "this" in r

            assert _true
