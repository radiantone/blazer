from typing import Generator

import blazer
from blazer.hpc.mpi import stream


def datagen() -> Generator:
    for i in range(0, 100):
        yield i


def myfunc(datum):
    from blazer.hpc.mpi import rank
    return "Hello[{}]".format(rank) + str(datum)


with blazer.begin():
    for result in stream(datagen(), myfunc, results=True):
        blazer.print("RESULT", result)
