from typing import Generator

import blazer
from blazer.hpc.mpi import scatter


def datagen() -> Generator:
    for i in range(0, 100):
        yield i


def myfunc(datum):
    from blazer.hpc.mpi import rank

    return "Hello[{}]".format(rank) + str(datum)


with blazer.begin():
    results = scatter(datagen(), myfunc)
    blazer.print("RESULT", results)
    """
    # A version that iterates over results
    for result in scatter(datagen(), myfunc):
        blazer.print("RESULT",result)
    """
