import blazer
from blazer.hpc.mpi import map, reduce


def sqr(x):
    return x * x


def add(x, y=0):
    return x + y


with blazer.begin2():
    result = map(sqr, list(range(0, 100)))

    blazer.print(result)
    result = reduce(add, result)

    blazer.print(result)

