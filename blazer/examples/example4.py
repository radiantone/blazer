import blazer
from blazer.hpc.mpi import map, reduce
from pipe import Pipe


def sqr(x):
    return x * x


def add(x, y=0):
    return x + y


if blazer.ROOT:
    print("R", 5 | add | sqr)

with blazer.begin():
    result = map(sqr, list(range(0, 100)))

    blazer.print(result)
    result = reduce(add, result)

    blazer.print(result)
