import blazer
from blazer.hpc.mpi import map, reduce

def sqr(x):
    return x * x

def add(x, y=0):
    return x+y

with blazer.begin():
    result = map(sqr, [1, 2, 3, 4])

    blazer.print(result)
    result = reduce(add, result)

    blazer.print(result)
