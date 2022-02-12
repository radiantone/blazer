import blazer
from blazer.hpc.mpi import map, reduce

def mult(x,y):
    return x * y

with blazer.begin():
    result = map(mult, [(1,1), (2,2), (3,3), (4,4)])

    blazer.print(result)
    result = reduce(mult, [(1,1), 2, 3, 4])

    blazer.print(result)
