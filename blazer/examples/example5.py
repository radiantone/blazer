import blazer
from blazer.hpc.mpi import mapreduce


def add(values):
    if values and len(values):
        return sum(values)
    else:
        return 0


with blazer.begin():
    if blazer.ROOT:
        data = list(range(0, 100))
        print("DATA: ", data)
        print("EXPECTING: ", sum(data))
    else:
        data = None

    result = mapreduce(add, add, data)

    blazer.print("RESULT:", result)
