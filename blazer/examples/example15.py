import blazer
from blazer.hpc.mpi import fetch, rank, shard

with blazer.begin():

    handle = shard([0, 1, 2, [3, 3, 3], 4, 5, 6, 7, 8, 9])
    print("HANDLE[{}]{}".format(rank, handle))
    data = fetch(handle, stream=True)

    for d in data:
        blazer.print("D:", d)

    data = fetch(handle)
    blazer.print("DATA", data)
