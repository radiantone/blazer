from random import randrange

import blazer
from blazer.hpc.mpi.primitives import rank

with blazer.environment() as vars:
    rv = randrange(10)
    vars["rank" + str(rank)] = [
        {"key": randrange(10)},
        randrange(10),
        randrange(10),
        randrange(10),
    ]

    print("RANK:", rank, "DATA", vars.vars)

blazer.print(vars["rank1"])
