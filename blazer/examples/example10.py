import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter, where, select, filter, rank, size

with blazer.begin():
   print("Done")
