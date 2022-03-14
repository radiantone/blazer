# salloc allocates the matching resources. 
# salloc -N 2 requests 2 nodes (physical servers)
#        -n requests # of processors,slots
# mpirun -np 3 requests 3 processes out of max 8 per salloc -n
#        this equates to ranks
NODES=2
CPUS=8
PROCS=$CPUS

salloc -n $CPUS -N $NODES mpirun -np $PROCS $PWD/venv/bin/python $1


