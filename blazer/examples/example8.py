import blazer

from blazer.hpc.mpi import mapreduce, reduce, rank
import blazer.hpc.alcf.cooley as cooley
import blazer.hpc.alcf.thetagpu as thetagpu


def count(values):
    if values and len(values):
        return {i:values.count(i) for i in values}

def add(values):
    import collections

    counter = collections.Counter()
    for d in values: 
        counter.update(d)

    return dict(counter)

with cooley.run():
    with blazer.begin(stop=False):
        if blazer.ROOT:
            data = ['one','two','two','three','three','three']
            print("DATA: ",data)
        else:
            data = None

        result = mapreduce(count, add, data, require_list=True)

        blazer.print("RESULT:", result)

with thetagpu.run():
    with blazer.begin():
        if blazer.ROOT:
            data = ['one','two','two','three','three','three']
            print("DATA: ",data)
        else:
            data = None

        result = mapreduce(count, add, data, require_list=True)

        blazer.print("RESULT:", result)