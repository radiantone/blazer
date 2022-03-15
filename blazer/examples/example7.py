import blazer
from blazer.hpc.mpi import mapreduce


def count(values):
    if values and len(values):
        return {i: values.count(i) for i in values}


def add(values):
    import collections

    counter = collections.Counter()
    for d in values:
        counter.update(d)

    return dict(counter)


with blazer.begin():
    if blazer.ROOT:
        data = ["one", "two", "two", "three", "three", "three"]
        print("DATA: ", data)
    else:
        data = None

    result = mapreduce(
        count,
        add,
        data,
    )

    blazer.print("RESULT:", result)
