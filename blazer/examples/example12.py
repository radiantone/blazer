""" Streaming map/reduce example """
from itertools import groupby
from random import randrange
from typing import Generator

import blazer
from blazer.hpc.mpi import stream


def datagen() -> Generator:
    for i in range(0, 1000):
        r = randrange(2)
        v = randrange(100)
        if r:
            yield {"one": 1, "value": v}
        else:
            yield {"zero": 0, "value": v}


def key_func(k):
    return k["key"]


def map(datum):
    datum["key"] = list(datum.keys())[0]
    return datum


def reduce(datalist):
    from blazer.hpc.mpi import rank

    _list = sorted(datalist, key=key_func)
    grouped = groupby(_list, key_func)
    return [{"rank": rank, key: list(group)} for key, group in grouped]


with blazer.begin():
    import json

    mapper = stream(datagen(), map, results=True)
    reducer = stream(mapper, reduce, results=True)
    if blazer.ROOT:
        for result in reducer:
            blazer.print("RESULT", json.dumps(result, indent=4))
