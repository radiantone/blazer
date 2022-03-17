from functools import partial

from pipe import select, where
from pydash import chunk
from pydash import filter_ as filter
from pydash import flatten, get, omit

from .primitives import (
    fetch,
    host,
    map,
    mapreduce,
    parallel,
    pipeline,
    rank,
    reduce,
    scatter,
    shard,
    size,
    stream,
)

__all__ = (
    "parallel",
    "scatter",
    "pipeline",
    "map",
    "mapreduce",
    "reduce",
    "partial",
    "stream",
    "select",
    "where",
    "flatten",
    "chunk",
    "omit",
    "get",
    "rank",
    "host",
    "size",
    "filter",
    "shard",
    "fetch",
)
