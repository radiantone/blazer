![Blazer Logo](./img/blazer-logo-tiny.svg)


An HPC abstraction over MPI that uses pipes and pydash primitives. Blazer will handle all the MPI orchestration behind the scenes for you. You just work strictly with data and functions. Easy!

### Install

From pypi
```bash
$ pip install blazer
```

From clone
```bash
$ git clone https://github.com/radiantone/blazer
$ cd blazer
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
(venv) $ python setup.py install
```

### Tests
```bash
(venv) $ mpirun -n 2 python setup.py test
blazer/tests/test_parallel.py::test_parallel PASSED                      [ 50%]
blazer/tests/test_pipeline.py::test_pipeline PASSED                      [100%]

============================== 2 passed in 0.48s ===============================
ctrl-c
```

### Examples
```python
import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter, where, select, filter, rank, size

def calc_some(value, *args):
    """ Do some calculations """
    result = { 'some': value }
    return result

def calc_stuff(value, *args):
    """ Do some calculations """
    result = { 'this': value }
    return result

def add_date(result):
    from datetime import datetime
    if type(result) is dict:
        result['date'] = str(datetime.now())
    return result

def calc_more_stuff(result):
    """ Do some more calculations """
    if type(result) is list:
        result += [{'more':'stuff'}]
    elif type(result) is dict:
        result['more'] = 'stuff'
    return result

INPUT_DATA = 'that'

with blazer.begin():
    
    result1=parallel([ 
        p(calc_stuff, 1),
        p(calc_stuff, 2),
        p(calc_stuff, 3),
        p(calc_stuff, 4),
        p(calc_stuff, 5)
    ])
    blazer.print("PARALLEL1:",result1)

    if blazer.ROOT:
        r = list(
            result1
            | where(lambda g: where(lambda g: g['this'] > 1))
            | select(lambda g: p(calc_stuff, g['this']*2))
        )
        # Run the composed computation in parallel, wait for result
        result = parallel(r)
        blazer.print("PARALLEL2:",result)

    r=pipeline([
        p(calc_stuff, 'DATA'),
        p(pipeline, [
            calc_some,
            add_date
        ]),
        calc_stuff
    ])
    blazer.print("PIPELINE:",r)

    scatter_data = scatter(list(range(0,(size*2)+2)), calc_some)
    blazer.print("SCATTER_DATA:",scatter_data)

    result = pipeline([
        p(calc_stuff, INPUT_DATA), 
        add_date,
        scatter_data,
        p(parallel,[ 
            calc_some,
            p(pipeline,[
                calc_stuff,
                p(parallel, [
                    calc_some,
                    calc_some
                ]),
                calc_stuff
            ]),
            calc_some
        ]),
        calc_more_stuff
    ])

    blazer.print("PIPELINE RESULT:",result)

    def get_data():
        """ Data generator """
        for i in range(0,(size*2)):
            yield i

    result = scatter(get_data(), calc_some)
    blazer.print("SCATTER:",result)

```

To run:
```
(venv) $ export PYTHONPATH=.
(venv) $ mpirun -n 4 python blazer/examples/example1.py 
PARALLEL1: [{'this': 1}, {'this': 2}, {'this': 3}, {'this': 4}, {'this': 5}]
PARALLEL2: [{'this': 4}, {'this': 6}, {'this': 2}, {'this': 8}, {'this': 10}]
PIPELINE: {'this': {'some': ({'this': 'DATA'},), 'date': '2022-02-11 02:47:23.356461'}}
SCATTER_DATA: [{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}, {'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}, {'some': 8}, {'some': 9}, {'some': None}, {'some': None}]
PIPELINE RESULT: [{'this': [{'this': ([{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}, {'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}, {'some': 8}, {'some': 9}, {'some': None}, {'some': None}],)}, {'some': {'some': [{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}, {'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}, {'some': 8}, {'some': 9}, {'some': None}, {'some': None}]}}]}, {'some': 'some'}, {'more': 'stuff'}]
[0, 1, 2, 3, 4, 5, 6, 7]
SCATTER: [{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}, {'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}]
```

A map/reduce example

```python
import blazer
from blazer.hpc.mpi import map, reduce

def sqr(x):
    return x * x

def add(x, y=0):
    return x+y

with blazer.begin():
    result = map(sqr, [1, 2, 3, 4])

    blazer.print(result)
    result = reduce(add, result)

    blazer.print(result)
```

To run:
```
(venv) $ export PYTHONPATH=.
(venv) $ mpirun -n 4 python blazer/examples/example3.py 
[1, 4, 9, 16]
30
```

> NOTE: blazer has only been tested on `mpirun (Open MPI) 4.1.0`
## Overview

Blazer is a _high-performance computing_ (HPC) library that hides the complexities of a super computer's _message-passing interface_ or (MPI).
Users want to focus on their code and their data and not fuss with low-level API's for orchestrating results, building pipelines and running fast, parallel code. This is why blazer exists!

With blazer, a user only needs to work with simple, straightforward python. No cumbersome API's, idioms, or decorators are needed.
This means they can get started quicker, run faster code, and get their jobs done _faster_!

### General Design

Blazer is designed around the concept of computing _primitives_ or operations. Some of the primitives include:

- **parallel** - For computing a list of tasks in parallel
- **pipeline** - For computing a list of tasks in sequence, passing the results along
- **map** - For mapping a task to a dataset
- **reduce** - For mapping a task to a data list and computing a single result

In addition there are other primitives to help manipulate lists of tasks or data, such as:

- **where** - Filter a list of tasks or data elements based on a function or lambda
- **select** - Apply a function to each list element and return the result

### Context Handlers

Blazer uses convenient context handlers to control blocks of code that need to be scheduled to MPI processes behind the scenes.
There are two types of context handlers currently. 

#### MPI Context Handler

`blazer.begin()` is a mandatory context that enables the MPI scheduler behind the various primitives to operate correctly.

```python

import blazer

blazer.begin():
    def get_data():
        """ Data generator """
        for i in range(0, (size * 2)):
            yield i

    result = scatter(get_data(), calc_some)
    blazer.print("SCATTER:", result)

```
#### GPU Context Handler

`blazer.gpu()` is a context that requests (from the invisible MPI scheduler) dedicated access to a specific GPU on your MPI node fabric.

```python
import logging
import blazer
import numpy as np

from blazer.hpc.mpi.primitives import host, rank
from numba import vectorize
from timeit import default_timer as timer

def dovectors():

    @vectorize(['float32(float32, float32)'], target='cuda')
    def dopow(a, b):
        return a ** b

    vec_size = 100

    a = b = np.array(np.random.sample(vec_size), dtype=np.float32)
    c = np.zeros(vec_size, dtype=np.float32)

    start = timer()
    dopow(a, b)
    duration = timer() - start
    return duration

with blazer.begin(gpu=True):  # on-fabric MPI scheduler
    with blazer.gpu() as gpu:  # on-metal GPU scheduler
        # gpu object contains metadata about the GPU assigned
        print(dovectors())
```

### Cross-Cluster Supercomputing

Blazer comes with a built-in design pattern for performing cross-cluster HPC. This is useful if you want to allocate compute resources on different super-computers and then build a pipeline of jobs across them. Here is a simple example using ALCF's Cooley and Theta systems (which are built into blazer).

```python
from blazer.hpc.alcf import cooley, thetagpu
from blazer.hpc.local import parallel, pipeline, partial as p

# Log into each cluster using MFA password from MobilePASS
cooleyjob   = cooley.job(user='dgovoni', n=1, q="debug", A="datascience", password=True, script="/home/dgovoni/git/blazer/testcooley.sh").login()       
thetajob    = thetagpu.job(user='dgovoni', n=1, q="single-gpu", A="datascience", password=True, script="/home/dgovoni/git/blazer/testthetagpu.sh").login()

def hello(data, *args):
    return "Hello "+str(data)

# Mix and match cluster compute jobs with local code tasks
# in serial chaining
cooleyjob("some data").then(hello).then(thetajob).then(hello)

# Run a cross cluster compute job
result = pipeline([
    p(thetajob,"some data2"),
    p(cooleyjob,"some data1")
])

print("Done")
```

When each job `.login()` method is run, it will gather the MFA login credentials for that system and then use that to schedule jobs on that system via ssh. 

Notice the use of the `pipeline` primitive above. It's the same primitive you would use to build your compute workflows! Composable tasks and composable super-computers.