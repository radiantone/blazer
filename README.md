![Blazer Logo](./img/blazer-logo.svg)


An HPC abstraction over MPI that uses pipes and pydash primitives.

```python
import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter, where, select, filter, rank

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
            calc_some
        ]),
        calc_stuff
    ])
    blazer.print("PIPELINE:",r)

    result = pipeline([
        p(calc_stuff, INPUT_DATA), 
        add_date,
        p(parallel,[ 
            calc_some,
            p(pipeline,[
                calc_stuff,
                calc_stuff
            ]),
            calc_some
        ]),
        calc_more_stuff
    ])

    blazer.print("PIPELINE RESULT:",result)

```

To run:
```
(venv) $ mpirun -n 4 python blazer/examples/example1.py 
PARALLEL1: [{'this': 1}, {'this': 2}, {'this': 3}, {'this': 4}, {'this': 5}]
PARALLEL2: [{'this': 4}, {'this': 6}, {'this': 2}, {'this': 8}, {'this': 10}]
PIPELINE: {'this': {'some': {'some': ({'this': 'DATA'},)}}}
PIPELINE RESULT: [{'this': {'some': {'this': 'that', 'date': '2022-02-08 09:37:07.417365'}}}, {'some': 'this'}, {'more': 'stuff'}]
```