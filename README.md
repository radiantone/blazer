![Blazer Logo](./img/blazer-logo-tiny.svg)


An HPC abstraction over MPI that uses pipes and pydash primitives. Blazer will handle all the MPI orchestration behind the scenes for you. You just work strictly with data and functions. Easy!

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
(venv) $ mpirun -n 4 python blazer/examples/example1.py 
PARALLEL1: [{'this': 1}, {'this': 2}, {'this': 3}, {'this': 4}, {'this': 5}]
PARALLEL2: [{'this': 4}, {'this': 6}, {'this': 2}, {'this': 8}, {'this': 10}]
PIPELINE: {'this': {'some': ({'this': 'DATA'},), 'date': '2022-02-10 09:02:33.794571'}}
SCATTER_DATA: [[{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}], [{'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}], [{'some': 8}, {'some': 9}, {'some': None}, {'some': None}]]
PIPELINE RESULT: [{'this': [{'this': ({'this': 'that', 'date': '2022-02-10 09:02:33.796205'},)}, {'some': {'some': {'this': 'that', 'date': '2022-02-10 09:02:33.796205'}}}]}, {'some': 'some'}, {'more': 'stuff'}]
SCATTER: [[{'some': 0}, {'some': 1}, {'some': 2}, {'some': 3}], [{'some': 4}, {'some': 5}, {'some': 6}, {'some': 7}]]
```