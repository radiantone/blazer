# blazer
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

r=parallel([ 
    p(calc_stuff, 1),
    p(calc_stuff, 2)
])
blazer.print("PARALLEL:",r)

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

blazer.stop()
```
