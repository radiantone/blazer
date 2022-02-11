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
    
    _pipeline = p(pipeline,[
        calc_stuff,
        add_date,
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

    def get_data():
        for i in range(0,(size*2)+2):
            yield i

    # In this example we scatter a list of input data across
    # all the compute nodes and execute a complex workflow on
    # every node for each chunk of data it receives
    # The data provided can be a generator as well to avoid memory
    # consumption
    scatter_data = scatter(get_data(), _pipeline)
    blazer.print("SCATTER_DATA:",scatter_data)
