import blazer
from blazer.hpc.mpi import parallel, pipeline, partial as p, scatter, where, select, filter, rank, size


def calc_some(value, *args):
    """ Do some calculations """
    result = {'some': value}
    return result


def calc_stuff(value, *args):
    """ Do some calculations """
    result = {'this': value}
    return result


def add_date(result):
    from datetime import datetime
    if type(result) is dict:
        result['date'] = str(datetime.now())
    return result


def calc_more_stuff(result):
    """ Do some more calculations """
    if type(result) is list:
        result += [{'more': 'stuff'}]
    elif type(result) is dict:
        result['more'] = 'stuff'
    return result


INPUT_DATA = 'that'

with blazer.begin():

    def get_data():
        """ Data generator """
        for i in range(0, (size * 2)):
            yield i


    result = scatter(get_data(), calc_some)
    blazer.print("SCATTER:", result)
