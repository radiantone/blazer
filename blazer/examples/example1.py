import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG
)
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
    result1 = parallel([
        p(calc_stuff, 1),
        p(calc_stuff, 2),
        p(calc_stuff, 3),
        p(calc_stuff, 4),
        p(calc_stuff, 5)
    ])
    blazer.print("PARALLEL1:", result1)

    if blazer.ROOT:
        r = list(
            result1
            | where(lambda g: where(lambda g: g['this'] > 1))
            | select(lambda g: p(calc_stuff, g['this'] * 2))
        )
        # Run the composed computation in parallel, wait for result
        result = parallel(r)
        blazer.print("PARALLEL2:", result)

    r = pipeline([
        p(calc_stuff, 'DATA'),
        p(pipeline, [
            calc_some,
            add_date
        ]),
        calc_stuff
    ])
    blazer.print("PIPELINE:", r)

    scatter_data = scatter(list(range(0, (size * 2) + 2)), calc_some)
    blazer.print("SCATTER_DATA:", scatter_data)

    result = pipeline([
        p(calc_stuff, INPUT_DATA),
        add_date,
        scatter_data,
        p(parallel, [
            calc_some,
            p(pipeline, [
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

    blazer.print("PIPELINE RESULT:", result)


    def get_data():
        """ Data generator """
        for i in range(0, (size * 2)):
            yield i


    result = scatter(get_data(), calc_some)
    blazer.print("SCATTER:", result)
