
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