def defer(func, *args, **kwargs):

    def decorator(*dargs, **dkwargs):
        return func(*dargs,**dkwargs)

    return decorator

def mpi(func, *args, **kwargs):

    def decorator(*dargs, **dkwargs):
        return func(*dargs,**dkwargs)

    return decorator