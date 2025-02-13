import time
from functools import wraps


def timeit(func):
    @wraps(func)
    def timed(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()

        print("func: {} / took: {} sec".format(func.__name__, te - ts))
        return result

    return timed
