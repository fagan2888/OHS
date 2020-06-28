from numpy import vectorize as np_vc
from numpy import float64 as np_float64
from numpy import nan as np_nan
from typing import Callable
from multiprocessing import Pool


POOL = None


def pool_init(core_count: int = 8) -> Pool:
    global POOL

    if POOL is None:
        POOL = Pool(core_count)

    return POOL


def _no_except(func: Callable) -> Callable:
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException:
            return np_nan
    return wrapped


def _vec(func: Callable) -> Callable:
    def wrapped(*args, **kwargs):
        vec_func = np_vc(func, cache=True, otypes=[np_float64])
        return vec_func(*args, **kwargs)
    return wrapped
