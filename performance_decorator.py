import time
from functools import wraps
from typing import Callable


def timing_decorator(f: Callable) -> Callable:
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = f(*args, **kwargs)
        print(f'Archive {f.__name__} work time: {time.perf_counter() - start:.7f}')
        return result

    return wrapper