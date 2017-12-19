import os
import pickle
import pandas as pd


def to_pickle(filepath, obj):
    with open(filepath, 'wb') as fp:
        pickle.dump(obj, fp)


def from_pickle(filepath):
    obj = None
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as fp:
            obj = pickle.load(fp)
        if obj is None:
            raise ValueError('`obj` must not be None.')
    return obj


def cache_located_at(filepath):
    def _func(f):
        def __func(*args, **kwargs):
            obj = from_pickle(filepath)
            if obj is not None:
                return obj

            result =  f(*args, **kwargs)

            to_pickle(filepath, result)
            return result
        return __func
    return _func


def load_pd_dataframe(filepath):
    r = from_pickle(filepath)
    if not isinstance(r, pd.DataFrame):
        raise TypeError('`r` must be pd.DataFrame.')
    return r
