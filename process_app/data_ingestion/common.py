from functools import wraps
from os import path, makedirs
from pandas import read_csv


def ingestion_development_cache(func):

    # TODO: Check environment to use or not

    @wraps(func)
    def wrapper(*args, **kwargs):
        connector_name = args[0].connector_name
        staging = func.__name__

        data_path = f'data/{connector_name}'
        data_name = f'{staging}.csv'
        data_full = f'{data_path}/{data_name}'

        if path.exists(data_full):
            print("WARNING: The staging was not downloaded (Using cache)")
            return read_csv(filepath_or_buffer=f'{data_path}/{data_name}', sep=';')
        else:
            if not path.exists(data_path):
                makedirs(data_path)
            df = func(*args, **kwargs)
            df.to_csv(path_or_buf=data_full, sep=';')
            return df

    return wrapper
