from functools import wraps
from os import path, makedirs
from pandas import read_csv


def upload_development_generate_csv(func):

    # TODO: Check environment to use or not
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        print("WARNING: The staging was not uploaded to CAROL")
        connector_name = args[0].connector_name
        df = args[1]
        staging = func.__name__

        data_path = f'data/{connector_name}'
        data_name = f'{staging}.csv'
        data_full = f'{data_path}/{data_name}'

        if not path.exists(data_path):
            makedirs(data_path)

        df.to_csv(path_or_buf=data_full, sep=';')

    return wrapper
