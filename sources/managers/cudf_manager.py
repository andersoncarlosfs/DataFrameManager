import collections
import inspect

import numpy

import cudf as cuDFManager

from collections import abc

def cumulative_count(df, columns):
    return df[[column]].groupby(columns, method = 'cudf').apply(lambda row: cuDFManager.Series(numpy.arange(len(row)), row.index))

def concatenate(df_origin, df_target):
    return cuDFManager.concat([df_origin, df_target])

def get_metadata(df):
    raise NotImplementedError('cuDFManager.get_metadata')     

def new(columns):
    return cuDFManager.DataFrame(columns = columns)      

def read(path, media_type, index_col = None, header = 'infer', sep = ','):

    # TO REDO
    if media_type is 'csv':
        df = cuDFManager.read_csv(path, index_col = index_col, header = header, sep = sep)
    else:
        raise NotImplementedError('cuDFManager.read_'.join(media_type))

    return df

def iteritemseries(series):
    return zip(iter(series.index), iter(series))

def my_standardize_mapping(into):
    if not inspect.isclass(into):
        if isinstance(into, collections.defaultdict):
            return partial(collections.defaultdict, into.default_factory)
        into = type(into)
    if not issubclass(into, abc.Mapping):
        raise TypeError(f"unsupported type: {into}")
    elif into == collections.defaultdict:
        raise TypeError("to_dict() only accepts initialized defaultdicts")
    return into

def to_dicts(series, into=dict):
    into_c = my_standardize_mapping(into)
    return into_c(iteritemseries(series))

def to_dictionnary(df, into=dict):
    into_c = my_standardize_mapping(into)
    return into_c((k, to_dicts(v)) for k, v in df.iteritems())

def execute(df):
    return df

# TO RECHEK
cuDFManager.concatenate = concatenate
cuDFManager.cumulative_count = cumulative_count
cuDFManager.get_metadata = get_metadata
cuDFManager.execute = execute  
cuDFManager.new = new
cuDFManager.order_values = order_values
cuDFManager.read = read
cuDFManager.to_dictionnary = to_dictionnary 

__all__ = ['cuDFManager']