import numpy

import dask.dataframe as DaskManager 

def concatenate(df_origin, df_target):
    raise NotImplementedError('DaskManager.concatenate') 

def cumulative_count(df, columns):
    raise NotImplementedError('DaskManager.cumulative_count') 

def get_metadata(df):
    raise NotImplementedError('DaskManager.get_metadata')     

# dask.dataframe does not yet support empty DataFrame
def new(data = None, index = None, columns = None):
    if data is None:
        data = numpy.array([])
        
    df = DaskManager.from_array(data, columns = columns)
        
    if index:
        df.index = DaskManager.from_array(numpy.array(index))
        
    return df

# keywords 'index' and 'index_col' not yet supported by dask.dataframe
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
def read(path, media_type, index_col = None, header = 'infer', sep = ',', encoding = None):

    # TO IMPROVE
    if media_type is 'csv':
        df = DaskManager.read_csv(path, header = header, sep = sep, encoding = encoding)
    else:
        raise NotImplementedError('DaskManager.read_'.join(media_type))

    if index_col:
        df.set_index(df.columns[index_col], inplace = True)
        
    return df        

def to_dictionnary(df):
    raise NotImplementedError('DaskManager.to_dictionnary') 

def execute(df):
    return df.compute()

def order_values(df, by, ascending = True):
    rows = df.shape[0].compute()
    if ascending:
        return df.nsmallest(rows, by)  
    return df.nlargest(rows, by)  

# TO RECHEK
DaskManager.concatenate = concatenate
DaskManager.cumulative_count = cumulative_count
DaskManager.get_metadata = get_metadata
DaskManager.execute = execute  
DaskManager.new = new
DaskManager.order_values = order_values
DaskManager.read = read
DaskManager.to_dictionnary = to_dictionnary

__all__ = ['DaskManager']