import pandas as PandasManager

def concatenate(df_origin, df_target):
    return df_origin.append(df_target)

def cumulative_count(df, columns):
    return df.groupby(column).cumcount()

def get_metadata(df):
    df_distinct = df.T.apply(lambda x: x.nunique(), axis = 1)
    df_duplicates = df.apply(lambda x: x.duplicated()).sum()
    df_types = df.dtypes

    max_rows =  PandasManager.get_option('display.max_rows')
    max_columns = PandasManager.get_option('display.max_columns')

    PandasManager.set_option('display.max_rows', len(df))
    PandasManager.set_option('display.max_columns', None)

    df = PandasManager.concat([df_distinct, df_duplicates, df_types], axis = 1, keys = ['uniques', 'duplicates', 'type']).sort_index()

    PandasManager.set_option('display.max_rows', max_rows)
    PandasManager.set_option('display.max_columns', max_columns)  

    return df

def new(data = None, index = None, columns = None):
    return PandasManager.DataFrame(data = data, index = index, columns = columns)           

def read(path, media_type, index_col = None, header = 'infer', sep = ',', encoding = None):

    # TO IMPROVE
    if media_type is 'csv':
        df = PandasManager.read_csv(path, index_col = index_col, header = header, sep = sep, encoding = encoding)
    else:
        raise NotImplementedError('PandasManager.read_'.join(media_type))

    return df

def to_dictionnary(df):
    return df.to_dict()

def execute(df):
    return df

def order_values(df, by, ascending = True):
    return df.sort_values(by = by, ascending = ascending)         

# TO RECHEK
PandasManager.concatenate = concatenate
PandasManager.cumulative_count = cumulative_count
PandasManager.get_metadata = get_metadata
PandasManager.execute = execute  
PandasManager.new = new
PandasManager.order_values = order_values
PandasManager.read = read
PandasManager.to_dictionnary = to_dictionnary  

__all__ = ['PandasManager']