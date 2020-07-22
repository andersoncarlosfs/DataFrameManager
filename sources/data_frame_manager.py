from warehouse_io.utils.context import Context

# proxying a data frame manager
if Context().gpu:
    from warehouse_io.utils.dfm.cudf_manager import cuDFManager as DataFrameManager
elif Context().scale:
    from warehouse_io.utils.dfm.dask_manager import DaskManager as DataFrameManager     
else:        
    from warehouse_io.utils.dfm.pandas_manager import PandasManager as DataFrameManager
    
__all__ = ['DataFrameManager']