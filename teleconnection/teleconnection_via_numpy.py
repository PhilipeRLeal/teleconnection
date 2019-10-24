# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 14:25:15 2019

@author: lealp
"""

import pandas as pd
pd.set_option('display.width', 50000)
pd.set_option('display.max_rows', 50000)
pd.set_option('display.max_columns', 5000)

from dask.array import corrcoef as da_corrcoef
import numpy as np
import xarray as xr


####################33 numpy function:


def get_teleconnection_via_numpy(ds_chunked, variable='air', dim='time'):
    
    '''
    
    Function description:
        
        This is an faster version of the Teleconnection map.
        
        The only problem here is that it does not create a geopandas geoDataframe
        with the underlying connection Linestring paths.
    
    
    -------------------------------------------------------------------------
    
    Parameters:
        
        ds_chunked (3-D xarray-Dataset): this should contain the data to be 
            analyzed. The data must have 3 dimensions 
            (i.e.: longitude, latitude and time)
            
        
        variable of the xarray-dataset to be used in the analysis
        
        dim (string): the dimesion that will be used for correlation
        
    returns: xarray-dataarray containing the Teleconnection Map
    
    '''
    
    da = ds_chunked[variable]
    
    dims_keys = [x for x in ds_chunked.dims.keys()]
    
    listed_dims = [d for d in dims_keys if d != dim]
    
    locations_depth = np.prod([ds_chunked.coords[x].size for x in listed_dims])
    
    locations_shape = [ds_chunked.coords[x].size for x in listed_dims]
    
    
    correlation_dim_depth = ds_chunked.coords[dim].size
    
    
    to_shape = (correlation_dim_depth, locations_depth)
    
    print('to_shape', to_shape)
    
    Correlate = np.abs(da_corrcoef(da.data.reshape(to_shape), 
                       rowvar=False # to ensure that each column is an entry 
                                    # (i.e. a different location in space that
                                    # must be correlated with everyone else) 
                       
                       ).min(axis=1) # getting minimum for each location in space
                
                      ) # turning all to absolute values. 
    
    
    Teleconnection = xr.DataArray(Correlate.reshape(locations_shape), 
                                  dims=listed_dims, 
                                  coords = {listed_dims[0]: ds_chunked[listed_dims[0]], 
                                            listed_dims[1]: ds_chunked[listed_dims[1]]}, 
                                            name='Teleconnection')
    
    return Teleconnection




if '__main__' == __name__:
        
    
    
    ds = xr.tutorial.open_dataset('air_temperature').load()
    
    ds_month = ds.resample(time='M').mean('time')
    
    
    ds_chunked = ds_month.chunk({'lon': 1000, 'lat': 1000})
    
    
    
    Teleconnection_map_via_np = get_teleconnection_via_numpy(ds_chunked, variable='air')
        

    Teleconnection_map_via_np.where((Teleconnection_map_via_np > 0.3) & 
                                    (Teleconnection_map_via_np < 0.6), 
                                    drop=True).plot(cmap='viridis')
    
    