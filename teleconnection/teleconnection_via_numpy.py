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
from shapely.geometry import Point, LineString
import geopandas as gpd

from utils import Base_class_space_time_netcdf_gdf 

####################33 numpy function:


def get_gdf(Correlate, Teleconnection, index, crs={'init' :'epsg:4326'}):
    
        
    Teleconnection_paths = Correlate.to_dask_dataframe().idxmin(axis=1).compute()
    
    origin = Teleconnection_paths.index
    to_point = Teleconnection_paths.values
    
    Origin_points = []
    destination_points = []
    lat, lon = index.columns
    for o,p in zip(origin, to_point): 
    
        Origin_points.append(Point(index.iloc[o][lon], index.iloc[o][lat]))
        destination_points.append(Point(index.iloc[p][lon], index.iloc[p][lat]))
        
    Teleconnection_paths = [LineString( (op, dp)) for op, dp in zip(Origin_points, destination_points)]
    
    Teleconnection_paths = gpd.GeoDataFrame({'Teleconnection':Teleconnection}, 
                                            geometry=Teleconnection_paths,
                                            crs=crs,
                                            index=np.arange(Teleconnection.size))
    
    return Teleconnection_paths

def get_teleconnection_via_numpy(ds, variable='air', dim='time', Telecon_threshold= -0.5):
    
    '''
    
    Function description:
        
        This is an faster version of the Teleconnection map.
        
        The only problem here is that it does not create a geopandas geoDataframe
        with the underlying connection Linestring paths.
    
    
    -------------------------------------------------------------------------
    
    Parameters:
        
        ds(3-D xarray-Dataset): this should contain the data to be 
                                analyzed. The data must have 3 dimensions 
                                (i.e.: longitude, latitude and time).
                                
                                It can be a xarray-Dataset or 
                                a chunked xarray-Dataset.
            
        
        variable of the xarray-dataset to be used in the analysis
        
        dim (string): the dimesion that will be used for correlation
    
    -------------------------------------------------------------------------
    
    returns: xarray-dataarray containing the Teleconnection Map
    
    '''
    
    da = ds[variable]
    
    dims_keys = [x for x in ds.dims.keys()]
    
    listed_dims = [d for d in dims_keys if d != dim]
    
    locations_depth = np.prod([ds.coords[x].size for x in listed_dims])
    
    #locations_shape = [ds.coords[x].size for x in listed_dims]
    
    
    idx = pd.MultiIndex.from_product([ds.coords[x].values for x in listed_dims], names=listed_dims)
    
    index = idx.to_frame().reset_index(drop=True)
    
    correlation_dim_depth = ds.coords[dim].size
    
    
    to_shape = (correlation_dim_depth, locations_depth)
    
    
    da = da.data.reshape(to_shape)
	
    Correlate = da_corrcoef(da, 
                       rowvar=False # to ensure that each column is an entry 
                                    # (i.e. a different location in space that
                                    # must be correlated with everyone else) 
                            )
    
    
    Teleconnection = Correlate.min(axis=1) # getting minimum for each location in space
                
                      
    
    Teleconnection_paths = get_gdf(Correlate, Teleconnection, index)

    Teleconnection_paths = Teleconnection_paths[Teleconnection_paths['Teleconnection'] <= Telecon_threshold]
    

    # use from_series method
   
    
    Teleconnection = xr.DataArray(data=Correlate, 
                                              dims=idx.names,
                                              #coords=idx,
                                              name='Teleconnection')
    
    for name in index.columns:
        
        Teleconnection[name] = (name, index[name])
        
        Teleconnection = Teleconnection.sortby(name)
    
    return Teleconnection, Teleconnection_paths



def main( ds, variable='air', dim='time', Telecon_threshold= -0.5,
         netcdf_temporal_coord_name='time',
         longitude_dimension='lon',
         latitude_dimension='lat'):
    
    
    B = Base_class_space_time_netcdf_gdf(ds, 
                                         netcdf_temporal_coord_name=netcdf_temporal_coord_name,
                                         longitude_dimension=longitude_dimension,
                                         latitude_dimension=latitude_dimension)
    
    ds = B.netcdf_ds
    
    


    return get_teleconnection_via_numpy(ds, variable=variable, dim=dim, Telecon_threshold= Telecon_threshold)

if '__main__' == __name__:
        
    import cartopy.crs as ccrs
    import matplotlib.pyplot as plt
    
    
    
    
    ds = xr.tutorial.open_dataset('air_temperature').load()
    
    
    ds_month = ds.resample(time='M').mean('time')
    
    
    ds_chunked = ds_month.chunk({'lon': 1000, 'lat': 1000})
    
    
    
    
    Teleconnection, Teleconnection_paths = main(ds_chunked, variable='air')
        
    
    # Plotting all teleconnection map
    
    
    Projection =ccrs.PlateCarree()
    
    fig, axs = plt.subplots(1,2, subplot_kw={'projection':Projection})
    
    axs = axs.ravel()
    
    Teleconnection.plot(ax=axs[0], transform = Projection, cmap='viridis')
    
    Teleconnection_paths.plot(ax=axs[0], 
                              column='Teleconnection', 
                              transform = Projection,
                              cmap='viridis', 
                              label='Teleconnection')
    
     # Plotting only relevant teleconnection values
    Teleconnection.where((Teleconnection > 0.8), 
                                    drop=True).plot(ax=axs[1], 
                                                    transform=Projection, 
                                                    cmap='viridis')
    
    