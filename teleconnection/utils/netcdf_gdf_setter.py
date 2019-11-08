# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 18:08:05 2019

@author: lealp
"""

import pandas as pd
pd.set_option('display.width', 50000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
from shapely.geometry import Point
import geopandas as gpd

import numpy as np



class Base_class_space_time_netcdf_gdf(object):
    def __init__(self, xarray_dataset=None, 
                 netcdf_temporal_coord_name='time',
                 longitude_dimension='lon',
                 latitude_dimension='lat',
				):
        
        
        '''
        Class description:
        ------------------
        
            This class is a base class for ensuring that the given netcdf is in conformity with the algorithm.
            
            Ex: the Netcdf has to be sorted in ascending order for all dimensions (ex: time ,longitude, latitude). 
            Otherwise, the returned algorithm would return Nan values for all slices
            
            Also, it is mandatory for the user to define the longitude and latitude dimension Names (ex: 'lon', 'lat'),
            since there is no stadardization for defining these properties in the Netcdf files worldwide.
            
            
            
        Attributes:
            
            xarray_dataset (None): 
            -----------------------
            
                the Xarry Netcdf object to be analyzed
                
                
            netcdf_temporal_coord_name (str = 'time'): 
            -----------------------------------
            
                the name of the time dimension in the netcdf file
             
                
            longitude_dimension (str = 'lon'): 
            ----------------------------------
            
                the name of the longitude/horizontal dimension in the netcdf file
            
            
            latitude_dimension (str = 'lat'): 
            ----------------------------------
                the name of the latitude/vertical dimension in the netcdf file
        

        
        '''
        self.longitude_dimension = longitude_dimension
        
        self.latitude_dimension = latitude_dimension
        self.__netcdf_ds = xarray_dataset
        
        
        self.__netcdf_ds = self.__netcdf_ds.sortby( [longitude_dimension, 
                                                    latitude_dimension, 
                                                    netcdf_temporal_coord_name])
        
        self._temporal_coords = netcdf_temporal_coord_name

        # setting Standard spatial_coords/dimension names 
        self.spatial_coords = dict(x = longitude_dimension,
                                   y = latitude_dimension)
        self._convert_lat_180_to_90()
        self._convert_long_360_to_180()
        
    
    @ property    
    def temporal_coords(self):
        return self._temporal_coords
    
    
	
    @ property    
    def netcdf_ds(self):
        return self.__netcdf_ds
    
    @ netcdf_ds.setter
    def netcdf_ds(self, new_netcdf_ds):
        
        self.__netcdf_ds = new_netcdf_ds
        
    
        
    def _convert_long_360_to_180(self):

        self.netcdf_ds = self.netcdf_ds.assign_coords({self.longitude_dimension :(((self.netcdf_ds[self.longitude_dimension] + 180) % 360) - 180)})
    
    def _convert_lat_180_to_90(self):
        
    
        self.netcdf_ds = self.netcdf_ds.assign_coords({self.latitude_dimension:( ((self.netcdf_ds[self.latitude_dimension] + 90) % 180) - 90)})
        
        
    def _get_coords_resolution(self, netcdf_ds, dimension):
        
        dx = np.diff(netcdf_ds[dimension][:2])[0]
            
        return np.abs(dx)

    def coord_resolution(self, dimension='longitude'):
        
        return self._get_coords_resolution(self.netcdf_ds, dimension)
    
    
    def netcdf_to_gdf(self, netcdf_ds):
        
        netcdf_as_dataframe = netcdf_ds.to_dataframe().reset_index()
        
        netcdf_as_dataframe.loc[:, 'geometry'] = None
        
        netcdf_as_dataframe['geometry'] = [Point([x,y]) for x,y in zip(netcdf_as_dataframe[self.spatial_coords['x']], netcdf_as_dataframe.reset_index()[self.spatial_coords['y']])]
        
        
        
        netcdf_as_dataframe = gpd.GeoDataFrame(netcdf_as_dataframe, crs=self.gdf.crs)
        
        return netcdf_as_dataframe
    
    