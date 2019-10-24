# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 18:31:50 2019

@author: lealp
"""


import pandas as pd

# for better visualization in Ipython.

pd.set_option('display.width', 50000)
pd.set_option('display.max_rows', 50000)
pd.set_option('display.max_columns', 5000)


import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
from scipy import stats
from dask.diagnostics import ProgressBar
import os
from shapely.geometry import Point
from shapely.geometry import LineString
import geopandas as gpd


# https://stackoverflow.com/questions/51680659/disparity-between-result-of-numpy-gradient-applied-directly-and-applied-using-xa/51690873#51690873


def k_cor(x,y):
    """
    
    Function description:
        Uses the scipy stats module to calculate a Kendall correlation test
    
    ---------------------------------------------------------------------
    
    Parameters:
        x (array): Input pixel vector to run tests on
        y (array): The date input vector
    
    ---------------------------------------------------------------------
    
    Return:
        the stats.kendalltau correlation value which varies between -1 and 1.
    
    """
    
    
   
    tau, p_value = stats.kendalltau(x, y)

    # Criterium to return results in case of Significance
    return tau

# The function we are going to use for applying our kendal test per pixel
    

def kendall_correlation(x,y,dim='month'):
    '''
    Function description:
        
        This is a wrapper of the "k_cor" function 
        that uses the xarray.apply_ufunc for fast processing.
        
    ---------------------------------------------------------------------
    
    
    Return (2D-xarray-DataArray):
        
        This function returns a 2D netcdf-xarray with the dimension "dim" 
        reduced by the correlation function. 
        
        Therefore, for a xarray-DataArray of coordinates (lon, lat, time),
        
        the returned netcdf of this function would have coordinates (lon, lat),
        
        and the values will be between (-1,1).
        
    '''
    
    # x = Pixel value, y = a vector containing the date, dim == dimension
    
    
    return xr.apply_ufunc(
        k_cor, x , y,
        dask='allowed',
        input_core_dims=[dim, dim],
        vectorize=True, # !Important!
        output_dtypes=[float]
        )
    

########### To apply over all points:
    
def get_correlation_for_x_pixel(x, dataArray, dim='time', see_progressBar=False):
    '''
    Function description:
        
        This function is a wrapper of the kendall_correlation function.
    
        It allows to see the progress bar between correlogram plots.
        
        
     ---------------------------------------------------------------------
    Parameters:
        
        x (array): an array as reference for evaluation of the correlogram 
                    of the dataSet
    
    
        ----------------------------
    
    
        dataArray (xarray-dataArray): 
            the dataArray to be used in the correlogram plot.
        
        
        dim (string): the dimension that will be reduced from 
                      the xarray-Dataarray.
        
        
        see_progressBar (boolean): a boolean parameter to allow the user to 
        
            see the evolution of the analysis:
                
     ---------------------------------------------------------------------
    
    
     Return (2D-xarray-DataArray):
        
        This function returns a 2D netcdf-xarray with the dimension "dim" 
        reduced by the correlation function. 
        
        Therefore, for a xarray-DataArray of coordinates (lon, lat, time),
        
        the returned netcdf of this function would have coordinates (lon, lat),
        
        and the values will be between (-1,1).
        
        
    '''
    if see_progressBar==False:
        r = kendall_correlation(dataArray, x ,[dim]).compute()  
        
    else:
            
        with ProgressBar():
        # Until 'compute' is run, no computation is executed
            r = kendall_correlation(dataArray, x ,[dim]).compute()  
            
    return r 


def get_Point_from_x(P):
    '''
    Function description:
    
        This function is wrapped inside the "get_geoseries_for_teleconnection_line_path" function.
        
        It converts a given dictionary into a Shapely-point object.
    
    
    ------------------------------------------------------------------
    
    Parameters:
        P (dict): it should contain a lon and a lat key named attributes with respective float values for
        convertion to shapely-Point object
    '''
    
    return Point(P['lon'], P['lat'])
    

def get_geoseries_for_teleconnection_line_path(x, point):
    
    '''
    
    Function description:
        
        This function converts a pair of points (x, point) into a 
        Geopandas-geoseries of a single LineString.
        This LineString describes the linear path between the 
        most negative correlated coordinate points in the xarray.
        
        The most negative correlated coordinate is the Teleconnection 
        we want to evaluate.
        
    ------------------------------------------------------------------
        
    Parameters:
        x: the reference point from the xarray that has been used to generate 
            the correlation plot over the whole netcdf coordinate 
            plane (lon, lat).
        
        point: the point of maximum teleconnection in respect to 'x'.
        
    
    ------------------------------------------------------------------    
        
    Returns:
        
        Geopandas-Geoseries: which contains the linear 
                            path (Shapely-linestrings) connecting 
                            ('x' and 'point')
        
    
    '''
    # creating a dictionary of all connected maximum points:
    Teleconnection_Point = get_Point_from_x(point)
    
    
    
    X_point = get_Point_from_x(x)
    
    
    Teleconnection_Line_path = LineString([X_point, Teleconnection_Point])
    
    
    GS = gpd.GeoSeries({'Correlation':np.abs(point.values.ravel()[0]), 
                        'geometry': Teleconnection_Line_path})
    
    return GS

def get_teleconnection_line_path(r_correlation_map, x, 
                                 variable, 
                                 coordinate_names = {'lat':'lat', 'lon':'lon'}):
    
    '''
    Function description:
    
        
        This function finds the teleconnection point of 'x' 
        inside of the r_correlation_map, and generates a geodataframe of all
        connection paths between each pair of teleconnecting points.
    
    ------------------------------------------------------------------
    Parameters:
    
        r_correlation_map(xarray-Dataset): it should contain the correlation
            values in respect to x over all pair of coordinate points in the
            
            xarray Map.
    
        
        ----------------------
        
        x (2D-xarray-Dataarray): it should  contain the coordinates that 
        
            has been used for the r_correlation_map generation.
        
        
        ----------------------
        
            
        coordinate_names (dict = {'lat':'lat', 'lon':'lon'}):
            
            The coordinate_names is a dictionary serves as a naming 
            standardizer object, that must contain the name of
            each coordinate in the xarray-DataSet/dataArray, except the one
            that is used for the correlation (i.e.: one must not use 'time')
        
    ------------------------------------------------------------------
    Returns:
    
        
        Geopandas-Geoseries: which contains the linear 
                            path (Shapely-linestrings) connecting 
                            ('x' and its respective 'Teleconnecting point')
    
    '''
    
    Tele_connection_for_point_x = r_correlation_map[variable]
    
    P = (Tele_connection_for_point_x.where(Tele_connection_for_point_x == np.unique(Tele_connection_for_point_x.min()), 
                                          drop=True)

        )
    
    
    if hasattr(P, coordinate_names['lon']) & hasattr(P, coordinate_names['lat']):
        
        
        if P[coordinate_names['lon']].size > 1:
            P = P.isel({coordinate_names['lon']:0})
            
        if P[ coordinate_names['lat']  ].size > 1:  
        
            P = P.isel({coordinate_names['lat']:0})
        
        
        if (P[ coordinate_names['lat'] ].size == P[ coordinate_names['lon']  ].size) & (P[ coordinate_names['lat'] ].size == 1):
            return get_geoseries_for_teleconnection_line_path(x, P)
        
        else:
            pass
        
    else:
        
        # in case no Point (longitude and latitude) is selected.
        # sometimes, a single coordinate gets missing (i.e.: longitude)
        
        # as a result, only an 1D-array (i.e.: latitude) remains.
        
        # i.e.: 
            
            # 1) no latitude for a set of longitude coordinates
            
            # 2) no longitude for a set of latitude coordinates
            
        
        # This problem is solved through this 'if-else' statement.
        
        pass
        
        
        
counter = 0

def get_correlation_for_each_pixel(dataSet, variable='air', 
                                   coordinate_names = {'lat':'lat', 'lon':'lon'}, 
                                   dim='time',
                                   see_progressBar=False, 
                                   verbose=True, 
                                   make_partial_plots={'condition':False,
                                                       'figure_base_path_save':r'C:\Users\lealp\Downloads\temp\imagens'}
                                   
                                   ):
    
    '''
    
    Function Description:
    
    This is a wrapper of all functions above,
    which iterates over each pixel in the passed xarray-Dataset retrieving each
    respective Teleconnection map and respective Connection-path (LineString)
    of each pair of coordinates.
    
    ------------------------------------------------------------------
    
    Parameters:
        ds_chunked
        
        
    ------------------------------------------------------------------
    
    
    Returns:
    
    '''
    
    global counter
    
    Teleconnection_Linepaths_gdf =  gpd.GeoDataFrame()
    
    dataArray=dataSet[variable]
    
    dsx = []
    for lon in dataSet.coords[ coordinate_names['lon'] ].values:
        for lat in dataSet.coords[ coordinate_names['lat']   ].values:
            
            
            x = dataSet.sel({coordinate_names['lon']:lon, coordinate_names['lat'] :lat})
            
            
            # evaluating the Teleconnection map relative to Point x:
            
            r_correlation_map = get_correlation_for_x_pixel(x=x , 
                                                            dataArray=dataArray, 
                                                            dim=dim,
                                                            see_progressBar=see_progressBar)
            
            
            # getting teleconnections pathways around the globe:
            
            GS = get_teleconnection_line_path(r_correlation_map, 
                                              x, 
                                              variable=variable,
                                              coordinate_names =coordinate_names)
            
            if make_partial_plots['condition'] == True:
                
                fig, ax = plt.subplots()
                r_correlation_map[variable].plot(ax=ax, cmap='viridis')
                
                GS.plot(ax=ax, color='k', linestyle='--')
                
                fig.suptitle('Teleconnection Map')
                
                counter +=1
                
                
                if make_partial_plots['figure_base_path_save'] !=None:
                    
                    basename = os.path.basename(make_partial_plots['figure_base_path_save'])
                    
                    if not os.path.exists(basename):
                        os.mkdir(basename)
                        
                    else:
                        pass
                    to_save_filename = os.path.join(make_partial_plots['figure_base_path_save'], 
                                             'fig_{0}'.format(str(counter)))
                    
                    fig.savefig(to_save_filename, dpi=300)
                    
                fig.tight_layout()   
                plt.close(fig)
            
            
            # ensuring that only one value is returned from the function
            
            Teleconnection_value = np.abs(np.unique(r_correlation_map[variable].min().values))[0]
            
            if verbose:
            
                print(lon, lat, Teleconnection_value, '\n')
            
            
            x[variable] = Teleconnection_value
            
                        
            Teleconnection_Linepaths_gdf = Teleconnection_Linepaths_gdf.append(GS, ignore_index=True)
     
            dsx.append(x)
    
    return dsx, Teleconnection_Linepaths_gdf



def convert_gdf_to_netcdf(gdf):
    '''
    Function description:
        
        This function converts a gdf of Points into a netcdf.
        
        
    ------------------------------------------------------------------
    
    Parameters:
        
        a geopandas Geodataframe.
        
    
    ------------------------------------------------------------------
    
    Returns:
        
        a netcdf version of the geopandas
    
    '''
        
    gdf['lon'] = gdf.geometry.centroid.x 
    
    gdf['lat'] = gdf.geometry.centroid.y 

    Spatial_corr_netcdf = gdf['Correlation'].to_xarray().set_index(['lon', 'lat'])
    
    return Spatial_corr_netcdf





if '__main__' == __name__:
    ds = xr.tutorial.open_dataset('air_temperature').load()
    
    ds_month = ds.resample(time='M').mean('time')
    
    
    ds_chunked = ds_month.chunk({'lon': 1000, 'lat': 1000})
    
    ds_chunked['air2'] = (np.cos(ds_chunked['air']) * 50) + 10
    
    basename = r'C:\Users\lealp\Downloads\temp\imagens'
		
    make_plots = {'condition': False,
                  'figure_base_path_save':basename}
    
    Teleconnection_map, Teleconnection_Linepaths_gdf = get_correlation_for_each_pixel(ds_chunked, 
                                                                                      variable='air',
                                                                                      make_partial_plots=make_plots)
    
    