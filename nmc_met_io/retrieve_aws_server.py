# _*_ coding: utf-8 _*_

# Copyright (c) 2020 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve Open data on AWS (从亚马逊云开源数据服务上获取数据).
refer to https://registry.opendata.aws/
"""

import os
import sys
import datetime
import numpy as np
import xarray as xr

try:
    # s3fs基于botocore, 提供Python的类似文件访问方法, 用于访问AWS的S3文件系统.
    import s3fs
except ImportError:
    print("s3fs not installed (conda install -c conda-forge s3fs)")
    sys.exit(1)


def retrieve_era5(varname, year, month, datestart, dateend):
    """
    Retrieve ERA5 Reanalysis from https://registry.opendata.aws/ecmwf-era5/.
    Open zarr file and get data in time range (datestart, dateend)

    Args:
        varname (str): variable names, like:
                       air_pressure_at_mean_sea_level
                       air_temperature_at_2_metres
                       air_temperature_at_2_metres_1hour_Maximum
                       air_temperature_at_2_metres_1hour_Minimum
                       dew_point_temperature_at_2_metres
                       eastward_wind_at_100_metres
                       eastward_wind_at_10_metres
                       integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation
                       lwe_thickness_of_surface_snow_amount
                       northward_wind_at_100_metres
                       northward_wind_at_10_metres
                       precipitation_amount_1hour_Accumulation
                       sea_surface_temperature
                       snow_density
                       surface_air_pressure
        year (int): year
        month (int): month
        datestart (str): start date, like '1987-12-02'
        dateend (str): end date, like '1987-12-02 23:59'
    
    Examples:
        year = 1987
        month = 12
        datestart = '1987-12-02'
        dateend = '1987-12-02 23:59'
        varname = 'air_temperature_at_2_metres'
        data = retrieve_era5(varname, year, month, datestart, dateend)
        print(data)
    """

    # Access S3 file system with anonymous.
    fs = s3fs.S3FileSystem(anon=True)
    
    # construct data file
    datestring = 'era5-pds/zarr/{year}/{month:02d}/data/'.format(year=year, month=month)
    datafile = datestring + varname + '.zarr/'

    # open zarr file
    data = xr.open_zarr(s3fs.S3Map(datafile, s3=fs))
    if varname in ['precipitation_amount_1hour_Accumulation']:
        data.sel(time1=slice(np.datetime64(datestart), np.datetime64(dateend)))
    else:
        data.sel(time0=slice(np.datetime64(datestart), np.datetime64(dateend)))

    return data

