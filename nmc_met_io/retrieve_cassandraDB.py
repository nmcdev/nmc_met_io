# -*- coding: utf-8 -*-

# Copyright (c) 2022 longtsing.
# Distributed under the terms of the GPL V3 License.

"""
This is the retrieve module which get data from cassandra DB
with Python Cassandra driver API.

The API is same as the retrieve_micaps_cassandra

Change Log:
    - updated by 王清龙/湖北/宜昌, 2023/5/9, e-mail:songofsongs@vip.qq.com
    - Created by 王清龙/湖北/宜昌, 2022/8/26, e-mail:songofsongs@vip.qq.com
"""

import bz2
import gzip
import pickle
import re
import sys
import warnings
import zlib
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

import nmc_met_io.config as CONFIG
from nmc_met_io.read_radar import StandardData
from nmc_met_io.read_satellite import resolve_awx_bytearray
from nmc_met_io.retrieve_shared import (
    collect_model_3d_grid,
    collect_model_3d_grids,
    collect_model_grids,
    collect_station_dataset,
    collect_xarray_dataset,
    lzw_decompress,
    parse_model_grid_bytearray,
    parse_radar_mosaic_bytearray,
    parse_station_data_bytearray,
    parse_swan_radar_bytearray,
    parse_tlogp_bytearray,
)

try:
    # please install cassandra-driver first
    from cassandra.cluster import Cluster
except ImportError:
    print("cassandra-driver is not installed (pip install cassandra-driver)")
    sys.exit(1)


_db_client={
    "ClusterIPAddresses":CONFIG.CONFIG['Cassandra']['ClusterIPAddresses'].split(","),
    "gdsPort":CONFIG.CONFIG['Cassandra']['ClusterPort'],
    "cluster":Cluster(CONFIG.CONFIG['Cassandra']['ClusterIPAddresses'].split(",")),
}
_db_session=None
def _open_DB():
    global _db_session
    _db_session=_db_client["cluster"].connect()
    _db_session.set_keyspace(CONFIG.CONFIG['Cassandra']['KeySpace'])

class CassandraDB:

    def __init__(self):
        # set MICAPS Cassandra集群地址
        # 创建类自动建立与数据库连接，也需要手动关闭连接
        if(_db_session is None):
            _open_DB()
        self.session=_db_session

    def getLatestDataName(self, directory, filter):
        if(directory[-1]=="/"):
            directory=directory[:-1]
        response=None
        status=200
        try:
            rows=self.session.execute("select * from \"latestdatatime\" where \"dataPath\" = '"+directory+"' and \"column1\"='"+filter+"' limit 1")
            response=rows.one().value
        except Exception:
            status=100
            response=None

        return status,response

    def getData(self, directory, fileName):
        if(directory[-1]=="/"):
            directory=directory[:-1]
        table=directory.split("/")[0]
        key=directory.replace(table+"/","")
        response=None
        status=200
        try:
            rows=self.session.execute("select * from \""+table+"\" where \"dataPath\" = '"+key+"' and \"column1\"='"+fileName+"' limit 1")
            response=rows.one().value
        except Exception:
            status=100
            response=None

        return status,response


    def getFileList(self,directory):
        if(directory[-1]=="/"):
            directory=directory[:-1]
        response=None
        status=None
        try:
            rows=self.session.execute("select * from \"treeview\" where \"dataPath\" = '"+directory+"' ")
            response=list(map(lambda x:x.column1,rows.all()))
            status=200
        except Exception:
            status=100
            response=None

        return status,response
    

def get_file_list(path, latest=None):
    """return file list of cassandra data servere path
    
    Args:
        path (string): cassandra data servere path.
        latest (integer): get the latest n files.
    
    Returns:
        list: list of filenames.
    """

    # connect to data service
    service = CassandraDB()

    # 获得指定目录下的所有文件
    status, response = service.getFileList(path)
    file_list = []
    if status == 200:
        if response is not None:
            file_list=response

    # sort the file list
    if latest is not None:
        file_list.sort(reverse=True)
        file_list = file_list[0:min(len(file_list), latest)]

    return file_list


def get_latest_initTime(directory, suffix="*.006"):
    """
    Get the latest initial time string.
    
    Args:
        directory (string): the data directory on the service.
        suffix (string, optional):  the filename filter pattern.

    Examples:
    >>> initTime = get_latest_initTime("ECMWF_HR/TMP/850")
    """

    # connect to data service
    service = CassandraDB()

    # get lastest data filename
    try:
        status, response = service.getLatestDataName(directory, suffix)
    except ValueError:
        print('Can not retrieve data from ' + directory)
        return None
    if status == 200:     # Standard response for successful HTTP requests
        if response is not None:
            filename = response
            if filename == '':
                return None
            else:
                return filename.split('.')[0]
        else:
            return None
    else:
        return None


def get_model_grid(directory, filename=None, suffix="*.024",
                   varname='data', varattrs={'units':''}, scale_off=None,
                   levattrs={'long_name':'pressure_level', 'units':'hPa',
                             '_CoordinateAxisType':'Pressure'}, 
                   cache=True, cache_clear=True, check_file_first=True):
    """
    Retrieve numeric model grid forecast from MICAPS cassandra service.
    Support ensemble member forecast.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param varname: set variable name.
    :param varattrs: set variable attributes, dictionary type.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param levattrs: set level coordinate attributes, diectionary type.
    :param cache: cache retrieved data to local directory, default is True.
    :param check_file_first: check file exists firstly. Default is True.
    :return: data, xarray type

    :Examples:
    >>> data = get_model_grid("ECMWF_HR/TMP/850")
    >>> data_ens = get_model_grid("ECMWF_ENSEMBLE/RAW/HGT/500", filename='18021708.024')
    >>> data_ens = get_model_grid('ECMWF_ENSEMBLE/RAW/TMP_2M', '19083008.024')
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
                check_file_first = False     # file existed
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(
            directory, 
            filename, 
            name="MICAPS_DATA", 
            cache_clear=cache_clear
        )
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data
    
    # get data contents
    try:
        # get the file list and check file exists
        if check_file_first:
            file_list = get_file_list(directory)
            if filename not in file_list:
                return None
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200:
        if response is not None:
            byteArray = gzip.decompress(response)
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            data = parse_model_grid_bytearray(
                byteArray,
                varname=varname,
                varattrs=varattrs,
                scale_off=scale_off,
                levattrs=levattrs,
                origin='MICAPS Cassandra DB',
            )

            # cache data
            if cache:
                with open(cache_file, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

            # return data
            return data

        else:
            return None
    else:
        return None


def get_model_grids(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple time grids from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_model_grid function.
    """

    return collect_model_grids(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_file_list_func=get_file_list,
        get_model_grid_func=get_model_grid,
        **kargs
    )


def get_model_points(directory, filenames, points, **kargs):
    """
    Retrieve point time series from MICAPS cassandra service.
    Return xarray, (time, points)
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        points (dict): dictionary, {'lon':[...], 'lat':[...]}.
        **kargs: key arguments passed to get_model_grids function.

    Examples:
    >>> directory = "NWFD_SCMOC/TMP/2M_ABOVE_GROUND"
    >>> fhours = np.arange(3, 75, 3)
    >>> filenames = ["19083008."+str(fhour).zfill(3) for fhour in fhours]
    >>> points = {'lon':[116.3833, 110.0], 'lat':[39.9, 32]}
    >>> data = get_model_points(dataDir, filenames, points)
    """

    data = get_model_grids(directory, filenames, **kargs)
    if data is not None:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    return None


def get_model_3D_grid(directory, filename, levels, allExists=True, pbar=False, **kargs):
    """
    Retrieve 3D [level, lat, lon] grids from  MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service, which includes all levels.
        filename (string): the data file name.
        levels (list): the high levels.
        allExists (boolean): all levels should be exist, if not, return None.
        pbar (boolean): show progress bar.
        **kargs: key arguments passed to get_model_grid function.

    Examples:
    >>> directory = "ECMWF_HR/TMP"
    >>> levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    >>> filename = "19083008.024"
    >>> data = get_model_3D_grid(directory, filename, levels)
    """

    return collect_model_3d_grid(
        directory,
        filename,
        levels,
        all_exists=allExists,
        pbar=pbar,
        get_model_grid_func=get_model_grid,
        **kargs
    )


def get_model_3D_grids(directory, filenames, levels, allExists=True, pbar=True, **kargs):
    """
     Retrieve 3D [time, level, lat, lon] grids from  MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service, which includes all levels.
        filenames (list): the list of data filenames, should be the same initial time.
        levels (list): the high levels.
        allExists (bool, optional): all files should exist, or return None.. Defaults to True.
        pbar (boolean): Show progress bar, default to True.
        **kargs: key arguments passed to get_model_grid function.

    Examples:
    >>> directory = "ECMWF_HR/TMP"
    >>> levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    >>> fhours = np.arange(0, 75, 3)
    >>> filenames = ["19083008."+str(fhour).zfill(3) for fhour in fhours]
    >>> data =  get_model_3D_grids(directory, filenames, levels)
    """

    return collect_model_3d_grids(
        directory,
        filenames,
        levels,
        all_exists=allExists,
        pbar=pbar,
        get_model_grid_func=get_model_grid,
        **kargs
    )


def get_model_profiles(directory, filenames, levels, points, **kargs):
    """
    Retrieve time series of vertical profile from 3D [time, level, lat, lon] grids from  MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service, which includes all levels.
        filenames (list): the list of data filenames or one file.
        levels (list): the high levels.
        points (dict): dictionary, {'lon':[...], 'lat':[...]}.
        **kargs: key arguments passed to get_model_3D_grids function.

    Examples:
      directory = "ECMWF_HR/TMP"
      levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
      filenames = ["20021320.024"]
      points = {'lon':[116.3833, 110.0], 'lat':[39.9, 32]}
      data = get_model_profiles(directory, filenames, levels, points)
    """

    data = get_model_3D_grids(directory, filenames, levels, **kargs)
    if data is not None:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    return None


def get_station_data(directory, filename=None, suffix="*.000",
                     dropna=True, cache=True, cache_clear=True):
    """
    Retrieve station data from MICAPS cassandra service.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will
                   be used to find the specified file.
    :param dropna: the column which values is all na will be dropped.
    :param limit: subset station data in the limit [lon0, lon1, lat0, lat1]
    :param cache: cache retrieved data to local directory, default is True.
    :return: pandas DataFrame.

    :example:
    >>> data = get_station_data("SURFACE/PLOT_10MIN")
    >>> data = get_station_data("SURFACE/TMP_MAX_24H_NATIONAL", filename="20190705150000.000")
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(
            directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # get data contents
    try:
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200:
        if response is not None:
            byteArray = gzip.decompress(response)
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            records = parse_station_data_bytearray(byteArray, dropna=dropna)

            # cache records
            if cache:
                with open(cache_file, 'wb') as f:
                    pickle.dump(records, f, protocol=pickle.HIGHEST_PROTOCOL)

            # return
            return records
        else:
            return None
    else:
        return None


def get_station_dataset(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple station observation from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_fy_awx function.
    """

    return collect_station_dataset(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_station_data_func=get_station_data,
        **kargs
    )


def get_fy_awx(directory, filename=None, suffix="*.AWX", units='', cache=True, cache_clear=True):
    """
    Retrieve FY satellite cloud awx format file.
    The awx file format is refered to “气象卫星分发产品及其格式规范AWX2.1”
    http://satellite.nsmc.org.cn/PortalSite/StaticContent/DocumentDownload.aspx?TypeID=10

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param units: data units, default is ''.
    :param cache: cache retrieved data to local directory, default is True.
    :return: satellite information and data.

    :Examples:
    >>> directory = "SATELLITE/FY4A/L1/CHINA/C004"
    >>> data = get_fy_awx(directory)
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # get data contents
    try:
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200 and response is not None:
        byteArray = gzip.decompress(response)
        if byteArray == '':
            print('There is no data ' + filename + ' in ' + directory)
            return None

        
        data = resolve_awx_bytearray(byteArray, units)

        # cache data
        if data is not None and cache:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

        # return
        return data
    else:
        return None


def get_fy_awxs(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple satellite images from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_fy_awx function.
    """

    return collect_xarray_dataset(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_data_func=get_fy_awx,
        concat_dim='time',
        **kargs
    )


def _lzw_decompress(compressed):
    """Decompress a list of output ks to a string.
    refer to https://stackoverflow.com/questions/6834388/basic-lzw-compression-help-in-python.
    """

    return lzw_decompress(compressed)


def get_radar_mosaic(directory, filename=None, suffix="*.BIN", cache=True, cache_clear=True):
    """
    该程序主要用于读取和处理中国气象局CRaMS系统的雷达回波全国拼图数据.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param cache: cache retrieved data to local directory, default is True.
    :return: xarray object.

    :Example:
    >>> data = get_radar_mosaic("RADARMOSAIC/CREF/")
    >>> dir_dir = "RADARMOSAIC/CREF/"
    >>> filename = "ACHN_CREF_20210413_005000.BIN"
    >>> CREF = get_radar_mosaic(dir_dir, filename=filename, cache=False)
    >>> print(CREF['time'].values)
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # get data contents
    try:
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200:
        if response is not None:
            byteArray = gzip.decompress(response)
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            data = parse_radar_mosaic_bytearray(
                byteArray, origin='MICAPS Cassandra DB'
            )
            if data is None:
                print('Can not decompress data.')
                return None

            # cache data
            if cache:
                with open(cache_file, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

            # return
            return data
        else:
            return None
    else:
        return None


def get_radar_mosaics(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple radar mosaics from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_fy_awx function.
    """

    return collect_xarray_dataset(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_data_func=get_radar_mosaic,
        concat_dim='time',
        **kargs
    )


def get_tlogp(directory, filename=None, suffix="*.000",
              remove_duplicate=False, remove_na=False,
              cache=False, cache_clear=True):
    """
    该程序用于读取micaps服务器上TLOGP数据信息, 文件格式与MICAPS第5类格式相同.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param remove_duplicate: boolean, the duplicate records will be removed.
    :param remove_na: boolean, the na records will be removed.
    :param cache: cache retrieved data to local directory, default is True.
    :return: pandas DataFrame object.

    >>> data = get_tlogp("UPPER_AIR/TLOGP/")
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                records = pickle.load(f)
                return records

    # get data contents
    try:
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200:
        if response is not None:
            byteArray = gzip.decompress(response)
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            records = parse_tlogp_bytearray(
                byteArray,
                remove_duplicate=remove_duplicate,
                remove_na=remove_na
            )
            if records is None:
                return None

            # cache data
            if cache:
                with open(cache_file, 'wb') as f:
                    pickle.dump(records, f, protocol=pickle.HIGHEST_PROTOCOL)

            # return
            return records
        else:
            return None
    else:
        return None


def get_tlogps(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple tlog observation from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_fy_awx function.
    """

    return collect_station_dataset(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_station_data_func=get_tlogp,
        **kargs
    )


def get_swan_radar(directory, filename=None, suffix="*.000", scale=[0.1, 0], 
                   varattrs={'long_name': 'quantitative_precipitation_forecast', 'short_name': 'QPF', 'units': 'mm'},
                   cache=True, cache_clear=True, attach_forecast_period=True):
    """
    该程序用于读取micaps服务器上SWAN的D131格点数据格式.
    refer to https://www.taodocs.com/p-274692126.html

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param scale: data value will be scaled = (data + scale[1]) * scale[0], normally,
                  CREF, CAPPI: [0.5, -66]
                  radar echo height, VIL, OHP, ...: [0.1, 0]
    :param varattrs: dictionary, variable attributes.
    :param cache: cache retrieved data to local directory, default is True.
    :return: pandas DataFrame object.

    >>> data = get_swan_radar("RADARMOSAIC/EXTRAPOLATION/QPF/")
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    if cache:
        cache_file = CONFIG.get_cache_file(directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # get data contents
    try:
        service = CassandraDB()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    if status == 200:
        if response is not None:
            byteArray = gzip.decompress(response)
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None
            if filename.endswith(('.BZ2','.bz2')):
                byteArray = bz2.decompress(byteArray)
            data = parse_swan_radar_bytearray(
                byteArray,
                filename=filename,
                scale=scale,
                varattrs=varattrs,
                attach_forecast_period=attach_forecast_period,
                origin='MICAPS Cassandra DB'
            )

            # cache data
            if cache:
                with open(cache_file, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

            # return
            return data
        else:
            return None
    else:
        return None


def get_swan_radars(directory, filenames, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple swan 131 radar from MICAPS cassandra service.
    
    Args:
        directory (string): the data directory on the service.
        filenames (list): the list of filenames.
        allExists (boolean): all files should exist, or return None.
        pbar (boolean): Show progress bar, default to False.
        **kargs: key arguments passed to get_fy_awx function.
    """

    return collect_xarray_dataset(
        directory,
        filenames,
        all_exists=allExists,
        pbar=pbar,
        get_data_func=get_swan_radar,
        concat_dim='time',
        **kargs
    )


def get_radar_standard(directory, filename=None, suffix="*.BZ2", cache=True, cache_clear=True):
    """
    该程序用于读取Micaps服务器上的单站雷达基数据, 该数据为
    "天气雷达基数据标准格式(V1.0版)", 返回数据类型为PyCINRAD的标准雷达数据类.
    refer to: https://github.com/CyanideCN/PyCINRAD
    
    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :param cache: cache retrieved data to local directory, default is True.
    :return: PyCINRAD StandardData object.

    :Examples:
    >>> import pyart
    >>> from nmc_met_io.retrieve_micaps_server import get_radar_standard
    >>> from nmc_met_io.export_radar import standard_data_to_pyart
    >>> data = get_radar_standard('SINGLERADAR/ARCHIVES/PRE_QC/武汉/')
    >>> radar = standard_data_to_pyart(data)
    >>> 
    """

    # get data file name
    if filename is None:
        try:
            # connect to data service
            service = CassandraDB()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        if status == 200:
            if response is not None:
                filename = response
                if filename == '':
                    return None
            else:
                return None

    # retrieve data from cached file
    byteArray = None
    if cache:
        cache_file = CONFIG.get_cache_file(directory, filename, name="MICAPS_DATA", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                byteArray = pickle.load(f) 

    if byteArray is None:
        # get data contents
        try:
            service = CassandraDB()
            status, response = service.getData(directory, filename)
        except ValueError:
            print('Can not retrieve data' + filename + ' from ' + directory)
            return None
        if status == 200:
            if response is not None:
                byteArray = gzip.decompress(response)
                if byteArray == '':
                    print('There is no data ' + filename + ' in ' + directory)
                    return None
        else:
            return None

    # read radar data
    file = BytesIO(bz2.decompress(byteArray))
    data = StandardData(file)
    file.close()

    # cache data
    if cache:
        with open(cache_file, 'wb') as f:
            pickle.dump(byteArray, f, protocol=pickle.HIGHEST_PROTOCOL)

    # return
    return data
