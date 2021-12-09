# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
This is the retrieve module which get data from MICAPS cassandra service
with Python API.

Checking url, like:
http://10.32.8.164:8080/DataService?requestType=getLatestDataName&directory=ECMWF_HR/TMP/850&fileName=&filter=*.024
"""

import warnings
import re
import http.client
import urllib.parse
import pickle
import bz2
import zlib
from io import BytesIO
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import pandas as pd
from tqdm import tqdm
from nmc_met_io import DataBlock_pb2
import nmc_met_io.config as CONFIG
from nmc_met_io.read_radar import StandardData


def get_http_result(host, port, url):
    """
    Get the http contents.
    """

    http_client = None
    try:
        http_client = http.client.HTTPConnection(host, port, timeout=120)
        http_client.request('GET', url)
        response = http_client.getresponse()
        return response.status, response.read()
    except Exception as e:
        print(e)
        return 0,
    finally:
        if http_client:
            http_client.close()


class GDSDataService:
    def __init__(self):
        # set MICAPS GDS服务器地址
        self.gdsIp = CONFIG.CONFIG['MICAPS']['GDS_IP']
        self.gdsPort = CONFIG.CONFIG['MICAPS']['GDS_PORT']

    def getLatestDataName(self, directory, filter):
        return get_http_result(
            self.gdsIp, self.gdsPort, "/DataService" +
            self.get_concate_url("getLatestDataName", directory, "", filter))

    def getData(self, directory, fileName):
        return get_http_result(
            self.gdsIp, self.gdsPort, "/DataService" +
            self.get_concate_url("getData", directory, fileName, ""))

    def getFileList(self,directory):
        return get_http_result(
            self.gdsIp, self.gdsPort, "/DataService" + 
            self.get_concate_url("getFileList", directory, "",""))

    # 将请求参数拼接到url
    def get_concate_url(self, requestType, directory, fileName, filter):
        url = ""
        url += "?requestType=" + requestType
        url += "&directory=" + directory
        url += "&fileName=" + fileName
        url += "&filter=" + filter
        return urllib.parse.quote(url, safe=':/?=&')


def get_file_list(path, latest=None):
    """return file list of cassandra data servere path
    
    Args:
        path (string): cassandra data servere path.
        latest (integer): get the latest n files.
    
    Returns:
        list: list of filenames.
    """

    # connect to data service
    service = GDSDataService()

    # 获得指定目录下的所有文件
    status, response = service.getFileList(path)
    MappingResult = DataBlock_pb2.MapResult()
    file_list = []
    if status == 200:
        if MappingResult is not None:
            # Protobuf的解析
            MappingResult.ParseFromString(response)
            results = MappingResult.resultMap
            # 遍历指定目录
            for name_size_pair in results.items():
                if (name_size_pair[1] != 'D'):
                    file_list.append(name_size_pair[0])

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
    service = GDSDataService()

    # get lastest data filename
    try:
        status, response = service.getLatestDataName(directory, suffix)
    except ValueError:
        print('Can not retrieve data from ' + directory)
        return None
    StringResult = DataBlock_pb2.StringResult()
    if status == 200:     # Standard response for successful HTTP requests
        StringResult.ParseFromString(response)
        if StringResult is not None:
            filename = StringResult.name
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
                             '_CoordinateAxisType':'Pressure'}, cache=True, cache_clear=True):
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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        file_list = get_file_list(directory)
        if filename not in file_list:
            return None
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult.errorCode == 1:
            return None
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray
            if byteArray == '':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            # define head information structure (278 bytes)
            head_dtype = [('discriminator', 'S4'), ('type', 'i2'),
                          ('modelName', 'S20'), ('element', 'S50'),
                          ('description', 'S30'), ('level', 'f4'),
                          ('year', 'i4'), ('month', 'i4'), ('day', 'i4'),
                          ('hour', 'i4'), ('timezone', 'i4'),
                          ('period', 'i4'), ('startLongitude', 'f4'),
                          ('endLongitude', 'f4'), ('longitudeGridSpace', 'f4'),
                          ('longitudeGridNumber', 'i4'),
                          ('startLatitude', 'f4'), ('endLatitude', 'f4'),
                          ('latitudeGridSpace', 'f4'),
                          ('latitudeGridNumber', 'i4'),
                          ('isolineStartValue', 'f4'),
                          ('isolineEndValue', 'f4'),
                          ('isolineSpace', 'f4'),
                          ('perturbationNumber', 'i2'),
                          ('ensembleTotalNumber', 'i2'),
                          ('minute', 'i2'), ('second', 'i2'),
                          ('Extent', 'S92')]

            # read head information
            head_info = np.frombuffer(byteArray[0:278], dtype=head_dtype)

            # get required grid information
            data_type = head_info['type'][0]
            nlon = head_info['longitudeGridNumber'][0]
            nlat = head_info['latitudeGridNumber'][0]
            nmem = head_info['ensembleTotalNumber'][0]

            # define data structure
            if data_type == 4:
                data_dtype = [('data', 'f4', (nlat, nlon))]
                data_len = nlat * nlon * 4
            elif data_type == 11:
                data_dtype = [('data', 'f4', (2, nlat, nlon))]
                data_len = 2 * nlat * nlon * 4
            else:
                raise Exception("Data type is not supported")

            # read data
            if nmem == 0:
                data = np.frombuffer(byteArray[278:], dtype=data_dtype)
                data = np.squeeze(data['data'])
            else:
                if data_type == 4:
                    data = np.full((nmem, nlat, nlon), np.nan)
                else:
                    data = np.full((nmem, 2, nlat, nlon), np.nan)
                ind = 0
                for _ in range(nmem):
                    head_info_mem = np.frombuffer(
                        byteArray[ind:(ind+278)], dtype=head_dtype)
                    ind += 278
                    data_mem = np.frombuffer(
                        byteArray[ind:(ind+data_len)], dtype=data_dtype)
                    ind += data_len
                    number = head_info_mem['perturbationNumber'][0]
                    if data_type == 4:
                        data[number, :, :] = np.squeeze(data_mem['data'])
                    else:
                        data[number, :, :, :] = np.squeeze(data_mem['data'])

            # scale and offset the data, if necessary.
            if scale_off is not None:
                data = data * scale_off[0] + scale_off[1]

            # construct longitude and latitude coordinates
            slon = head_info['startLongitude'][0]
            dlon = head_info['longitudeGridSpace'][0]
            slat = head_info['startLatitude'][0]
            dlat = head_info['latitudeGridSpace'][0]
            lon = np.arange(nlon) * dlon + slon
            lat = np.arange(nlat) * dlat + slat
            level = np.array([head_info['level'][0]])

            # construct initial time and forecast hour
            init_time = datetime(head_info['year'][0], head_info['month'][0],
                                 head_info['day'][0], head_info['hour'][0])
            fhour = np.array([head_info['period'][0]], dtype=np.float)
            time = init_time + timedelta(hours=fhour[0])
            init_time = np.array([init_time], dtype='datetime64[ms]')
            time = np.array([time], dtype='datetime64[ms]')

            # define coordinates
            time_coord = ('time', time)
            lon_coord = ('lon', lon, {
                'long_name':'longitude', 'units':'degrees_east',
                '_CoordinateAxisType':'Lon', "axis": "X"})
            lat_coord = ('lat', lat, {
                'long_name':'latitude', 'units':'degrees_north',
                '_CoordinateAxisType':'Lat', 'axis': "Y"})
            if level[0] != 0:
                level_coord = ('level', level, levattrs)
            if nmem != 0:
                number = np.arange(nmem)
                number_coord = ('number', number, {'_CoordinateAxisType':'Ensemble'})

            # create to xarray
            if data_type == 4:
                if nmem == 0:
                    if level[0] == 0:
                        data = data[np.newaxis, ...]
                        data = xr.Dataset({
                            varname:(['time', 'lat', 'lon'], data, varattrs)},
                            coords={
                                'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
                    else:
                        data = data[np.newaxis, np.newaxis, ...]
                        data = xr.Dataset({
                            varname:(['time', 'level', 'lat', 'lon'], data, varattrs)},
                            coords={
                                'time':time_coord, 'level':level_coord, 
                                'lat':lat_coord, 'lon':lon_coord})
                else:
                    if level[0] == 0:
                        data = data[:, np.newaxis, ...]
                        data = xr.Dataset({
                            varname:(['number', 'time', 'lat', 'lon'], data, varattrs)},
                            coords={
                                'number':number_coord, 'time':time_coord,
                                'lat':lat_coord, 'lon':lon_coord})
                    else:
                        data = data[:, np.newaxis, np.newaxis, ...]
                        data = xr.Dataset({
                            varname:(['number', 'time', 'level', 'lat', 'lon'], data, varattrs)},
                            coords={
                                'number':number_coord, 'time':time_coord, 'level':level_coord, 
                                'lat':lat_coord, 'lon':lon_coord})
            elif data_type == 11:
                speedattrs = {'long_name':'wind speed', 'units':'m/s'}
                angleattrs = {'long_name':'wind angle', 'units':'degree'}
                if nmem == 0:
                    speed = np.squeeze(data[0, :, :])
                    angle = np.squeeze(data[1, :, :])
                    # 原始数据文件中存储为: 西风为0度，南风为90度，东风为180度，北风为270度
                    # 修改为气象风向常规定义: 北方为0度, 东风为90度, 南风为180度, 西方为270度
                    angle = 270. - angle
                    angle[angle<0] = angle[angle<0] + 360.
                    if level[0] == 0:
                        speed = speed[np.newaxis, ...]
                        angle = angle[np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (['time', 'lat', 'lon'], speed, speedattrs),
                            'angle': (['time', 'lat', 'lon'], angle, angleattrs)},
                            coords={'lon': lon_coord, 'lat': lat_coord, 'time': time_coord})
                    else:
                        speed = speed[np.newaxis, np.newaxis, ...]
                        angle = angle[np.newaxis, np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (['time', 'level', 'lat', 'lon'], speed, speedattrs),
                            'angle': (['time', 'level', 'lat', 'lon'], angle, angleattrs)},
                            coords={'lon': lon_coord, 'lat': lat_coord, 
                                    'level': level_coord, 'time': time_coord})
                else:
                    speed = np.squeeze(data[0, :, :, :])
                    angle = np.squeeze(data[1, :, :, :])
                    # 原始数据文件中存储为: 西风为0度，南风为90度，东风为180度，北风为270度
                    # 修改为气象风向常规定义: 北方为0度, 东风为90度, 南风为180度, 西方为270度
                    angle = 270. - angle
                    angle[angle<0] = angle[angle<0] + 360.
                    if level[0] == 0:
                        speed = speed[:, np.newaxis, ...]
                        angle = angle[:, np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (
                                ['number', 'time', 'lat', 'lon'], speed, speedattrs),
                            'angle': (
                                ['number', 'time', 'lat', 'lon'], angle, angleattrs)},
                            coords={
                                'lon': lon_coord, 'lat': lat_coord,
                                'number': number_coord, 'time': time_coord})
                    else:
                        speed = speed[:, np.newaxis, np.newaxis, ...]
                        angle = angle[:, np.newaxis, np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (
                                ['number', 'time', 'level', 'lat', 'lon'],
                                speed, speedattrs),
                            'angle': (
                                ['number', 'time', 'level', 'lat', 'lon'],
                                angle, angleattrs)},
                            coords={
                                'lon': lon_coord, 'lat': lat_coord, 'level': level_coord,
                                'number': number_coord, 'time': time_coord})
            
            # add time coordinates
            data.coords['forecast_reference_time'] = init_time[0]
            data.coords['forecast_period'] = ('time', fhour, {
                'long_name':'forecast_period', 'units':'hour'})

            # add attributes
            data.attrs['Conventions'] = "CF-1.6"
            data.attrs['Origin'] = 'MICAPS Cassandra Server'

            # sort latitude coordinates
            data = data.loc[{'lat':sorted(data.coords['lat'].values)}]

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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_model_grid(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return xr.concat(dataset, dim='time')


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
    if data:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    else:
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

    dataset = []
    if pbar:
        tqdm_levels = tqdm(levels, desc=directory+": ")
    else:
        tqdm_levels = levels
    for level in tqdm_levels:
        if directory[-1] == '/':
            dataDir = directory + str(int(level)).strip()
        else:
            dataDir = directory + '/' + str(int(level)).strip()
        data = get_model_grid(dataDir, filename=filename, **kargs)
        if data:
                dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(dataDir+'/'+filename))
                return None

    return xr.concat(dataset, dim='level')


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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory+": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        dataset_temp = []
        for level in levels:
            if directory[-1] == '/':
                dataDir = directory + str(int(level)).strip()
            else:
                dataDir = directory + '/' + str(int(level)).strip()
            data = get_model_grid(dataDir, filename=filename, **kargs)
            if data:
                    dataset_temp.append(data)
            else:
                if allExists:
                    warnings.warn("{} doese not exists.".format(dataDir+'/'+filename))
                    return None
        dataset.append(xr.concat(dataset_temp, dim='level'))
    
    return xr.concat(dataset, dim='time')


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
    if data:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    else:
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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray

            # define head structure
            head_dtype = [('discriminator', 'S4'), ('type', 'i2'),
                          ('description', 'S100'),
                          ('level', 'f4'), ('levelDescription', 'S50'),
                          ('year', 'i4'), ('month', 'i4'), ('day', 'i4'),
                          ('hour', 'i4'), ('minute', 'i4'), ('second', 'i4'),
                          ('Timezone', 'i4'), ('extent', 'S100')]

            # read head information
            head_info = np.frombuffer(byteArray[0:288], dtype=head_dtype)
            ind = 288

            # read the number of stations
            station_number = np.frombuffer(
                byteArray[ind:(ind+4)], dtype='i4')[0]
            ind += 4

            # read the number of elements
            element_number = np.frombuffer(
                byteArray[ind:(ind+2)], dtype='i2')[0]
            ind += 2

            # construct record structure
            element_type_map = {
                1: 'b1', 2: 'i2', 3: 'i4', 4: 'i8', 5: 'f4', 6: 'f8', 7: 'S'}
            element_map = {}
            for i in range(element_number):
                element_id = str(
                    np.frombuffer(byteArray[ind:(ind+2)], dtype='i2')[0])
                ind += 2
                element_type = np.frombuffer(
                    byteArray[ind:(ind+2)], dtype='i2')[0]
                ind += 2
                element_map[element_id] = element_type_map[element_type]

            # loop every station to retrieve record
            record_head_dtype = [
                ('ID', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('numb', 'i2')]
            records = []
            for i in range(station_number):
                record_head = np.frombuffer(
                    byteArray[ind:(ind+14)], dtype=record_head_dtype)
                ind += 14
                record = {
                    'ID': record_head['ID'][0], 'lon': record_head['lon'][0],
                    'lat': record_head['lat'][0]}
                for j in range(record_head['numb'][0]):    # the record element number is not same, missing value is not included.
                    element_id = str(
                        np.frombuffer(byteArray[ind:(ind + 2)], dtype='i2')[0])
                    ind += 2
                    element_type = element_map[element_id]
                    if element_type == 'S':                # if the element type is string, we need get the length of string
                        str_len = np.frombuffer(byteArray[ind:(ind + 2)], dtype='i2')[0]
                        ind += 2
                        element_type = element_type + str(str_len)
                    element_len = int(element_type[1:])
                    record[element_id] = np.frombuffer(
                        byteArray[ind:(ind + element_len)],
                        dtype=element_type)[0]
                    ind += element_len
                records += [record]

            # convert to pandas data frame
            records = pd.DataFrame(records)
            records.set_index('ID')

            # get time
            time = datetime(
                head_info['year'][0], head_info['month'][0],
                head_info['day'][0], head_info['hour'][0],
                head_info['minute'][0], head_info['second'][0])
            records['time'] = time

            # change column name for common observation
            records.rename(columns={'3': 'Alt', '4': 'Grade', '5': 'Type', '21': 'Name',
                '201': 'Wind_angle', '203': 'Wind_speed', '205': 'Wind_angle_1m_avg', '207': 'Wind_speed_1m_avg',
                '209': 'Wind_angle_2m_avg', '211': 'Wind_speed_2m_avg', '213': 'Wind_angle_10m_avg', '215': 'Wind_speed_10m_avg',
                '217': 'Wind_angle_max', '219': 'Wind_speed_max', '221': 'Wind_angle_instant', '223': 'Wind_speed_instant',
                '225': 'Gust_angle', '227': 'Gust_speed', '229': 'Gust_angle_6h', '231': 'Gust_speed_6h',
                '233': 'Gust_angle_12h', '235': 'Gust_speed_12h', '237': 'Wind_power', 
                '401': 'Sea_level_pressure', '403': 'Pressure_3h_trend', '405': 'Pressure_24h_trend',
                '407': 'Station_pressure', '409': 'Pressure_max', '411': 'Pressure_min', '413': 'Pressure',
                '415': 'Pressure_day_avg', '417': 'SLP_day_avg', '419': 'Hight', '421': 'Geopotential_hight',
                '601': 'Temp', '603': 'Temp_max', '605': 'Temp_min', '607': 'Temp_24h_trend', 
                '609': 'Temp_24h_max', '611':'Temp_24h_min', '613': 'Temp_dav_avg',
                '801': 'Dewpoint', '803': 'Dewpoint_depression', '805': 'Relative_humidity',
                '807': 'Relative_humidity_min', '809': 'Relative_humidity_day_avg', 
                '811': 'Water_vapor_pressure', '813': 'Water_vapor_pressure_day_avg',
                '1001': 'Rain', '1003': 'Rain_1h', '1005': 'Rain_3h', '1007': 'Rain_6h', 
                '1009': 'Rain_12h', '1011': 'Rain_24h', '1013': 'Rain_day',
                '1015': 'Rain_20-08', '1017': 'Rain_08-20', '1019': 'Rain_20-20', '1021': 'Rain_08-08',
                '1023': 'Evaporation', '1025': 'Evaporation_large', '1027': 'Precipitable_water',
                '1201': 'Vis_1min', '1203': 'Vis_10min', '1205': 'Vis_min', '1207': 'Vis_manual',
                '1401': 'Total_cloud_cover', '1403': 'Low_cloud_cover', '1405': 'Cloud_base_hight',
                '1407': 'Low_cloud', '1409': 'Middle_cloud', '1411': 'High_cloud',
                '1413': 'TCC_day_avg', '1415': 'LCC_day_avg', '1417': 'Cloud_cover', '1419': 'Cloud_type',
                '1601': 'Weather_current', '1603': 'Weather_past_1', '1605': 'Weather_past_2',
                '2001': 'Surface_temp', '2003': 'Surface_temp_max', '2005': 'Surface_temp_min'},
                inplace=True)

            # drop all NaN columns
            if dropna:
                records = records.dropna(axis=1, how='all')

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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_station_data(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return pd.concat(dataset)


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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray
            if byteArray == b'':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            # the first class file head  一级文件头记录采用定长方式, 共40字节
            head1_dtype = [
                ('SAT96', 'S12'),                    # SAT96 filename
                ('byteSequence', 'i2'),              # 整型数的字节顺序, 0 低字节在前, 高字节在后; !=0 高字节在前, 低字节在后.
                ('firstClassHeadLength', 'i2'),      # 第一节文件头长度
                ('secondClassHeadLength', 'i2'),     # 第二节文件头长度
                ('padDataLength', 'i2'),             # 填充段数据长度
                ('recordLength', 'i2'),              # 记录长度(字节), 图像产品: 记录长度=图形宽度, 格点场产品: 记录长度=横向格点数x格点数据字长
                ('headRecordNumber', 'i2'),          # 文件头占用记录数, 一级文件头、二填充段扩展以及的所占用总记录个数
                ('dataRecordNumber', 'i2'),          # 产品数据占用记录数
                ('productCategory', 'i2'),           # 产品类别, 1：静止, 2：极轨, 3：格点定量, 4：离散, 5: 图形和分析
                ('compressMethod', 'i2'),            # 压缩方式, 0: 未压缩; 1 行程编码压缩; 2 LZW方式压缩; 3 特点方式压缩
                ('formatString', 'S8'),              # 格式说明字符串, 'SAT2004'
                ('qualityFlag', 'i2')]               # 产品数据质量标记, 1 完全可靠; 2 基本可靠; 3 有缺值, 可用; 4 不可用
            head1_info = np.frombuffer(byteArray[0:40], dtype=head1_dtype)
            ind = 40

            if head1_info['productCategory']:
                # the second class file head  二级文件头采用不定长方式，内容依据产品的不同而不同.
                head2_dtype = [
                    ('satelliteName', 'S8'),                 # 卫星名
                    ('year', 'i2'), ('month', 'i2'),
                    ('day', 'i2'), ('hour', 'i2'),
                    ('minute', 'i2'),
                    ('channel', 'i2'),                       # 通道号, 1红外, 2水汽, 3红外分裂, 4可见光, 5中红外, 6备用
                    ('flagOfProjection', 'i2'),              # 投影, 0为投影, 1兰勃托, 2麦卡托, 3极射, 4等经纬度, 5等面积
                    ('widthOfImage', 'i2'),
                    ('heightOfImage', 'i2'),
                    ('scanLineNumberOfImageTopLeft', 'i2'),
                    ('pixelNumberOfImageTopLeft', 'i2'),
                    ('sampleRatio', 'i2'),
                    ('latitudeOfNorth', 'i2'),
                    ('latitudeOfSouth', 'i2'),
                    ('longitudeOfWest', 'i2'),
                    ('longitudeOfEast', 'i2'),
                    ('centerLatitudeOfProjection', 'i2'),
                    ('centerLongitudeOfProjection', 'i2'),
                    ('standardLatitude1', 'i2'),
                    ('standardLatitude2', 'i2'),
                    ('horizontalResolution', 'i2'),
                    ('verticalResolution', 'i2'),
                    ('overlapFlagGeoGrid', 'i2'),
                    ('overlapValueGeoGrid', 'i2'),
                    ('dataLengthOfColorTable', 'i2'),
                    ('dataLengthOfCalibration', 'i2'),
                    ('dataLengthOfGeolocation', 'i2'),
                    ('reserved', 'i2')]
                head2_info = np.frombuffer(byteArray[ind:(ind+64)], dtype=head2_dtype)
                ind += 64

                # color table
                if head2_info['dataLengthOfColorTable'] != 0:
                    table_R =  np.frombuffer(byteArray[ind:(ind + 256)], dtype='u1')
                    ind += 256
                    table_G =  np.frombuffer(byteArray[ind:(ind + 256)], dtype='u1')
                    ind += 256
                    table_B =  np.frombuffer(byteArray[ind:(ind + 256)], dtype='u1')
                    ind += 256
                
                # calibration table
                calibration_table = None
                if head2_info['dataLengthOfCalibration'] != 0:
                    calibration_table = np.frombuffer(byteArray[ind:(ind + 2048)], dtype='i2')
                    calibration_table = calibration_table * 0.01
                    if (np.array_equal(calibration_table[0::4], calibration_table[1::4]) and
                        np.array_equal(calibration_table[0::4], calibration_table[2::4]) and
                        np.array_equal(calibration_table[0::4], calibration_table[3::4])):
                        # This is a trick, refer to http://bbs.06climate.com/forum.php?mod=viewthread&tid=89296
                        calibration_table = calibration_table[0::4]
                    ind += 2048

                # geolocation table
                if head2_info['dataLengthOfGeolocation'] != 0:
                    geolocation_dtype = [
                         ('coordinate', 'i2'),
                         ('source', 'i2'),
                         ('delta', 'i2'),
                         ('left_top_lat', 'i2'),
                         ('left_top_lon', 'i2'),
                         ('horizontalNumber', 'i2'),
                         ('verticalNumber', 'i2'),
                         ('reserved', 'i2')]
                    geolocation_info = np.frombuffer(byteArray[ind:(ind+16)], dtype=geolocation_dtype)
                    ind += 16
                    geolocation_length = geolocation_info['horizontal_number'][0] * geolocation_info['vertical_number'][0] * 2
                    geolocation_table = np.frombuffer(byteArray[ind:(ind+geolocation_length)], dtype='i2')
                    ind += geolocation_length

                # pad field
                pad_field = np.frombuffer(byteArray[ind:(ind+head1_info['padDataLength'][0])], dtype='u1')
                ind += head1_info['padDataLength'][0]

                 # retrieve data records
                data_len = (head1_info['dataRecordNumber'][0].astype(int) *
                            head1_info['recordLength'][0])
                data = np.frombuffer(byteArray[ind:(ind + data_len)], dtype='u1', count=data_len)
                if calibration_table is not None:
                    data = calibration_table[data]
                data.shape = (head1_info['dataRecordNumber'][0], head1_info['recordLength'][0])
                
                # 由于数据是按照左上角开始放置, 为此需要对纬度顺序进行反转
                data = np.flip(data, axis=0)

                # construct longitude and latitude coordinates
                # if use the verticalResolution and horizontalResolution, lon and lat will not be correct.
                #lat = (
                #    head2_info['latitudeOfNorth'][0]/100. - 
                #    np.arange(head2_info['heightOfImage'][0])*head2_info['verticalResolution'][0]/100.)
                #lon = (
                #    head2_info['longitudeOfWest'][0]/100. + 
                #    np.arange(head2_info['widthOfImage'][0])*head2_info['horizontalResolution'][0]/100.)
                lat = np.linspace(
                    head2_info['latitudeOfSouth'][0]/100., head2_info['latitudeOfNorth'][0]/100.,
                    num=head2_info['heightOfImage'][0])
                lon = np.linspace(
                    head2_info['longitudeOfWest'][0]/100., head2_info['longitudeOfEast'][0]/100.,
                    num=head2_info['widthOfImage'][0])
                
                # construct time
                time = datetime(
                    head2_info['year'][0], head2_info['month'][0],
                    head2_info['day'][0], head2_info['hour'][0], head2_info['minute'][0])
                time = np.array([time], dtype='datetime64[ms]')

                # define coordinates
                time_coord = ('time', time)
                lon_coord = ('lon', lon, {
                    'long_name':'longitude', 'units':'degrees_east',
                    '_CoordinateAxisType':'Lon', 'axis': "X"})
                lat_coord = ('lat', lat, {
                    'long_name':'latitude', 'units':'degrees_north',
                    '_CoordinateAxisType':'Lat', 'axis': "Y"})
                channel_coord = ('channel', [head2_info['channel'][0]],
                                 {'long_name':'channel', 'units':''})

                # create xarray
                data = data[np.newaxis, np.newaxis, ...]
                varattrs = {
                    'productCategory': head1_info['productCategory'][0],   # 产品类型, 1:静止, 2:极轨, 3:格点, 4:离散, 5:图形和分析
                    'formatString': head1_info['formatString'][0],         # 产品格式名称
                    'qualityFlag': head1_info['qualityFlag'][0],           # 产品质量标识
                    'satelliteName': head2_info['satelliteName'][0],       # 卫星名称
                    'flagOfProjection': head2_info['flagOfProjection'][0], # 投影方式, 0:未投影, 1:兰勃托, 2:麦卡托, 3:极射, 4:等经纬, 5:等面积
                    'units': units}
                data = xr.Dataset({
                    'image':(['time', 'channel', 'lat', 'lon'], data, varattrs)},
                    coords={ 'time':time_coord, 'channel':channel_coord,
                    'lat':lat_coord, 'lon':lon_coord})

                # add attributes
                data.attrs['Conventions'] = "CF-1.6"
                data.attrs['Origin'] = 'MICAPS Cassandra Server'

                # cache data
                if cache:
                    with open(cache_file, 'wb') as f:
                        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

                # return
                return data
            else:
                print("The productCategory is not supported.")
                return None
        else:
            return None
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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_fy_awx(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return xr.concat(dataset, dim='time')


def _lzw_decompress(compressed):
    """Decompress a list of output ks to a string.
    refer to https://stackoverflow.com/questions/6834388/basic-lzw-compression-help-in-python.
    """

    # Build the dictionary.
    dict_size = 256
    dictionary = {chr(i): chr(i) for i in range(dict_size)}

    w = result = compressed.pop(0)
    for k in compressed:
        if k in dictionary:
            entry = dictionary[k]
        elif k == dict_size:
            entry = w + w[0]
        else:
            raise ValueError('Bad compressed k: %s' % k)
        result += entry

        # Add w+entry[0] to the dictionary.
        dictionary[dict_size] = w + entry[0]
        dict_size += 1

        w = entry
    return result


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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray
            if byteArray == b'':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            # check data format version
            if byteArray[0:3] == b'MOC':
                # the new V3 format
                head_dtype = [
                    ('label', 'S4'),         # 文件固定标识：MOC
                    ('Version', 'S4'),       # 文件格式版本代码，如: 1.0，1.1，etc
                    ('FileBytes', 'i4'),     # 包含头信息在内的文件字节数，不超过2M
                    ('MosaicID', 'i2'),      # 拼图产品编号
                    ('coordinate', 'i2'),    # 坐标类型: 2=笛卡儿坐标,3=等经纬网格坐标
                    ('varname', 'S8'),       # 产品代码,如: ET,VIL,CR,CAP,OHP,OHPC
                    ('description', 'S64'),  # 产品描述,如Composite Reflectivity mosaic
                    ('BlockPos', 'i4'),      # 产品数据起始位置(字节顺序)
                    ('BlockLen', 'i4'),      # 产品数据字节数

                    ('TimeZone', 'i4'),      # 数据时钟,0=世界时,28800=北京时
                    ('yr', 'i2'),            # 观测时间中的年份
                    ('mon', 'i2'),           # 观测时间中的月份（1－12）
                    ('day', 'i2'),           # 观测时间中的日期（1－31）
                    ('hr', 'i2'),            # 观测时间中的小时（00－23）
                    ('min', 'i2'),           # 观测时间中的分（00－59）
                    ('sec', 'i2'),           # 观测时间中的秒（00－59）
                    ('ObsSeconds', 'i4'),    # 观测时间的seconds
                    ('ObsDates', 'u2'),      # 观测时间中的Julian dates
                    ('GenDates', 'u2'),      # 产品处理时间的天数
                    ('GenSeconds', 'i4'),    # 产品处理时间的描述

                    ('edge_s', 'i4'),        # 数据区的南边界，单位：1/1000度，放大1千倍
                    ('edge_w', 'i4'),        # 数据区的西边界，单位：1/1000度，放大1千倍
                    ('edge_n', 'i4'),        # 数据区的北边界，单位：1/1000度，放大1千倍
                    ('edge_e', 'i4'),        # 数据区的东边界，单位：1/1000度，放大1千倍
                    ('cx', 'i4'),            # 数据区中心坐标，单位：1/1000度，放大1千倍
                    ('cy', 'i4'),            # 数据区中心坐标，单位：1/1000度，放大1千倍
                    ('nX', 'i4'),            # 格点坐标为列数
                    ('nY', 'i4'),            # 格点坐标为行数
                    ('dx', 'i4'),            # 格点坐标为列分辨率，单位：1/10000度，放大1万倍
                    ('dy', 'i4'),            # 格点坐标为行分辨率，单位：1/10000度，放大1万倍
                    ('height', 'i2'),        # 雷达高度
                    ('Compress', 'i2'),      # 数据压缩标识, 0=无,1=bz2,2=zip,3=lzw
                    ('num_of_radars', 'i4'), # 有多少个雷达进行了拼图
                    ('UnZipBytes', 'i4'),    # 数据段压缩前的字节数
                    ('scale', 'i2'),
                    ('unUsed', 'i2'),
                    ('RgnID', 'S8'),
                    ('units', 'S8'),
                    ('reserved', 'S60')
                ]

                # read head information
                head_info = np.frombuffer(byteArray[0:256], dtype=head_dtype)
                ind = 256

                # get data information
                varname = head_info['varname'][0]
                longname = head_info['description'][0]
                units = head_info['units'][0]

                # define data variable
                rows = head_info['nY'][0]
                cols = head_info['nX'][0]
                dlat = head_info['dx'][0]/10000.
                dlon = head_info['dy'][0]/10000.

                # decompress byte Array
                # 目前主要支持bz2压缩格式, zip, lzw格式还没有测试过
                if   head_info['Compress'] == 0:    # 无压缩
                    data = np.frombuffer(byteArray[ind:], 'i2')
                elif head_info['Compress'] == 1:    # 
                    data = np.frombuffer(bz2.decompress(byteArray[ind:]), 'i2')
                elif head_info['Compress'] == 2:
                    data = np.frombuffer(zlib.decompress(byteArray[ind:]), 'i2')
                elif head_info['Compress'] == 3:
                    data = np.frombuffer(_lzw_decompress(byteArray[ind:]), 'i2')
                else:
                    print('Can not decompress data.')
                    return None
                
                # reshape data
                data.shape = (rows, cols)

                # deal missing data and restore values
                data = data.astype(np.float32)
                data[data < 0] = np.nan
                data /= head_info['scale'][0]

                # set longitude and latitude coordinates
                lat = head_info['edge_n'][0]/1000. - np.arange(rows)*dlat - dlat/2.0
                lon = head_info['edge_w'][0]/1000. + np.arange(cols)*dlon - dlon/2.0

                # reverse latitude axis
                data = np.flip(data, 0)
                lat = lat[::-1]

                # set time coordinates
                # 直接使用时间有问题, 需要天数减去1, 秒数加上28800(8小时)
                time = datetime(head_info['yr'][0], head_info['mon'][0], head_info['day'][0],
                                head_info['hr'][0], head_info['min'][0], head_info['sec'][0])
                time = np.array([time], dtype='datetime64[m]')
                data = np.expand_dims(data, axis=0)

            else:
                # the old LONLAT format.
                # define head structure
                head_dtype = [
                    ('description', 'S128'),
                    # product name,  QREF=基本反射率, CREF=组合反射率,
                    # VIL=液态水含量, OHP=一小时降水
                    ('name', 'S32'),
                    ('organization', 'S16'),
                    ('grid_flag', 'u2'),  # 经纬网格数据标识，固定值19532
                    ('data_byte', 'i2'),  # 数据单元字节数，固定值2
                    ('slat', 'f4'),       # 数据区的南纬（度）
                    ('wlon', 'f4'),       # 数据区的西经（度）
                    ('nlat', 'f4'),       # 数据区的北纬（度）
                    ('elon', 'f4'),       # 数据区的东经（度）
                    ('clat', 'f4'),       # 数据区中心纬度（度）
                    ('clon', 'f4'),       # 数据区中心经度（度）
                    ('rows', 'i4'),       # 数据区的行数
                    ('cols', 'i4'),       # 每行数据的列数
                    ('dlat', 'f4'),       # 纬向分辨率（度）
                    ('dlon', 'f4'),       # 经向分辨率（度）
                    ('nodata', 'f4'),     # 无数据区的编码值
                    ('levelbybtes', 'i4'),  # 单层数据字节数
                    ('levelnum', 'i2'),   # 数据层个数
                    ('amp', 'i2'),        # 数值放大系数
                    ('compmode', 'i2'),   # 数据压缩存储时为1，否则为0
                    ('dates', 'u2'),      # 数据观测时间，为1970年1月1日以来的天数
                    ('seconds', 'i4'),    # 数据观测时间的秒数
                    ('min_value', 'i2'),  # 放大后的数据最小取值
                    ('max_value', 'i2'),  # 放大后的数据最大取值
                    ('reserved', 'i2', 6)  # 保留字节
                ]

                # read head information
                head_info = np.frombuffer(byteArray[0:256], dtype=head_dtype)
                ind = 256

                # get data information
                varname = head_info['name'][0].decode("utf-8", 'ignore').rsplit('\x00')[0]
                longname = {'CREF': 'Composite Reflectivity', 'QREF': 'Basic Reflectivity',
                            'VIL': 'Vertically Integrated Liquid', 'OHP': 'One Hour Precipitation'}
                longname = longname.get(varname, 'radar mosaic')
                units = head_info['organization'][0].decode("utf-8", 'ignore').rsplit('\x00')[0]
                amp = head_info['amp'][0]

                # define data variable
                rows = head_info['rows'][0]
                cols = head_info['cols'][0]
                dlat = head_info['dlat'][0]
                dlon = head_info['dlon'][0]
                data = np.full(rows*cols, -9999, dtype=np.int32)

                # put data into array
                while ind < len(byteArray):
                    irow = np.frombuffer(byteArray[ind:(ind + 2)], dtype='i2')[0]
                    ind += 2
                    icol = np.frombuffer(byteArray[ind:(ind + 2)], dtype='i2')[0]
                    ind += 2
                    if irow == -1 or icol == -1:
                        break
                    nrec = np.frombuffer(byteArray[ind:(ind + 2)], dtype='i2')[0]
                    ind += 2
                    recd = np.frombuffer(
                        byteArray[ind:(ind + 2*nrec)], dtype='i2', count=nrec)
                    ind += 2*nrec
                    position = (irow-1)*cols+icol-1
                    data[position:(position+nrec)] = recd

                # reshape data
                data.shape = (rows, cols)

                # deal missing data and restore values
                data = data.astype(np.float32)
                data[data < 0] = np.nan
                data /= amp

                # set longitude and latitude coordinates
                lat = head_info['nlat'][0] - np.arange(rows)*dlat - dlat/2.0
                lon = head_info['wlon'][0] + np.arange(cols)*dlon - dlon/2.0

                # reverse latitude axis
                data = np.flip(data, 0)
                lat = lat[::-1]

                # set time coordinates
                # 直接使用时间有问题, 需要天数减去1, 秒数加上28800(8小时)
                time = datetime(1970, 1, 1) + timedelta(
                    days=head_info['dates'][0].astype(np.float64)-1,
                    seconds=head_info['seconds'][0].astype(np.float64)+28800)
                time = np.array([time], dtype='datetime64[m]')
                data = np.expand_dims(data, axis=0)

            # define coordinates
            time_coord = ('time', time)
            lon_coord = ('lon', lon, {
                'long_name':'longitude', 'units':'degrees_east',
                '_CoordinateAxisType':'Lon', "axis": "X"})
            lat_coord = ('lat', lat, {
                'long_name':'latitude', 'units':'degrees_north',
                '_CoordinateAxisType':'Lat', "axis": "Y"})

            # create xarray
            varattrs = {'long_name': longname, 'short_name': varname, 'units': units}
            data = xr.Dataset({'data':(['time', 'lat', 'lon'], data, varattrs)},
                coords={'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})

            # add attributes
            data.attrs['Conventions'] = "CF-1.6"
            data.attrs['Origin'] = 'MICAPS Cassandra Server'

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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_radar_mosaic(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return xr.concat(dataset, dim='time')


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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray
            if byteArray == b'':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            # decode bytes to string
            txt = byteArray.decode("utf-8")
            txt = list(filter(None, re.split(' |\n', txt)))

            # observation date and time
            if len(txt[3]) < 4:
                year = int(txt[3]) + 2000
            else:
                year = int(txt[3])
            month = int(txt[4])
            day = int(txt[5])
            hour = int(txt[6])
            time = datetime(year, month, day, hour)

            # the number of records
            number = int(txt[7])
            if number < 1:
                return None

            # cut the data
            txt = txt[8:]

            # put the data into dictionary
            index = 0
            records = []
            while index < len(txt):
                # get the record information
                ID = txt[index].strip()
                lon = float(txt[index+1])
                lat = float(txt[index+2])
                alt = float(txt[index+3])
                number = int(int(txt[index+4])/6)
                index += 5

                # get the sounding records
                for i in range(number):
                    record = {
                        'ID': ID, 'lon': lon, 'lat': lat, 'alt': alt,
                        'time': time,
                        'p': float(txt[index]), 'h': float(txt[index+1]),
                        't': float(txt[index+2]), 'td': float(txt[index+3]),
                        'wd': float(txt[index+4]),
                        'ws': float(txt[index+5])}
                    records.append(record)
                    index += 6

            # transform to pandas data frame
            records = pd.DataFrame(records)
            records.set_index('ID')

            # dealing missing values
            records = records.replace(9999.0, np.nan)
            if remove_duplicate:
                records = records.drop_duplicates()
            if remove_na:
                records = records.dropna(subset=['p', 'h', 't', 'td'])

            # the sounding height value convert to meters by multiple 10
            records['h'] = records['h'] * 10.0

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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_tlogp(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return pd.concat(dataset)


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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
        service = GDSDataService()
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray
            if byteArray == b'':
                print('There is no data ' + filename + ' in ' + directory)
                return None

            # define head structure
            head_dtype = [
                ('ZonName', 'S12'),
                ('DataName', 'S38'),
                ('Flag', 'S8'),
                ('Version', 'S8'),
                ('year', 'i2'),
                ('month', 'i2'),     
                ('day', 'i2'),     
                ('hour', 'i2'),    
                ('minute', 'i2'),
                ('interval', 'i2'),
                ('XNumGrids', 'i2'),
                ('YNumGrids', 'i2'),
                ('ZNumGrids', 'i2'),
                ('RadarCount', 'i4'),
                ('StartLon', 'f4'),
                ('StartLat', 'f4'),
                ('CenterLon', 'f4'),
                ('CenterLat', 'f4'),
                ('XReso', 'f4'),
                ('YReso', 'f4'),
                ('ZhighGrids', 'f4', 40),
                ('RadarStationName', 'S20', 16),
                ('RadarLongitude', 'f4', 20),
                ('RadarLatitude', 'f4', 20),
                ('RadarAltitude', 'f4', 20),
                ('MosaicFlag', 'S1', 20),
                ('m_iDataType', 'i2'),
                ('m_iLevelDimension', 'i2'),
                ('Reserved', 'S168')]

            # read head information
            head_info = np.frombuffer(byteArray[0:1024], dtype=head_dtype)
            ind = 1024

            # get coordinates
            nlon = head_info['XNumGrids'][0].astype(np.int64)
            nlat = head_info['YNumGrids'][0].astype(np.int64)
            nlev = head_info['ZNumGrids'][0].astype(np.int64)
            dlon = head_info['XReso'][0].astype(np.float)
            dlat = head_info['YReso'][0].astype(np.float)
            lat = head_info['StartLat'][0] - np.arange(nlat)*dlat - dlat/2.0
            lon = head_info['StartLon'][0] + np.arange(nlon)*dlon - dlon/2.0
            level = head_info['ZhighGrids'][0][0:nlev]

            # retrieve data records
            data_type = ['u1', 'u1', 'u2', 'i2']
            data_type = data_type[head_info['m_iDataType'][0]]
            data_len = (nlon * nlat * nlev)
            data = np.frombuffer(
                byteArray[ind:(ind + data_len*int(data_type[1]))], 
                dtype=data_type, count=data_len)

            # convert data type
            data.shape = (nlev, nlat, nlon)
            data = data.astype(np.float32)
            data = (data + scale[1]) * scale[0]

             # reverse latitude axis
            data = np.flip(data, 1)
            lat = lat[::-1]

            # set time coordinates
            init_time = datetime(
                head_info['year'][0], head_info['month'][0], 
                head_info['day'][0], head_info['hour'][0], head_info['minute'][0])
            if attach_forecast_period:
                fhour = int(filename.split('.')[1])/60.0
            else:
                fhour = 0
            fhour = np.array([fhour], dtype=np.float)
            time = init_time + timedelta(hours=fhour[0])
            init_time = np.array([init_time], dtype='datetime64[ms]')
            time = np.array([time], dtype='datetime64[ms]')

            # define coordinates
            time_coord = ('time', time)
            lon_coord = ('lon', lon, {
                'long_name':'longitude', 'units':'degrees_east',
                '_CoordinateAxisType':'Lon', "axis": "X"})
            lat_coord = ('lat', lat, {
                'long_name':'latitude', 'units':'degrees_north',
                '_CoordinateAxisType':'Lat', "axis": "Y"})
            level_coord = ('level', level, {
                'long_name':'height', 'units':'m'})

            # create xarray
            data = np.expand_dims(data, axis=0)
            data = xr.Dataset({'data':(['time', 'level', 'lat', 'lon'], data, varattrs)},
                coords={'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

            # add time coordinates
            data.coords['forecast_reference_time'] = init_time[0]
            data.coords['forecast_period'] = ('time', fhour, {
                'long_name':'forecast_period', 'units':'hour'})

            # add attributes
            data.attrs['Conventions'] = "CF-1.6"
            data.attrs['Origin'] = 'MICAPS Cassandra Server'

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

    dataset = []
    if pbar:
        tqdm_filenames = tqdm(filenames, desc=directory + ": ")
    else:
        tqdm_filenames = filenames
    for filename in tqdm_filenames:
        data = get_swan_radar(directory, filename=filename, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(directory+'/'+filename))
                return None
    
    return xr.concat(dataset, dim='time')


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
            service = GDSDataService()
            status, response = service.getLatestDataName(directory, suffix)
        except ValueError:
            print('Can not retrieve data from ' + directory)
            return None
        StringResult = DataBlock_pb2.StringResult()
        if status == 200:
            StringResult.ParseFromString(response)
            if StringResult is not None:
                filename = StringResult.name
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
            service = GDSDataService()
            status, response = service.getData(directory, filename)
        except ValueError:
            print('Can not retrieve data' + filename + ' from ' + directory)
            return None
        ByteArrayResult = DataBlock_pb2.ByteArrayResult()
        if status == 200:
            ByteArrayResult.ParseFromString(response)
            if ByteArrayResult is not None:
                byteArray = ByteArrayResult.byteArray
                if byteArray == b'':
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
