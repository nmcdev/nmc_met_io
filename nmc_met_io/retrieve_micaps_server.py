# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
This is the retrieve module which get data from MICAPS cassandra service
with Python API.

Checking url, like:
http://10.32.8.164:8080/DataService?requestType=getLatestDataName&directory=ECMWF_HR/TMP/850&fileName=&filter=*.024
"""

import re
import http.client
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import pandas as pd
from nmc_met_io import DataBlock_pb2
from nmc_met_io.config import _get_config_from_rcfile


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
        config = _get_config_from_rcfile()
        self.gdsIp = config['MICAPS']['GDS_IP']
        self.gdsPort = config['MICAPS']['GDS_PORT']

    def getLatestDataName(self, directory, filter):
        return get_http_result(
            self.gdsIp, self.gdsPort, "/DataService" +
            self.get_concate_url("getLatestDataName", directory, "", filter))

    def getData(self, directory, fileName):
        return get_http_result(
            self.gdsIp, self.gdsPort, "/DataService" +
            self.get_concate_url("getData", directory, fileName, ""))

    # 将请求参数拼接到url
    def get_concate_url(self, requestType, directory, fileName, filter):
        url = ""
        url += "?requestType=" + requestType
        url += "&directory=" + directory
        url += "&fileName=" + fileName
        url += "&filter=" + filter
        return url


def get_model_grid(directory, filename=None, suffix="*.024"):
    """
    Retrieve numeric model grid forecast from MICAPS cassandra service.
    Support ensemble member forecast.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :return: data, xarray type

    :Examples:
    >>> data = get_model_grid("ECMWF_HR/TMP/850")
    >>> data_ens = get_model_grid("ECMWF_ENSEMBLE/RAW/HGT/500",
                                  filename='18021708.024')
    """

    # connect to data service
    service = GDSDataService()

    # get data file name
    if filename is None:
        try:
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

    # get data contents
    try:
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray

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
            head_info = np.fromstring(byteArray[0:278], dtype=head_dtype)

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
                data = np.fromstring(byteArray[278:], dtype=data_dtype)
                data = np.squeeze(data['data'])
            else:
                if data_type == 4:
                    data = np.full((nmem, nlat, nlon), np.nan)
                else:
                    data = np.full((2, nmem, nlat, nlon), np.nan)
                ind = 0
                for imem in range(nmem):
                    head_info_mem = np.fromstring(
                        byteArray[ind:(ind+278)], dtype=head_dtype)
                    ind += 278
                    data_mem = np.fromstring(
                        byteArray[ind:(ind+data_len)], dtype=data_dtype)
                    ind += data_len
                    number = head_info_mem['perturbationNumber'][0]
                    if data_type == 4:
                        data[number, :, :] = np.squeeze(data_mem['data'])
                    else:
                        data[:, number, :, :] = np.squeeze(data_mem['data'])

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
            init_time = np.array([init_time], dtype='datetime64[m]')
            time = np.array([time], dtype='datetime64[m]')

            # construct ensemble number
            if nmem != 0:
                number = np.arange(nmem)

            # create to xarray
            if data_type == 4:
                if nmem == 0:
                    if level[0] == 0:
                        data = data[np.newaxis, ...]
                        data = xr.DataArray(
                            data, coords=[time, lat, lon],
                            dims=['time', 'lat', 'lon'], name="data")
                    else:
                        data = data[np.newaxis, np.newaxis, ...]
                        data = xr.DataArray(
                            data, coords=[time, level, lat, lon],
                            dims=['time', 'level', 'lat', 'lon'],
                            name="data")
                else:
                    if level[0] == 0:
                        data = data[np.newaxis, ...]
                        data = xr.DataArray(
                            data, coords=[time, number, lat, lon],
                            dims=['time', 'number', 'lat', 'lon'],
                            name="data")
                    else:
                        data = data[np.newaxis, :, np.newaxis, ...]
                        data = xr.DataArray(
                            data, coords=[time, number, level, lat, lon],
                            dims=['time', 'number', 'level', 'lat', 'lon'],
                            name="data")
            elif data_type == 11:
                if nmem == 0:
                    speed = np.squeeze(data[0, :, :])
                    angle = np.squeeze(data[1, :, :])
                    if level[0] == 0:
                        speed = speed[np.newaxis, ...]
                        angle = angle[np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (['time', 'lat', 'lon'], speed),
                            'angle': (['time', 'lat', 'lon'], angle)},
                            coords={'lon': lon, 'lat': lat, 'time': time})
                    else:
                        speed = speed[np.newaxis, np.newaxis, ...]
                        angle = angle[np.newaxis, np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (['time', 'level', 'lat', 'lon'], speed),
                            'angle': (['time', 'level', 'lat', 'lon'], angle)},
                            coords={'lon': lon, 'lat': lat, 'level': level,
                                    'time': time})
                else:
                    speed = np.squeeze(data[0, :, :, :])
                    angle = np.squeeze(data[1, :, :, :])
                    if level[0] == 0:
                        speed = speed[np.newaxis, ...]
                        angle = angle[np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (
                                ['time', 'number', 'lat', 'lon'], speed),
                            'angle': (
                                ['time', 'number', 'lat', 'lon'], angle)},
                            coords={
                                'lon': lon, 'lat': lat, 'number': number,
                                'time': time})
                    else:
                        speed = speed[np.newaxis, :, np.newaxis, ...]
                        angle = angle[np.newaxis, :, np.newaxis, ...]
                        data = xr.Dataset({
                            'speed': (
                                ['time', 'number', 'level', 'lat', 'lon'],
                                speed),
                            'angle': (
                                ['time', 'number', 'level', 'lat', 'lon'],
                                angle)},
                            coords={
                                'lon': lon, 'lat': lat, 'level': level,
                                'number': number, 'time': time})
            # add time coordinates
            data.coords['init_time'] = ('time', init_time)
            data.coords['fhour'] = ('time', fhour)

            # add attributes
            data.attrs['data_directory'] = directory
            data.attrs['data_filename'] = filename
            data.attrs['organization'] = 'Created by NMC.'

            # return data
            return data

        else:
            return None
    else:
        return None


def get_station_data(directory, filename=None, suffix="*.000"):
    """
    Retrieve station data from MICAPS cassandra service.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will
                   be used to find the specified file.
    :return: pandas DataFrame.

    :example:
    >>> data = get_station_data("SURFACE/PLOT_10MIN")
    """

    # connect to data service
    service = GDSDataService()

    # get data file name
    if filename is None:
        try:
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

    # get data contents
    try:
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
            head_info = np.fromstring(byteArray[0:288], dtype=head_dtype)
            ind = 288

            # read the number of stations
            station_number = np.fromstring(
                byteArray[ind:(ind+4)], dtype='i4')[0]
            ind += 4

            # read the number of elements
            element_number = np.fromstring(
                byteArray[ind:(ind+2)], dtype='i2')[0]
            ind += 2

            # construct record structure
            element_type_map = {
                1: 'b1', 2: 'i2', 3: 'i4', 4: 'i8', 5: 'f4', 6: 'f8', 7: 'S1'}
            element_map = {}
            for i in range(element_number):
                element_id = str(
                    np.fromstring(byteArray[ind:(ind+2)], dtype='i2')[0])
                ind += 2
                element_type = np.fromstring(
                    byteArray[ind:(ind+2)], dtype='i2')[0]
                ind += 2
                element_map[element_id] = element_type_map[element_type]

            # loop every station to retrieve record
            record_head_dtype = [
                ('ID', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('numb', 'i2')]
            records = []
            for i in range(station_number):
                record_head = np.fromstring(
                    byteArray[ind:(ind+14)], dtype=record_head_dtype)
                ind += 14
                record = {
                    'ID': record_head['ID'][0], 'lon': record_head['lon'][0],
                    'lat': record_head['lat'][0]}
                for j in range(record_head['numb'][0]):    # the record element number is not same, missing value is not included.
                    element_id = str(
                        np.fromstring(byteArray[ind:(ind + 2)], dtype='i2')[0])
                    ind += 2
                    element_type = element_map[element_id]
                    element_len = int(element_type[1])
                    record[element_id] = np.fromstring(
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
            records.rename(columns={'3': 'alt',
                '201': 'wind_angle', '203': 'wind_speed', '205': 'wind_angle_1m_avg', '207': 'wind_speed_1m_avg',
                '209': 'wind_angle_2m_avg', '211': 'wind_speed_2m_avg', '213': 'wind_angle_10m_avg', '215': 'wind_speed_10m_avg',
                '217': 'wind_angle_max', '219': 'wind_speed_max', '221': 'wind_angle_instant', '223': 'wind_speed_instant',
                '225': 'gust_angle', '227': 'gust_speed', '229': 'gust_angle_6h', '231': 'gust_speed_6h',
                '233': 'gust_angle_12h', '235': 'gust_speed_12h', '237': 'wind_power', 
                '401': 'sea_level_pressure', '403': 'pressure_3h_trend', '405': 'pressure_24h_trend',
                '407': 'station_pressure', '409': 'pressure_max', '411': 'pressure_min', '413': 'pressure',
                '415': 'pressure_day_avg', '417': 'slp_day_avg', '419': 'hight', '421': 'geopotential_hight',
                '601': 'temp', '603': 'temp_max', '605': 'temp_min', '607': 'temp_24h_trend', 
                '609': 'temp_24h_max', '611':'temp_24h_min', '613': 'temp_dav_avg',
                '801': 'dewpoint', '803': 'dewpoint_depression', '805': 'relative_humidity',
                '807': 'relative_humidity_min', '809': 'relative_humidity_day_avg', 
                '811': 'water_vapor_pressure', '813': 'water_vapor_pressure_day_avg',
                '1001': 'rain', '1003': 'rain_1h', '1005': 'rain_3h', '1007': 'rain_6h', '1009': 'rain_12h', '1013': 'rain_day',
                '1015': 'rain_20-08', '1017': 'rain_08-20', '1019': 'rain_20-20', '1021': 'rain_08-08',
                '1023': 'evaporation', '1025': 'evaporation_large', '1027': 'precipitable_water',
                '1201': 'vis_1min', '1203': 'vis_10min', '1205': 'vis_min', '1207': 'vis_manual',
                '1401': 'total_cloud_cover', '1403': 'low_cloud_cover', '1405': 'cloud_base_hight',
                '1407': 'low_cloud', '1409': 'middle_cloud', '1411': 'high_cloud',
                '1413': 'tcc_day_avg', '1415': 'lcc_day_avg', '1417': 'cloud_cover', '1419': 'cloud_type',
                '1601': 'weather_current', '1603': 'weather_past_1', '1606': 'weather_past_2',
                '2001': 'surface_temp', '2003': 'surface_temp_max', '2005': 'surface_temp_min'},
                inplace=True)

            # return
            return records
        else:
            return None
    else:
        return None


def get_fy_awx(directory, filename=None, suffix="*.AWX"):
    """
    Retrieve FY satellite cloud awx format file.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :return: satellite information and data.

    :Examples:
    >>> directory = "SATELLITE/FY2E/L1/IR1/EQUAL"
    >>> data = get_fy_awx(directory)
    """

    # connect to data service
    service = GDSDataService()

    # get data file name
    if filename is None:
        try:
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

    # get data contents
    try:
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
            head_dtype = [
                ('SAT96', 'S12'),    # SAT96 filename
                ('byteSequence', 'i2'),    # integer number byte sequence
                ('firstClassHeadLength', 'i2'),
                ('secondClassHeadLength', 'i2'),
                ('padDataLength', 'i2'),
                ('recordLength', 'i2'),
                ('headRecordNumber', 'i2'),
                ('dataRecordNumber', 'i2'),
                ('productCategory', 'i2'),
                ('compressMethod', 'i2'),
                ('formatString', 'S8'),
                ('qualityFlag', 'i2'),
                ('satelliteName', 'S8'),
                ('year', 'i2'), ('month', 'i2'),
                ('day', 'i2'), ('hour', 'i2'),
                ('minute', 'i2'),
                ('channel', 'i2'),
                ('flagOfProjection', 'i2'),
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
            head_info = np.fromstring(byteArray[0:104], dtype=head_dtype)
            ind = 104

            # head rest information
            head_rest_len = (head_info['recordLength'][0].astype(np.int) *
                             head_info['headRecordNumber'][0] - ind)
            head_rest = np.fromstring(
                byteArray[ind:(ind + head_rest_len)],
                dtype='u1', count=head_rest_len)
            ind += head_rest_len

            # retrieve data records
            data_len = (head_info['recordLength'][0].astype(np.int) *
                        head_info['dataRecordNumber'][0])
            data = np.fromstring(
                byteArray[ind:(ind + data_len)], dtype='u1',
                count=data_len)
            data.shape = (head_info['recordLength'][0],
                          head_info['dataRecordNumber'][0])

            # return
            return head_info, data
        else:
            return None
    else:
        return None


def get_radar_mosaic(directory, filename=None, suffix="*.LATLON"):
    """
    该程序主要用于读取和处理中国气象局CRaMS系统的雷达回波全国拼图数据.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :return: xarray object.

    :Example:
    >>> data = get_radar_mosaic("RADARMOSAIC/CREF/")
    """

    # connect to data service
    service = GDSDataService()

    # get data file name
    if filename is None:
        try:
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

    # get data contents
    try:
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
            head_info = np.fromstring(byteArray[0:256], dtype=head_dtype)
            ind = 256

            # define data variable
            rows = head_info['rows'][0]
            cols = head_info['cols'][0]
            dlat = head_info['dlat'][0]
            dlon = head_info['dlon'][0]
            data = np.full(rows*cols, -9999, dtype=np.int32)

            # put data into array
            while ind < len(byteArray):
                irow = np.fromstring(byteArray[ind:(ind + 2)], dtype='i2')[0]
                ind += 2
                icol = np.fromstring(byteArray[ind:(ind + 2)], dtype='i2')[0]
                ind += 2
                if irow == -1 or icol == -1:
                    break
                nrec = np.fromstring(byteArray[ind:(ind + 2)], dtype='i2')[0]
                ind += 2
                recd = np.fromstring(
                    byteArray[ind:(ind + 2*nrec)], dtype='i2', count=nrec)
                ind += 2*nrec
                position = (irow-1)*cols+icol-1
                data[position:(position+nrec)] = recd - 1

            # reshape data
            data.shape = (rows, cols)

            # set longitude and latitude coordinates
            lats = head_info['nlat'][0] - np.arange(rows)*dlat - dlat/2.0
            lons = head_info['wlon'][0] - np.arange(cols)*dlon - dlon/2.0

            # reverse latitude axis
            data = np.flip(data, 0)
            lats = lats[::-1]

            # set time coordinates
            time = datetime(1970, 1, 1) + timedelta(
                days=head_info['dates'][0].astype(np.float64),
                seconds=head_info['seconds'][0].astype(np.float64))
            time = np.array([time], dtype='datetime64[m]')
            data = np.expand_dims(data, axis=0)

            # create xarray
            data = xr.DataArray(
                data, coords=[time, lats, lons],
                dims=['time', 'latitude', 'longitude'],
                name="radar_mosaic")

            # return
            return data
        else:
            return None
    else:
        return None


def get_tlogp(directory, filename=None, suffix="*.000"):
    """
    该程序用于读取micaps服务器上TLOGP数据信息, 文件格式与MICAPS第5类格式相同.

    :param directory: the data directory on the service
    :param filename: the data filename, if none, will be the latest file.
    :param suffix: the filename filter pattern which will be used to
                   find the specified file.
    :return: pandas DataFrame object.

    >>> data = get_tlogp("UPPER_AIR/TLOGP/")
    """

    # connect to data service
    service = GDSDataService()

    # get data file name
    if filename is None:
        try:
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

    # get data contents
    try:
        status, response = service.getData(directory, filename)
    except ValueError:
        print('Can not retrieve data' + filename + ' from ' + directory)
        return None
    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
    if status == 200:
        ByteArrayResult.ParseFromString(response)
        if ByteArrayResult is not None:
            byteArray = ByteArrayResult.byteArray

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
                        'wind_angle': float(txt[index+4]),
                        'wind_speed': float(txt[index+5])}
                    records.append(record)
                    index += 6

            # transform to pandas data frame
            records = pd.DataFrame(records)
            records.set_index('ID')

            # return
            return records
        else:
            return None
    else:
        return None

