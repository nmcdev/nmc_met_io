# -*- coding: utf-8 -*-

"""Shared helpers for retrieval modules."""

from datetime import datetime, timedelta
import bz2
import re
import warnings
import zlib

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm


_STATION_COLUMN_MAP = {
    '3': 'Alt', '4': 'Grade', '5': 'Type', '21': 'Name',
    '201': 'Wind_angle', '203': 'Wind_speed', '205': 'Wind_angle_1m_avg',
    '207': 'Wind_speed_1m_avg', '209': 'Wind_angle_2m_avg',
    '211': 'Wind_speed_2m_avg', '213': 'Wind_angle_10m_avg',
    '215': 'Wind_speed_10m_avg', '217': 'Wind_angle_max',
    '219': 'Wind_speed_max', '221': 'Wind_angle_instant',
    '223': 'Wind_speed_instant', '225': 'Gust_angle', '227': 'Gust_speed',
    '229': 'Gust_angle_6h', '231': 'Gust_speed_6h', '233': 'Gust_angle_12h',
    '235': 'Gust_speed_12h', '237': 'Wind_power',
    '401': 'Sea_level_pressure', '403': 'Pressure_3h_trend',
    '405': 'Pressure_24h_trend', '407': 'Station_pressure',
    '409': 'Pressure_max', '411': 'Pressure_min', '413': 'Pressure',
    '415': 'Pressure_day_avg', '417': 'SLP_day_avg', '419': 'Hight',
    '421': 'Geopotential_hight', '601': 'Temp', '603': 'Temp_max',
    '605': 'Temp_min', '607': 'Temp_24h_trend', '609': 'Temp_24h_max',
    '611': 'Temp_24h_min', '613': 'Temp_dav_avg', '801': 'Dewpoint',
    '803': 'Dewpoint_depression', '805': 'Relative_humidity',
    '807': 'Relative_humidity_min', '809': 'Relative_humidity_day_avg',
    '811': 'Water_vapor_pressure', '813': 'Water_vapor_pressure_day_avg',
    '1001': 'Rain', '1003': 'Rain_1h', '1005': 'Rain_3h', '1007': 'Rain_6h',
    '1009': 'Rain_12h', '1011': 'Rain_24h', '1013': 'Rain_day',
    '1015': 'Rain_20-08', '1017': 'Rain_08-20', '1019': 'Rain_20-20',
    '1021': 'Rain_08-08', '1023': 'Evaporation', '1025': 'Evaporation_large',
    '1027': 'Precipitable_water', '1201': 'Vis_1min', '1203': 'Vis_10min',
    '1205': 'Vis_min', '1207': 'Vis_manual', '1401': 'Total_cloud_cover',
    '1403': 'Low_cloud_cover', '1405': 'Cloud_base_hight',
    '1407': 'Low_cloud', '1409': 'Middle_cloud', '1411': 'High_cloud',
    '1413': 'TCC_day_avg', '1415': 'LCC_day_avg', '1417': 'Cloud_cover',
    '1419': 'Cloud_type', '1601': 'Weather_current',
    '1603': 'Weather_past_1', '1605': 'Weather_past_2',
    '2001': 'Surface_temp', '2003': 'Surface_temp_max',
    '2005': 'Surface_temp_min',
}


def parse_model_grid_bytearray(
    byte_array, varname, varattrs, scale_off, levattrs, origin
):
    """Parse MICAPS model grid bytes to xarray.Dataset."""

    head_dtype = [
        ('discriminator', 'S4'), ('type', 'i2'),
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
        ('Extent', 'S92'),
    ]

    head_info = np.frombuffer(byte_array[0:278], dtype=head_dtype)
    data_type = head_info['type'][0]
    nlon = head_info['longitudeGridNumber'][0]
    nlat = head_info['latitudeGridNumber'][0]
    nmem = head_info['ensembleTotalNumber'][0]

    if data_type == 4:
        data_dtype = [('data', 'f4', (nlat, nlon))]
        data_len = nlat * nlon * 4
    elif data_type == 11:
        data_dtype = [('data', 'f4', (2, nlat, nlon))]
        data_len = 2 * nlat * nlon * 4
    else:
        raise Exception("Data type is not supported")

    if nmem == 0:
        data = np.frombuffer(byte_array[278:], dtype=data_dtype)
        data = np.squeeze(data['data'])
    else:
        if data_type == 4:
            data = np.full((nmem, nlat, nlon), np.nan)
        else:
            data = np.full((nmem, 2, nlat, nlon), np.nan)
        ind = 0
        for _ in range(nmem):
            head_info_mem = np.frombuffer(
                byte_array[ind:(ind + 278)], dtype=head_dtype
            )
            ind += 278
            data_mem = np.frombuffer(
                byte_array[ind:(ind + data_len)], dtype=data_dtype
            )
            ind += data_len
            number = head_info_mem['perturbationNumber'][0]
            if data_type == 4:
                data[number, :, :] = np.squeeze(data_mem['data'])
            else:
                data[number, :, :, :] = np.squeeze(data_mem['data'])

    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]

    slon = head_info['startLongitude'][0]
    dlon = head_info['longitudeGridSpace'][0]
    slat = head_info['startLatitude'][0]
    dlat = head_info['latitudeGridSpace'][0]
    lon = np.arange(nlon) * dlon + slon
    lat = np.arange(nlat) * dlat + slat
    level = np.array([head_info['level'][0]])

    init_time = datetime(
        head_info['year'][0], head_info['month'][0],
        head_info['day'][0], head_info['hour'][0]
    )
    fhour = np.array([head_info['period'][0]], dtype=np.float64)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name': 'longitude', 'units': 'degrees_east',
        '_CoordinateAxisType': 'Lon', 'axis': 'X'
    })
    lat_coord = ('lat', lat, {
        'long_name': 'latitude', 'units': 'degrees_north',
        '_CoordinateAxisType': 'Lat', 'axis': 'Y'
    })
    if level[0] != 0:
        level_coord = ('level', level, levattrs)
    if nmem != 0:
        number = np.arange(nmem)
        number_coord = ('number', number, {'_CoordinateAxisType': 'Ensemble'})

    if data_type == 4:
        if nmem == 0:
            if level[0] == 0:
                data = data[np.newaxis, ...]
                data = xr.Dataset({
                    varname: (['time', 'lat', 'lon'], data, varattrs)},
                    coords={'time': time_coord, 'lat': lat_coord, 'lon': lon_coord}
                )
            else:
                data = data[np.newaxis, np.newaxis, ...]
                data = xr.Dataset({
                    varname: (['time', 'level', 'lat', 'lon'], data, varattrs)},
                    coords={
                        'time': time_coord, 'level': level_coord,
                        'lat': lat_coord, 'lon': lon_coord
                    }
                )
        else:
            if level[0] == 0:
                data = data[:, np.newaxis, ...]
                data = xr.Dataset({
                    varname: (['number', 'time', 'lat', 'lon'], data, varattrs)},
                    coords={
                        'number': number_coord, 'time': time_coord,
                        'lat': lat_coord, 'lon': lon_coord
                    }
                )
            else:
                data = data[:, np.newaxis, np.newaxis, ...]
                data = xr.Dataset({
                    varname: (
                        ['number', 'time', 'level', 'lat', 'lon'],
                        data, varattrs
                    )},
                    coords={
                        'number': number_coord, 'time': time_coord,
                        'level': level_coord, 'lat': lat_coord, 'lon': lon_coord
                    }
                )
    elif data_type == 11:
        speedattrs = {'long_name': 'wind speed', 'units': 'm/s'}
        angleattrs = {'long_name': 'wind angle', 'units': 'degree'}
        if nmem == 0:
            speed = np.squeeze(data[0, :, :])
            angle = np.squeeze(data[1, :, :])
            angle = 270. - angle
            angle[angle < 0] = angle[angle < 0] + 360.
            if level[0] == 0:
                speed = speed[np.newaxis, ...]
                angle = angle[np.newaxis, ...]
                data = xr.Dataset({
                    'speed': (['time', 'lat', 'lon'], speed, speedattrs),
                    'angle': (['time', 'lat', 'lon'], angle, angleattrs)},
                    coords={'lon': lon_coord, 'lat': lat_coord, 'time': time_coord}
                )
            else:
                speed = speed[np.newaxis, np.newaxis, ...]
                angle = angle[np.newaxis, np.newaxis, ...]
                data = xr.Dataset({
                    'speed': (['time', 'level', 'lat', 'lon'], speed, speedattrs),
                    'angle': (['time', 'level', 'lat', 'lon'], angle, angleattrs)},
                    coords={
                        'lon': lon_coord, 'lat': lat_coord,
                        'level': level_coord, 'time': time_coord
                    }
                )
        else:
            speed = np.squeeze(data[:, 0, :, :])
            angle = np.squeeze(data[:, 1, :, :])
            angle = 270. - angle
            angle[angle < 0] = angle[angle < 0] + 360.
            if level[0] == 0:
                speed = speed[:, np.newaxis, ...]
                angle = angle[:, np.newaxis, ...]
                data = xr.Dataset({
                    'speed': (['number', 'time', 'lat', 'lon'], speed, speedattrs),
                    'angle': (['number', 'time', 'lat', 'lon'], angle, angleattrs)},
                    coords={
                        'lon': lon_coord, 'lat': lat_coord,
                        'number': number_coord, 'time': time_coord
                    }
                )
            else:
                speed = speed[:, np.newaxis, np.newaxis, ...]
                angle = angle[:, np.newaxis, np.newaxis, ...]
                data = xr.Dataset({
                    'speed': (
                        ['number', 'time', 'level', 'lat', 'lon'],
                        speed, speedattrs
                    ),
                    'angle': (
                        ['number', 'time', 'level', 'lat', 'lon'],
                        angle, angleattrs
                    )},
                    coords={
                        'lon': lon_coord, 'lat': lat_coord, 'level': level_coord,
                        'number': number_coord, 'time': time_coord
                    }
                )

    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name': 'forecast_period', 'units': 'hour'
    })
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = origin
    data = data.loc[{'lat': sorted(data.coords['lat'].values)}]
    return data


def parse_station_data_bytearray(byte_array, dropna=True):
    """Parse MICAPS station bytes to pandas.DataFrame."""

    head_dtype = [
        ('discriminator', 'S4'), ('type', 'i2'),
        ('description', 'S100'),
        ('level', 'f4'), ('levelDescription', 'S50'),
        ('year', 'i4'), ('month', 'i4'), ('day', 'i4'),
        ('hour', 'i4'), ('minute', 'i4'), ('second', 'i4'),
        ('Timezone', 'i4'), ('id_type', 'i2'), ('extent', 'S98')
    ]

    head_info = np.frombuffer(byte_array[0:288], dtype=head_dtype)
    id_type = head_info['id_type'][0]
    ind = 288
    station_number = np.frombuffer(byte_array[ind:(ind + 4)], dtype='i4')[0]
    ind += 4
    element_number = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
    ind += 2

    element_type_map = {
        1: 'b1', 2: 'i2', 3: 'i4', 4: 'i8', 5: 'f4', 6: 'f8', 7: 'S'
    }
    element_map = {}
    for _ in range(element_number):
        element_id = str(np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0])
        ind += 2
        element_type = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
        ind += 2
        element_map[element_id] = element_type_map[element_type]

    if id_type == 0:
        record_head_dtype = [('ID', 'i4'), ('lon', 'f4'), ('lat', 'f4'), ('numb', 'i2')]
        records = []
        for _ in range(station_number):
            record_head = np.frombuffer(byte_array[ind:(ind + 14)], dtype=record_head_dtype)
            ind += 14
            record = {
                'ID': record_head['ID'][0], 'lon': record_head['lon'][0],
                'lat': record_head['lat'][0]
            }
            for _ in range(record_head['numb'][0]):
                element_id = str(np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0])
                ind += 2
                element_type = element_map[element_id]
                if element_type == 'S':
                    str_len = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
                    ind += 2
                    element_type = element_type + str(str_len)
                element_len = int(element_type[1:])
                record[element_id] = np.frombuffer(
                    byte_array[ind:(ind + element_len)], dtype=element_type
                )[0]
                ind += element_len
            records += [record]
    else:
        record_head_dtype = [('lon', 'f4'), ('lat', 'f4'), ('numb', 'i2')]
        records = []
        for _ in range(station_number):
            id_string_length = np.frombuffer(byte_array[ind:(ind + 2)], dtype="i2")[0]
            record_id = np.frombuffer(
                byte_array[ind + 2:(ind + 2 + id_string_length)],
                dtype="S" + str(id_string_length)
            )[0].decode()
            ind += (2 + id_string_length)
            record_head = np.frombuffer(byte_array[ind:(ind + 10)], dtype=record_head_dtype)
            ind += 10
            record = {
                'ID': record_id, 'lon': record_head['lon'][0],
                'lat': record_head['lat'][0]
            }
            for _ in range(record_head['numb'][0]):
                element_id = str(np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0])
                ind += 2
                element_type = element_map[element_id]
                if element_type == 'S':
                    str_len = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
                    ind += 2
                    element_type = element_type + str(str_len)
                element_len = int(element_type[1:])
                record[element_id] = np.frombuffer(
                    byte_array[ind:(ind + element_len)], dtype=element_type
                )[0]
                ind += element_len
            records += [record]

    records = pd.DataFrame(records)
    records.set_index('ID')
    time = datetime(
        head_info['year'][0], head_info['month'][0], head_info['day'][0],
        head_info['hour'][0], head_info['minute'][0], head_info['second'][0]
    )
    records['time'] = time
    records.rename(columns=_STATION_COLUMN_MAP, inplace=True)
    if dropna:
        records = records.dropna(axis=1, how='all')
    return records


def collect_model_grids(
    directory, filenames, all_exists, pbar, get_file_list_func, get_model_grid_func, **kwargs
):
    """Collect multiple model grids and concat on time."""

    dataset = []
    iterator = tqdm(filenames, desc=directory + ": ") if pbar else filenames
    file_list = get_file_list_func(directory)
    for filename in iterator:
        if filename in file_list:
            data = get_model_grid_func(
                directory, filename=filename, check_file_first=False, **kwargs
            )
            if data is not None:
                dataset.append(data)
            elif all_exists:
                warnings.warn("{} doese not exists.".format(directory + '/' + filename))
                return None
        elif all_exists:
            warnings.warn("{} doese not exists.".format(directory + '/' + filename))
            return None
    return xr.concat(dataset, dim='time')


def collect_model_3d_grid(
    directory, filename, levels, all_exists, pbar, get_model_grid_func, **kwargs
):
    """Collect one time 3D model grid."""

    dataset = []
    iterator = tqdm(levels, desc=directory + ": ") if pbar else levels
    for level in iterator:
        data_dir = "{}/{}".format(directory.rstrip('/'), str(int(level)).strip())
        data = get_model_grid_func(data_dir, filename=filename, **kwargs)
        if data is not None:
            dataset.append(data)
        elif all_exists:
            warnings.warn("{} doese not exists.".format(data_dir + '/' + filename))
            return None
    return xr.concat(dataset, dim='level')


def collect_model_3d_grids(
    directory, filenames, levels, all_exists, pbar, get_model_grid_func, **kwargs
):
    """Collect multiple time 3D model grids."""

    dataset = []
    iterator = tqdm(filenames, desc=directory + ": ") if pbar else filenames
    for filename in iterator:
        dataset_temp = []
        for level in levels:
            data_dir = "{}/{}".format(directory.rstrip('/'), str(int(level)).strip())
            data = get_model_grid_func(data_dir, filename=filename, **kwargs)
            if data is not None:
                dataset_temp.append(data)
            elif all_exists:
                warnings.warn("{} doese not exists.".format(data_dir + '/' + filename))
                return None
        dataset.append(xr.concat(dataset_temp, dim='level'))
    return xr.concat(dataset, dim='time')


def collect_station_dataset(
    directory, filenames, all_exists, pbar, get_station_data_func, **kwargs
):
    """Collect multiple station datasets and concat rows."""

    dataset = []
    iterator = tqdm(filenames, desc=directory + ": ") if pbar else filenames
    for filename in iterator:
        data = get_station_data_func(directory, filename=filename, **kwargs)
        if data is not None:
            dataset.append(data)
        elif all_exists:
            warnings.warn("{} doese not exists.".format(directory + '/' + filename))
            return None
    return pd.concat(dataset)


def collect_xarray_dataset(
    directory, filenames, all_exists, pbar, get_data_func, concat_dim='time', **kwargs
):
    """Collect multiple xarray datasets and concat."""

    dataset = []
    iterator = tqdm(filenames, desc=directory + ": ") if pbar else filenames
    for filename in iterator:
        data = get_data_func(directory, filename=filename, **kwargs)
        if data is not None:
            dataset.append(data)
        elif all_exists:
            warnings.warn("{} doese not exists.".format(directory + '/' + filename))
            return None
    return xr.concat(dataset, dim=concat_dim)


def lzw_decompress(compressed):
    """Decompress a list of output ks to a string."""

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
        dictionary[dict_size] = w + entry[0]
        dict_size += 1
        w = entry
    return result


def parse_radar_mosaic_bytearray(byte_array, origin):
    """Parse radar mosaic bytes to xarray.Dataset."""

    if byte_array[0:3] == b'MOC':
        head_dtype = [
            ('label', 'S4'),
            ('Version', 'S4'),
            ('FileBytes', 'i4'),
            ('MosaicID', 'i2'),
            ('coordinate', 'i2'),
            ('varname', 'S8'),
            ('description', 'S64'),
            ('BlockPos', 'i4'),
            ('BlockLen', 'i4'),
            ('TimeZone', 'i4'),
            ('yr', 'i2'),
            ('mon', 'i2'),
            ('day', 'i2'),
            ('hr', 'i2'),
            ('min', 'i2'),
            ('sec', 'i2'),
            ('ObsSeconds', 'i4'),
            ('ObsDates', 'u2'),
            ('GenDates', 'u2'),
            ('GenSeconds', 'i4'),
            ('edge_s', 'i4'),
            ('edge_w', 'i4'),
            ('edge_n', 'i4'),
            ('edge_e', 'i4'),
            ('cx', 'i4'),
            ('cy', 'i4'),
            ('nX', 'i4'),
            ('nY', 'i4'),
            ('dx', 'i4'),
            ('dy', 'i4'),
            ('height', 'i2'),
            ('Compress', 'i2'),
            ('num_of_radars', 'i4'),
            ('UnZipBytes', 'i4'),
            ('scale', 'i2'),
            ('unUsed', 'i2'),
            ('RgnID', 'S8'),
            ('units', 'S8'),
            ('reserved', 'S60')
        ]

        head_info = np.frombuffer(byte_array[0:256], dtype=head_dtype)
        ind = 256

        varname = head_info['varname'][0]
        longname = head_info['description'][0]
        units = head_info['units'][0]
        rows = head_info['nY'][0]
        cols = head_info['nX'][0]
        dlat = head_info['dx'][0] / 10000.
        dlon = head_info['dy'][0] / 10000.

        if head_info['Compress'] == 0:
            data = np.frombuffer(byte_array[ind:], 'i2')
        elif head_info['Compress'] == 1:
            data = np.frombuffer(bz2.decompress(byte_array[ind:]), 'i2')
        elif head_info['Compress'] == 2:
            data = np.frombuffer(zlib.decompress(byte_array[ind:]), 'i2')
        elif head_info['Compress'] == 3:
            data = np.frombuffer(lzw_decompress(byte_array[ind:]), 'i2')
        else:
            return None

        data.shape = (rows, cols)
        data = data.astype(np.float32)
        data[data < 0] = np.nan
        data /= head_info['scale'][0]
        lat = head_info['edge_n'][0] / 1000. - np.arange(rows) * dlat - dlat / 2.0
        lon = head_info['edge_w'][0] / 1000. + np.arange(cols) * dlon - dlon / 2.0
        data = np.flip(data, 0)
        lat = lat[::-1]

        time = datetime(
            head_info['yr'][0], head_info['mon'][0], head_info['day'][0],
            head_info['hr'][0], head_info['min'][0], head_info['sec'][0]
        )
        time = np.array([time], dtype='datetime64[m]')
        data = np.expand_dims(data, axis=0)
    else:
        head_dtype = [
            ('description', 'S128'),
            ('name', 'S32'),
            ('organization', 'S16'),
            ('grid_flag', 'u2'),
            ('data_byte', 'i2'),
            ('slat', 'f4'),
            ('wlon', 'f4'),
            ('nlat', 'f4'),
            ('elon', 'f4'),
            ('clat', 'f4'),
            ('clon', 'f4'),
            ('rows', 'i4'),
            ('cols', 'i4'),
            ('dlat', 'f4'),
            ('dlon', 'f4'),
            ('nodata', 'f4'),
            ('levelbybtes', 'i4'),
            ('levelnum', 'i2'),
            ('amp', 'i2'),
            ('compmode', 'i2'),
            ('dates', 'u2'),
            ('seconds', 'i4'),
            ('min_value', 'i2'),
            ('max_value', 'i2'),
            ('reserved', 'i2', 6)
        ]

        head_info = np.frombuffer(byte_array[0:256], dtype=head_dtype)
        ind = 256
        varname = head_info['name'][0].decode("utf-8", 'ignore').rsplit('\x00')[0]
        longname = {
            'CREF': 'Composite Reflectivity',
            'QREF': 'Basic Reflectivity',
            'VIL': 'Vertically Integrated Liquid',
            'OHP': 'One Hour Precipitation'
        }.get(varname, 'radar mosaic')
        units = head_info['organization'][0].decode("utf-8", 'ignore').rsplit('\x00')[0]
        amp = head_info['amp'][0]
        rows = head_info['rows'][0]
        cols = head_info['cols'][0]
        dlat = head_info['dlat'][0]
        dlon = head_info['dlon'][0]
        data = np.full(rows * cols, -9999, dtype=np.int32)

        while ind < len(byte_array):
            irow = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
            ind += 2
            icol = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
            ind += 2
            if irow == -1 or icol == -1:
                break
            nrec = np.frombuffer(byte_array[ind:(ind + 2)], dtype='i2')[0]
            ind += 2
            recd = np.frombuffer(byte_array[ind:(ind + 2 * nrec)], dtype='i2', count=nrec)
            ind += 2 * nrec
            position = (irow - 1) * cols + icol - 1
            data[position:(position + nrec)] = recd

        data.shape = (rows, cols)
        data = data.astype(np.float32)
        data[data < 0] = np.nan
        data /= amp
        lat = head_info['nlat'][0] - np.arange(rows) * dlat - dlat / 2.0
        lon = head_info['wlon'][0] + np.arange(cols) * dlon - dlon / 2.0
        data = np.flip(data, 0)
        lat = lat[::-1]
        time = datetime(1970, 1, 1) + timedelta(
            days=head_info['dates'][0].astype(np.float64) - 1,
            seconds=head_info['seconds'][0].astype(np.float64) + 28800
        )
        time = np.array([time], dtype='datetime64[m]')
        data = np.expand_dims(data, axis=0)

    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name': 'longitude', 'units': 'degrees_east',
        '_CoordinateAxisType': 'Lon', 'axis': 'X'
    })
    lat_coord = ('lat', lat, {
        'long_name': 'latitude', 'units': 'degrees_north',
        '_CoordinateAxisType': 'Lat', 'axis': 'Y'
    })
    varattrs = {'long_name': longname, 'short_name': varname, 'units': units}
    data = xr.Dataset(
        {'data': (['time', 'lat', 'lon'], data, varattrs)},
        coords={'time': time_coord, 'lat': lat_coord, 'lon': lon_coord}
    )
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = origin
    return data


def parse_tlogp_bytearray(byte_array, remove_duplicate=False, remove_na=False):
    """Parse TLOGP bytes to pandas.DataFrame."""

    txt = byte_array.decode("utf-8")
    txt = list(filter(None, re.split(' |\n', txt)))
    if len(txt[3]) < 4:
        year = int(txt[3]) + 2000
    else:
        year = int(txt[3])
    month = int(txt[4])
    day = int(txt[5])
    hour = int(txt[6])
    time = datetime(year, month, day, hour)

    number = int(txt[7])
    if number < 1:
        return None

    txt = txt[8:]
    index = 0
    records = []
    while index < len(txt):
        station_id = txt[index].strip()
        lon = float(txt[index + 1])
        lat = float(txt[index + 2])
        alt = float(txt[index + 3])
        number = int(int(txt[index + 4]) / 6)
        index += 5

        for _ in range(number):
            record = {
                'ID': station_id, 'lon': lon, 'lat': lat, 'alt': alt,
                'time': time,
                'p': float(txt[index]), 'h': float(txt[index + 1]),
                't': float(txt[index + 2]), 'td': float(txt[index + 3]),
                'wd': float(txt[index + 4]), 'ws': float(txt[index + 5])
            }
            records.append(record)
            index += 6

    records = pd.DataFrame(records)
    records.set_index('ID')
    records = records.replace(9999.0, np.nan)
    if remove_duplicate:
        records = records.drop_duplicates()
    if remove_na:
        records = records.dropna(subset=['p', 'h', 't', 'td'])
    records['h'] = records['h'] * 10.0
    return records


def parse_swan_radar_bytearray(
    byte_array, filename, scale, varattrs, attach_forecast_period, origin
):
    """Parse SWAN radar bytes to xarray.Dataset."""

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
        ('Reserved', 'S168')
    ]

    head_info = np.frombuffer(byte_array[0:1024], dtype=head_dtype)
    ind = 1024
    nlon = head_info['XNumGrids'][0].astype(np.int64)
    nlat = head_info['YNumGrids'][0].astype(np.int64)
    nlev = head_info['ZNumGrids'][0].astype(np.int64)
    dlon = head_info['XReso'][0].astype(np.float64)
    dlat = head_info['YReso'][0].astype(np.float64)
    lat = head_info['StartLat'][0] - np.arange(nlat) * dlat - dlat / 2.0
    lon = head_info['StartLon'][0] + np.arange(nlon) * dlon - dlon / 2.0
    level = head_info['ZhighGrids'][0][0:nlev]

    data_type = ['u1', 'u1', 'u2', 'i2']
    data_type = data_type[head_info['m_iDataType'][0]]
    data_len = nlon * nlat * nlev
    data = np.frombuffer(
        byte_array[ind:(ind + data_len * int(data_type[1]))],
        dtype=data_type, count=data_len
    )

    data.shape = (nlev, nlat, nlon)
    data = data.astype(np.float32)
    data = (data + scale[1]) * scale[0]
    data = np.flip(data, 1)
    lat = lat[::-1]

    init_time = datetime(
        head_info['year'][0], head_info['month'][0],
        head_info['day'][0], head_info['hour'][0], head_info['minute'][0]
    )
    if attach_forecast_period:
        fhour = int(filename.split('.')[1]) / 60.0
    else:
        fhour = 0
    fhour = np.array([fhour], dtype=np.float64)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name': 'longitude', 'units': 'degrees_east',
        '_CoordinateAxisType': 'Lon', 'axis': 'X'
    })
    lat_coord = ('lat', lat, {
        'long_name': 'latitude', 'units': 'degrees_north',
        '_CoordinateAxisType': 'Lat', 'axis': 'Y'
    })
    level_coord = ('level', level, {'long_name': 'height', 'units': 'm'})

    data = np.expand_dims(data, axis=0)
    data = xr.Dataset(
        {'data': (['time', 'level', 'lat', 'lon'], data, varattrs)},
        coords={'time': time_coord, 'level': level_coord, 'lat': lat_coord, 'lon': lon_coord}
    )
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name': 'forecast_period', 'units': 'hour'
    })
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = origin
    return data


def extract_nafp_grid_metadata(contents, init_time_str, valid_time, fcst_level, units=None):
    """Extract common NAFP grid metadata used by CIMISS/CMADAAS readers."""

    init_time = datetime.strptime(init_time_str, '%Y%m%d%H')
    fhour = np.array([valid_time], dtype=np.float64)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

    start_lat = float(contents['startLat'])
    start_lon = float(contents['startLon'])
    nlon = int(contents['lonCount'])
    nlat = int(contents['latCount'])
    dlon = float(contents['lonStep'])
    dlat = float(contents['latStep'])
    lon = start_lon + np.arange(nlon) * dlon
    lat = start_lat + np.arange(nlat) * dlat
    name = contents['fieldNames']
    if units is None:
        units = contents['fieldUnits']

    if isinstance(fcst_level, str):
        fcst_level = 0

    return init_time, fhour, time, lon, lat, name, units, fcst_level
