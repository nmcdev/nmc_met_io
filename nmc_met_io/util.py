# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.


import os
import pathlib
import warnings
import re
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def product_filename(model=None, product=None, level=None, obs_time=None,
                     init_time=None, fhour=None, valid_time=None,
                     statistic=None, place=None, suffix=None, root_dir=None):
    """
    Construct standard product file name, all parameters should not
    include "_".

    :param model: model name.
    :param product: product name.
    :param level: vertical level.
    :param obs_time: observation level.
    :param init_time: model initial model time.
    :param fhour: model forecast time.
    :param valid_time: model forecast valid time.
    :param statistic: statistic method name.
    :param place: place or station name.
    :param suffix: file suffix.
    :param root_dir: file directory.
    :return: product file name.
    """

    # define filename
    filename = ""

    # model name
    if model is not None:
        filename = filename + "MD_" + str(model).strip().upper()

    # product name
    if product is not None:
        filename = filename + "_PD_" + str(product).strip()

    # vertical level
    if level is not None:
        filename = filename + "_LV_" + str(level).strip()

    # observation time
    if obs_time is not None:
        if isinstance(obs_time, datetime):
            filename = filename + "_OT_" + obs_time.strftime("%Y%m%d%H")
        elif isinstance(obs_time, np.datetime64):
            filename = filename + "_OT_" + \
                       pd.to_datetime(str(obs_time)).strftime("%Y%m%d%H")
        else:
            filename = filename + "_OT_" + str(obs_time).strip()

    # model initial time
    if init_time is not None:
        if isinstance(init_time, datetime):
            filename = filename + "_IT_" + init_time.strftime("%Y%m%d%H")
        elif isinstance(init_time, np.datetime64):
            filename = filename + "_IT_" + \
                       pd.to_datetime(str(init_time)).strftime("%Y%m%d%H")
        else:
            filename = filename + "_IT_" + str(init_time).strip()

    # model forecast hour
    if fhour is not None:
        filename = filename + "_FH_" + str(fhour).strip()

    # model valid time
    if valid_time is not None:
        if isinstance(valid_time, datetime):
            filename = filename + "_VT_" + valid_time.strftime("%Y%m%d%H")
        elif isinstance(valid_time, np.datetime64):
            filename = filename + "_VT_" + \
                       pd.to_datetime(str(valid_time)).strftime("%Y%m%d%H")
        else:
            filename = filename + "_VT_" + str(valid_time).strip()

    # statistic name
    if statistic is not None:
        filename = filename + "_SN_" + str(statistic).strip()

    # place name
    if place is not None:
        filename = filename + "_PN_" + str(place).strip()

    # remove the first "_"
    if filename[0] == "_":
        filename = filename[1:]

    # add suffix
    if suffix is not None:
        if suffix[0] == ".":
            filename = filename + suffix
        else:
            filename = filename + "." + suffix

    # add root directory
    if root_dir is not None:
        filename = os.path.join(root_dir, filename)

    # return
    return filename


def product_filename_retrieve(filename):
    """
    Retrieve information from the standard product filename.

    :param filename: file name.
    :return: filename information dictionary.
    """

    file_name = pathlib.PureWindowsPath(filename)
    file_stem = file_name.stem.split("_")
    return dict(zip(file_stem[0::2], file_stem[1::2]))


def get_fcst_times(validTime, initHours=[8, 20], format=None,
                   initStep=1, min_fhour=1, max_fhour=84):
    """
    获得某一预报时刻相应的不同起报时间, 预报时效序列.

    Args:
        validTime (string or datetime): 预报时间.
        initHours (int, optional): 模式的起报时刻, 例如 [8, 20]或[2,8,14,20]
        format (string, optional): 预报时间为字符串时的格式. Defaults to None.
        initStep (int, optional): 模式起报的时间间隔, 设置可以加快计算速度. Defaults to 1.
        min_fhour (int, optional): minimum forecast hour. Defaults to 0.
        max_fhour (int, optional): maximum forecast hour. Defaults to 84.
    """
    
    # convert to pd.Timestamp
    if not isinstance(validTime, pd.Timestamp):
        validTime = pd.to_datetime(validTime, format=format)
        
    # check initHour and find the first initial time
    initHours = np.asarray(initHours)
    if np.min(initHours) < 0 or np.max(initHours) > 23:
        raise ValueError('initHours should be in 0-23')
    fhours = []
    initTimes = []
    fhour = min_fhour
    while fhour <= max_fhour:
        initTime = validTime - pd.Timedelta(fhour, unit='hour')
        if initTime.hour in initHours and fhour >= min_fhour:
            fhours.append(fhour)
            initTimes.append(initTime)
            fhour += initStep
        else:
            fhour += 1
    
    # return
    return initTimes, fhours
    

def get_filenames(initTime, fhours="0/72/3;", zeroFill=3):
    """
    Construct filenames 
    
    Args:
        initTime (string or datatime): initTime
        fhours (str, optional): forecast hours description. Defaults to "0/72/3;".
        zeroFill (integer, optional): fill zero for fix length of forecast hour.

    Examples:
    >>> filenames =  get_filenames('19083020', fhours="0/72/3;72/246/6")
    >>> print(filenames)
    """

    if isinstance(initTime, datetime):
        initTimeStr = initTime.strftime("%y%m%d%H")
    else:
        initTimeStr = initTime

    filenames = []
    for fhour in fhours.split(";"):
        fhour = fhour.strip()
        if fhour == '':
            continue
        fhour = fhour.split("/")
        start = int(fhour[0])
        end = int(fhour[1])
        step = int(fhour[2])
        for fh in np.arange(start, end, step):
            filenames.append(initTimeStr+"."+str(fh).zfill(zeroFill))

    return filenames


def get_initTime_deal(periods, iTimes, currentTime=None):
    """
    Construct model initial time. 通过判断当前时间在某个时间段, 处理指定的起报时刻.
    
    Args:
        periods (list): 处理时间段, 格式为'start~end', 如['5~17']
        iTimes (list): 模式起报时刻, 格式为'today/yesterday-hour', 如['yesterday-20']
        currentTime (datetime, optional): current time. Defaults to system time.

    Examples:
    >>> periods = ['0~3', '4~15','16~23']
    >>> iTimes = ['yesterday-00', 'yesterday-12','today-00']
    >>> print(get_initTime_deal(periods, iTimes))
    """

    # get current time
    if currentTime is None:
        currentTime = datetime.now()

    # loop every periods
    for period, itime in zip(periods, iTimes):
        period = re.split(" |-|~", period)
        period = [float(p) for p in period]
        period = [min(period), max(period)]
        # construct initial time
        if (currentTime.hour >= period[0] and currentTime.hour <= period[1]):
            itime = re.split(' |-', itime)
            if itime[0].upper() == 'YESTERDAY':
                currentTime = currentTime - timedelta(days=1)
                initTime = datetime(currentTime.year, currentTime.month, currentTime.day, int(itime[1]))
                return initTime
            elif itime[0].upper() == 'TODAY':
                initTime = datetime(currentTime.year, currentTime.month, currentTime.day, int(itime[1]))
                return initTime
            else:
                return None


def get_initTime(iHours, delayHour=6, currentTime=None, N=1):
    """
    Construct model initial time. 通过当前时间, 模式的起报时刻, 以及模式的延迟时间, 计算模式的起报时间.
    
    Args:
        iHours (list): model initial hours, like [8, 20]
        delayHour (integer): model data delay hours.
        currentTime (datetime, optional): current time. Defaults to system time.
        N (integer): 返回最近的N个时次的模式起报时间.

    Examples:
    >>> print(get_initTime([8, 20]))
    """

    # get current time
    if currentTime is None:
        currentTime = datetime.now()
    currentTime = currentTime - timedelta(hours=delayHour)

    # loop every hour
    i = 0
    initTimes = []
    while (i < N):
        currentTime = currentTime - timedelta(hours=1)
        if currentTime.hour in iHours:
            initTime = datetime(
                currentTime.year, currentTime.month,
                currentTime.day, currentTime.hour)
            initTimes.append(initTime)
            i += 1
    
    return initTimes


def get_sub_stations(data, limit=[70, 140, 8, 60],
                     dimname=['lon', 'lat']):
    """
    Extract sub station observations.

    Args:
        data (pandas dataframe): observation records, 
        limit (list, optional): subregion lon and lat limit. Defaults to [70, 140, 8, 60].
        dimname (list, optional): dimension column name. Defaults to ['lon', 'lat']
    """

    if 'lon' in data:
        subdata = data[(limit[2] <= data['lat']) & (data['lat'] <= limit[3]) &
                       (limit[0] <= data['lon']) & (data['lon'] <= limit[1])]
    elif 'Lon' in data:
        subdata = data[(limit[2] <= data['Lat']) & (data['Lat'] <= limit[3]) &
                       (limit[0] <= data['Lon']) & (data['Lon'] <= limit[1])]
    elif 'x' in data:
        subdata = data[(limit[2] <= data['y']) & (data['y'] <= limit[3]) &
                       (limit[0] <= data['x']) & (data['x'] <= limit[1])]
    else:
        raise ValueError("'lon' or 'Lat' coordinates is not in data.")
    return subdata


def get_sub_grid(gdata, glon, glat, limit, pad=0):
    """
    Extract grid subset region.

    :param gdata: 2D grid data, [nlat, nlon]
    :param glon: 1D or 2D array, longitude.
    :param glat: 1D or 2D array, latitude.
    :param limit: subset boundary, [lonmin, lonmax, latmin, latmax]
    :param pad: pad for bounds.
    :return: subset boundary index.
    """
    
    # add pad
    limit = [limit[0]-pad, limit[1]+pad,
             limit[2]-pad, limit[3]+pad]
    
    # convert to numpy array
    lat = np.squeeze(np.asarray(glat))
    lon = np.squeeze(np.asarray(glon))
    gdata = np.squeeze(np.asarray(gdata))
    
    # get 1D coordinates
    if glon.ndim == 2:
        lat = lat[:, 0]
        lon = lon[0, :]

    # latitude lower and upper index
    latli = np.argmin(np.abs(lat - limit[2]))
    latui = np.argmin(np.abs(lat - limit[3]))

    # longitude lower and upper index
    lonli = np.argmin(np.abs(lon - limit[0]))
    lonui = np.argmin(np.abs(lon - limit[1]))
    
    # extract information
    if glon.ndim == 2:
        glon = glon[latli:latui+1, lonli:lonui+1]
        glat = glat[latli:latui+1, lonli:lonui+1]
    else:
        glon = glon[lonli:lonui+1]
        glat = glat[latli:latui+1]
    gdata = gdata[latli:latui+1, lonli:lonui+1]

    # return subset boundary index
    return gdata, glon, glat
