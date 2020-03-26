# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.


import warnings
import re
import pathlib
from datetime import datetime, timedelta
import numpy as np


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


def get_initTime(iHours, delayHour=6, currentTime=None):
    """
    Construct model initial time. 通过当前时间, 模式的起报时刻, 以及模式的延迟时间, 计算模式的起报时间.
    
    Args:
        iHours (list): model initial hours, like [8, 20]
        delayHour (integer): model data delay hours.
        currentTime (datetime, optional): current time. Defaults to system time.

    Examples:
    >>> print(get_initTime([8, 20]))
    """

    # get current time
    if currentTime is None:
        currentTime = datetime.now()
    currentTime = currentTime - timedelta(hours=delayHour)

    # loop every hour
    i = 0
    while (not currentTime.hour in iHours):
        currentTime = currentTime - timedelta(hours=1)
        i += 1
        if i > 24:
            warnings.warn("Can not find initial time.")
            break

    # return initial time
    initTime = datetime(currentTime.year, currentTime.month,
                        currentTime.day, currentTime.hour)
    return initTime
