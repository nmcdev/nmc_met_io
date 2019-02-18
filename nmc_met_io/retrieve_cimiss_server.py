# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve the CIMISS data using REST API with pure python code.

refer to:
  http://10.20.76.55/cimissapiweb/MethodData_list.action
  https://github.com/babybearming/CIMISSDataGet/blob/master/cimissRead_v0.1.py
"""

import json
from datetime import datetime, timedelta
import urllib3
import numpy as np
import pandas as pd
import xarray as xr
from nmc_met_io.config import _get_config_from_rcfile


def get_http_result(interface_id, params, data_format='json'):
    """
    Get the http result from CIMISS REST api service.

    :param interface_id: MUSIC interface id.
    :param params: dictionary for MUSIC parameters.
    :param data_format: MUSIC server data format.
    :return:
    """

    # set  MUSIC server dns and user information
    config = _get_config_from_rcfile()
    dns = config['CIMISS']['DNS']
    user_id = config['CIMISS']['USER_ID']
    pwd = config['CIMISS']['PASSWORD']

    # construct url
    url = 'http://' + dns + '/cimiss-web/api?userId=' + user_id + \
          '&pwd=' + pwd + '&interfaceId=' + interface_id

    # params
    for key in params:
        url += '&' + key + '=' + params[key]

    # data format
    url += '&dataFormat=' + data_format

    # request http contents
    http = urllib3.PoolManager()
    req = http.request('GET', url)
    if req.status != 200:
        print('Can not access the url: ' + url)
        return None

    return req.data


def cimiss_obs_by_time_range(time_range, sta_levels=None,
                             data_code="SURF_CHN_MUL_HOR_N",
                             elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS by time and station ID.

    :param time_range: time range for retrieve,
                       "[YYYYMMDDHHMISS, YYYYMMDDHHMISS]",
                       like"[201509010000,20150903060000]"
    :param sta_levels: station levels, like "011,012,013" for standard,
                       base and general stations.
    :param data_code: dataset code, like "SURF_CHN_MUL_HOR",
                      "SURF_CHN_MUL_HOR_N", and so on.
    :param elements: elements for retrieve, 'ele1,ele2,...'
    :return: observation records, pandas DataFrame type

    :Example:
    >>> time_range = "[20180219000000,20180221000000]"
    >>> sta_levels = "011,012,013"
    >>> data_code = "SURF_CHN_MUL_DAY"
    >>> elements = "Station_Id_C,Station_Name,Datetime,Lat,Lon,PRE_Time_0808"
    >>> data = cimiss_obs_by_time_range(time_range, sta_levels=sta_levels,
                                        data_code=data_code, elements=elements)
    >>> print "retrieve successfully" if data is not None else "failed"
    retrieve successfully
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'timeRange': time_range}
    if sta_levels is not None:
        params['staLevels'] = sta_levels

    # interface id
    interface_id = "getSurfEleByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])

    # return
    return data


def cimiss_obs_by_time_and_id(times, data_code="SURF_CHN_MUL_HOR_N",
                              elements="Station_Id_C,Datetime,TEM",
                              sta_ids="54511"):
    """
    Retrieve station records from CIMISS by time and station ID

    :param times: time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
    :param data_code: dataset code, like "SURF_CHN_MUL_HOR",
                      "SURF_CHN_MUL_HOR_N", and so on.
    :param elements: elements for retrieve, 'ele1,ele2,...'
    :param sta_ids: station ids, 'xxxxx,xxxxx,...'
    :return: observation records, pandas DataFrame type

    :Example:
    >>> data = cimiss_obs_by_time_and_id('20170318000000')
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'times': times,
              'staIds': sta_ids,
              'orderby': "Datetime:ASC"}

    # interface id
    interface_id = "getSurfEleByTimeAndStaID"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])

    # return
    return data


def cimiss_obs_in_rect_by_time(times, limit, data_code="SURF_CHN_MUL_HOR_N",
                               elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS in region by time.

    :param times: times for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param data_code: dataset code, like "SURF_CHN_MUL_HOR",
                      "SURF_CHN_MUL_HOR_N", and so on
    :param elements: elements for retrieve, 'ele1,ele2,...'
    :return: observation records, pandas DataFrame type

    :Example:
    >>> data = cimiss_obs_in_rect_by_time('20170320000000', [35, 110, 45, 120])
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'times': times,
              'minLat': '{:.10f}'.format(limit[0]),
              'minLon': '{:.10f}'.format(limit[1]),
              'maxLat': '{:.10f}'.format(limit[2]),
              'maxLon': '{:.10f}'.format(limit[3]),
              'orderby': "Datetime:ASC"}

    # interface id
    interface_id = "getSurfEleInRectByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])

    # return
    return data


def cimiss_obs_in_rect_by_time_range(
        time_range, limit,
        data_code="SURF_CHN_MUL_HOR_N",
        elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve observation records from CIMISS by rect and time range.

    :param time_range: time range for retrieve,
                       "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
    :param limit: (min_lat, min_lon, max_lat, max_lon)
    :param data_code: dataset code, like "SURF_CHN_MUL_HOR",
                      "SURF_CHN_MUL_HOR_N", and so on.
    :param elements: elements for retrieve, 'ele1,ele2,...'
    :return: observation records, pandas DataFrame type

    :Example:
    >>> elements = ("Station_Id_C,Station_Id_d,Station_Name,"
                    "Station_levl,Datetime,Lat,Lon,PRE_Time_0808")
    >>> time_range = "[20160801000000,20160801000000]"
    >>> data_code = "SURF_CHN_MUL_DAY"
    >>> data = cimiss_obs_in_rect_by_time_range(
            time_range,[35,110,45,120], data_code=data_code,
            elements=elements)
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'timeRange': time_range,
              'minLat': '{:.10f}'.format(limit[0]),
              'minLon': '{:.10f}'.format(limit[1]),
              'maxLat': '{:.10f}'.format(limit[2]),
              'maxLon': '{:.10f}'.format(limit[3]),
              'orderby': "Datetime:ASC"}

    # interface id
    interface_id = "getSurfEleInRectByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])

    # return
    return data


def cimiss_obs_grid_by_time(
        time_str, data_code="SURF_CMPA_RT_NC", fcst_ele="PRE"):
    """
    Retrieve surface analysis grid products,
    like CMPAS-V2.1融合降水分析实时数据产品（NC）.
    For SURF_CMPA_RT_NC, this function will retrieve
    the 0.01 resolution data and take long time.

    :param time_str: analysis time string, like "2017100800"
    :param data_code: data code
    :param fcst_ele: elements
    :return: data, xarray type

    :Example:
    >>> time_str = "2017110612"
    >>> data_code = "SURF_CMPA_RT_NC"
    >>> data = cimiss_obs_grid_by_time(time_str, data_code=data_code,
                                       fcst_ele="PRE")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'time': time_str + "0000",
              'fcstEle': fcst_ele}

    # set interface id
    interface_id = "getSurfEleGridByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # get time information
    time = datetime.strptime(time_str, '%Y%m%d%H')

    # extract coordinates and data
    start_lat = float(contents['startLat'])
    start_lon = float(contents['startLon'])
    nlon = int(contents['lonCount'])
    nlat = int(contents['latCount'])
    dlon = float(contents['lonStep'])
    dlat = float(contents['latStep'])
    lon = start_lon + np.arange(nlon) * dlon
    lat = start_lat + np.arange(nlat) * dlat
    name = contents['fieldNames']
    units = contents['fieldUnits']

    # construct xarray
    data = np.array(contents['DS'])
    data = data[np.newaxis, ...]
    data = xr.DataArray(data, coords=[time, lat, lon],
                        dims=['time', 'lat', 'lon'], name=name)

    # add attributes
    data.attrs['units'] = units
    data.attrs['organization'] = 'Created by NMC.'

    # return data
    return data


def cimiss_obs_file_by_time_range(time_range,
                                  data_code="SURF_CMPA_RT_NC"):
    """
    Retrieve CIMISS data file information.

    :param time_range: time range for retrieve,
                       "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
    :param data_code: data code
    :return: dictionary

    :Examples:
    >>> time_range = "[20180401000000,20180402000000]"
    >>> files = cimiss_obs_file_by_time_range(time_range)
    >>> filenames = files['DS']
    >>> print(files['DS'][0]['FILE_URL'])
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'timeRange': time_range}

    # set interface id
    interface_id = "getSurfFileByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # return
    return contents


def cimiss_model_by_time(init_time_str, limit=None,
                         data_code='NAFP_FOR_FTM_HIGH_EC_GLB',
                         fcst_level=0, valid_time=0, fcst_ele="TEF2"):
    """
    Retrieve grid data from CIMISS service.

    :param init_time_str: model run time, like "2016081712"
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param data_code: MUSIC data code, default is "NAFP_FOR_FTM_HIGH_EC_GLB"
    :param fcst_level: vertical level, default is 0.
    :param valid_time: forecast element, default is 2m temperature "TEF2"
    :param fcst_ele: forecast hour, default is 0
    :return:
    """

    # set retrieve parameters
    if limit is None:
        params = {'dataCode': data_code,
                  'time': init_time_str + '0000',
                  'fcstLevel': '{:d}'.format(fcst_level),
                  'validTime': '{:d}'.format(valid_time),
                  'fcstEle': fcst_ele}

        interface_id = 'getNafpEleGridByTimeAndLevelAndValidtime'
    else:
        params = {'dataCode': data_code,
                  'time': init_time_str + '0000',
                  'minLat': '{:.10f}'.format(limit[0]),
                  "minLon": '{:.10f}'.format(limit[1]),
                  "maxLat": '{:.10f}'.format(limit[2]),
                  "maxLon": '{:.10f}'.format(limit[3]),
                  'fcstLevel': '{:d}'.format(fcst_level),
                  'validTime': '{:d}'.format(valid_time),
                  'fcstEle': fcst_ele}

        interface_id = 'getNafpEleGridInRectByTimeAndLevelAndValidtime'

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None

    # get time information
    init_time = datetime.strptime(init_time_str, '%Y%m%d%H')
    fhour = valid_time
    time = init_time + timedelta(hours=fhour)

    # extract coordinates and data
    start_lat = float(contents['startLat'])
    start_lon = float(contents['startLon'])
    nlon = int(contents['lonCount'])
    nlat = int(contents['latCount'])
    dlon = float(contents['lonStep'])
    dlat = float(contents['latStep'])
    lon = start_lon + np.arange(nlon)*dlon
    lat = start_lat + np.arange(nlat)*dlat
    name = contents['fieldNames']
    units = contents['fieldUnits']

    # construct xarray
    data = np.array(contents['DS'])
    if fcst_level == 0:
        data = data[np.newaxis, ...]
        data = xr.DataArray(data, coords=[time, lat, lon],
                            dims=['time', 'lat', 'lon'], name=name)
    else:
        data = data[np.newaxis, np.newaxis, ...]
        data = xr.DataArray(data, coords=[time, fcst_level, lat, lon],
                            dims=['time', 'level', 'lat', 'lon'], name=name)

    # add time coordinates
    data.coords['init_time'] = ('time', init_time)
    data.coords['fhour'] = ('time', fhour)

    # add attributes
    data.attrs['units'] = units
    data.attrs['organization'] = 'Created by NMC.'

    # return data
    return data
