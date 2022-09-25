# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve the CIMISS data using REST API with pure python code.

refer to:
  http://10.20.76.55/cimissapiweb/MethodData_list.action
  https://github.com/babybearming/CIMISSDataGet/blob/master/cimissRead_v0.1.py
"""

import os
import warnings
import json
import pickle
from datetime import datetime, timedelta
import urllib3
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm
import nmc_met_io.config as CONFIG
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
    dns = CONFIG.CONFIG['CIMISS']['DNS']
    user_id = CONFIG.CONFIG['CIMISS']['USER_ID']
    pwd = CONFIG.CONFIG['CIMISS']['PASSWORD']

    # construct url
    url = 'http://' + dns + '/cimiss-web/api?userId=' + user_id + \
          '&pwd=' + pwd + '&interfaceId=' + interface_id

    # params
    for key in params:
        url += '&' + key + '=' + params[key].strip()

    # data format
    url += '&dataFormat=' + data_format

    # request http contents
    http = urllib3.PoolManager()
    req = http.request('GET', url)
    if req.status != 200:
        print('Can not access the url: ' + url)
        return None

    return req.data


def cimiss_obs_convert_type(obs_data):
    """
    Convert observation dataframe to numeric types.
    
    Args:
        obs_data (dataframe): data frame of observations.
    """
    for column in obs_data.columns:
        if column.upper().startswith('STATION'):
            continue
        if column.lower() in [
            "province", "country", "city", "cnty", "town",
            "data_id", "rep_corr_id", "admin_code_chn"]:
            continue
        if column.upper() == "DATETIME":
            obs_data[column] = pd.to_datetime(obs_data[column], format="%Y%m%d%H%M%S")
            continue
        obs_data[column] = pd.to_numeric(obs_data[column])
        obs_data[column].mask(obs_data[column] >=  999990.0, inplace=True)

    return obs_data


def cimiss_get_obs_latest_time(data_code="SURF_CHN_MUL_HOR", latestTime=6):
    """
    Get the latest time of the observations.
    
    Args:
        data_code (str, optional): dataset code, like "SURF_CHN_MUL_HOR", 
                                   "SURF_CHN_MUL_HOR_N". Defaults to "SURF_CHN_MUL_HOR".
        latestTime (int, optional): latestTime > 0, like 2 is return 
                                    the latest time in 2 hours. Defaults to 6.
    Returns:
        the latest time, like '20200216020000'
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'latestTime': str(latestTime)}

    # interface id
    interface_id = "getSurfLatestTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    time = pd.to_datetime(data['Datetime'], format="%Y%m%d%H%M%S")

    return time[0]


def cimiss_obs_by_time(times, data_code="SURF_CHN_MUL_HOR_N",
                       sta_levels=None, ranges=None, order=None, 
                       count=None, distinct=False, trans_type=True,
                       elements="Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM"):
    """
    Retrieve station records from CIMISS by times.
    
    Args:
        times (str): time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        distinct (bool, optional): return unique records. Defaults to False.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
        obsdata = cimiss_obs_by_time('20181108000000')
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'times': times,
              'orderby': order if order is not None else "Datetime:ASC",
              'elements': elements}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)
    if distinct: params['distinct'] = "true"

    # Interface, refer to
    # http://10.20.76.55/cimissapiweb/apicustomapiclassdefine_list.action?ids=getSurfEleByTime&apiclass=SURF_API
    interface_id = "getSurfEleByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_by_time_range(time_range, data_code="SURF_CHN_MUL_HOR_N",
                             sta_levels=None, ranges=None, order=None, 
                             count=None, distinct=False, trans_type=True,
                             elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS by time range.
    
    Args:
        time_range (str): time range for retrieve,  "[YYYYMMDDHHMISS, YYYYMMDDHHMISS]",
                          like"[201509010000,20150903060000]"
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        distinct (bool, optional): return unique records. Defaults to False.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
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
              'timeRange': time_range,
              'orderby': order if order is not None else "Datetime:ASC",
              'elements': elements,}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)
    if distinct: params['distinct'] = "true"

    # interface id
    interface_id = "getSurfEleByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_by_time_and_id(times, data_code="SURF_CHN_MUL_HOR_N",
                              sta_levels=None, ranges=None, order=None, 
                              count=None, trans_type=True,
                              elements="Station_Id_C,Datetime,TEM",
                              sta_ids="54511"):
    """
    Retrieve station records from CIMISS by times and station ID
    
    Args:
        times (str): time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
        sta_ids (str, optional): station ids, 'xxxxx,xxxxx,...'. Defaults to "54511".
    
    Returns:
       pandas data frame: observation records.
    
    Examples:
    >>> data = cimiss_obs_by_time_and_id('20170318000000')
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'times': times,
              'staIds': sta_ids,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleByTimeAndStaID"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_by_time_range_and_id(time_range, data_code="SURF_CHN_MUL_HOR_N",
                                    sta_levels=None, ranges=None, order=None, 
                                    count=None, trans_type=True,
                                    elements="Station_Id_C,Datetime,TEM",
                                    sta_ids="54511"):
    """
    Retrieve station records from CIMISS by time range and station ID
    
    Args:
        time_range (str): time range for retrieve,  "[YYYYMMDDHHMISS, YYYYMMDDHHMISS]",
                          like"[201509010000,20150903060000]"
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
        sta_ids (str, optional): station ids, 'xxxxx,xxxxx,...'. Defaults to "54511".
    
    Returns:
       pandas data frame: observation records.
    
    Examples:
    >>> data = cimiss_obs_by_time_range_and_id("[20160801000000,20160802000000]")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'timeRange': time_range,
              'staIds': sta_ids,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleByTimeRangeAndStaID"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_rect_by_time(times, limit, data_code="SURF_CHN_MUL_HOR_N",
                               sta_levels=None, ranges=None, order=None, 
                               count=None, trans_type=True,
                               elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS in region by times.
    
    Args:
        times (str): time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
        limit (list):  map limits, [min_lat, min_lon, max_lat, max_lon]
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
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
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleInRectByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_rect_by_time_range(time_range, limit, data_code="SURF_CHN_MUL_HOR_N",
                                     sta_levels=None, ranges=None, order=None, 
                                     count=None, trans_type=True,
                                     elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve observation records from CIMISS by rect and time range.
    
    Args:
        time_range (str): time for retrieve, "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
        limit (list):  map limits, [min_lat, min_lon, max_lat, max_lon]
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
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
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleInRectByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_admin_by_time(times, admin="110000", data_code="SURF_CHN_MUL_HOR_N",
                                sta_levels=None, ranges=None, order=None, 
                                count=None, trans_type=True,
                                elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS in provinces by time.
    
    Args:
        times (str): time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
        admin (str, optional):  administration(or province code), sperated by ",",
                      like "110000" is Beijing, "440000" is Guangdong. Defaults to "110000".
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
    >>> data = cimiss_obs_in_admin_by_time('20200206000000', admin="110000")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'times': times,
              'adminCodes': admin,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleInRegionByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_admin_by_time_range(time_range, admin="110000", data_code="SURF_CHN_MUL_HOR_N",
                                     sta_levels=None, ranges=None, order=None, 
                                     count=None, trans_type=True,
                                     elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve observation records from CIMISS by provinces and time range.
    
    Args:
        time_range (str): time for retrieve, "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
        admin (str, optional):  administration(or province code), sperated by ",",
                      like "110000" is Beijing, "440000" is Guangdong. Defaults to "110000".
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
    >>> elements = ("Station_Id_C,Station_Id_d,Station_Name,"
                    "Station_levl,Datetime,Lat,Lon,PRE_Time_0808")
    >>> time_range = "[20160801000000,20160801000000]"
    >>> data_code = "SURF_CHN_MUL_DAY"
    >>> data = cimiss_obs_in_admin_by_time_range(
            time_range,admin="110000", data_code=data_code,
            elements=elements)
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'timeRange': time_range,
              'adminCodes': admin,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleInRegionByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_basin_by_time(times, basin="CJLY", data_code="SURF_CHN_MUL_HOR_N",
                                sta_levels=None, ranges=None, order=None, 
                                count=None, trans_type=True,
                                elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve station records from CIMISS in basin by time.
    
    Args:
        times (str): time for retrieve, 'YYYYMMDDHHMISS,YYYYMMDDHHMISS,...'
        basin (str, optional):  basin codes, sperated by ",",  like "CJLY" is Yangzi River, 
                                "sta_2480" is 2480 stations. Defaults to "CJLY".
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
    >>> data = cimiss_obs_in_basin_by_time('20200206000000', basinCodes="CJLY")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'times': times,
              'basinCodes': basin,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    interface_id = "getSurfEleInBasinByTime"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_basin_by_time_range(time_range, basin="CJLY", data_code="SURF_CHN_MUL_HOR_N",
                                      sta_levels=None, ranges=None, order=None, 
                                      count=None, trans_type=True,
                                      elements="Station_Id_C,Datetime,Lat,Lon,TEM"):
    """
    Retrieve observation records from CIMISS by basin and time range.
    
    Args:
        time_range (str): time for retrieve, "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
        basin (str, optional):  basin codes, sperated by ",",  like "CJLY" is Yangzi River, 
                                "sta_2480" is 2480 stations. Defaults to "CJLY".
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        sta_levels (str, optional): station levels, seperated by ',',
             like "011,012,013" for standard, base and general stations. Defaults to None.
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
    >>> elements = ("Station_Id_C,Station_Id_d,Station_Name,"
                    "Station_levl,Datetime,Lat,Lon,PRE_Time_0808")
    >>> time_range = "[20160801000000,20160801000000]"
    >>> data_code = "SURF_CHN_MUL_DAY"
    >>> data = cimiss_obs_in_basin_by_time_range(
            time_range, basin="CJLY", data_code=data_code,
            elements=elements)
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'elements': elements,
              'timeRange': time_range,
              'basinCodes': basin,
              'orderby': order if order is not None else "Datetime:ASC"}
    if sta_levels is not None: params['staLevels'] = sta_levels
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # interface id
    # http://10.20.76.55/cimissapiweb/apicustomapiclassdefine_list.action?ids=getNafpEleGridByTimeAndLevelAndValidtime&apiclass=NAFP_API
    interface_id = "getSurfEleInBasinByTimeRange"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_by_period(minYear, maxYear, minMD, maxMD, data_code="SURF_CHN_MUL_HOR_N",
                         ranges=None, order=None, count=None, trans_type=True,
                         elements="Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM"):
    """
    Retrieve station records from CIMISS by same period in years.
    
    Args:
        minYear (int): start year, like 2005
        maxYear (int): end year, like 2005
        minMD (str): start date, like "0125" is 01/25
        maxMD (str): end date, like "0205" is 02/25
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
        obsdata = cimiss_obs_by_period(2015, 2018, "0501", "0505")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'minYear': str(int(minYear)),
              'maxYear': str(int(maxYear)),
              'minMD': str(minMD),
              'maxMD': str(maxMD),
              'orderby': order if order is not None else "Datetime:ASC",
              'elements': elements}
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # Interface, refer to
    interface_id = "getSurfEleByInHistoryBySamePeriod"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_by_period_and_id(minYear, maxYear, minMD, maxMD, data_code="SURF_CHN_MUL_HOR_N",
                                ranges=None, order=None, count=None, trans_type=True,
                                elements="Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM",
                                sta_ids="54511"):
    """
    Retrieve station id records from CIMISS by same period in years.
    
    Args:
        minYear (int): start year, like 2005
        maxYear (int): end year, like 2005
        minMD (str): start date, like "0125" is 01/25
        maxMD (str): end date, like "0205" is 02/25
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
         sta_ids (str, optional): station ids, 'xxxxx,xxxxx,...'. Defaults to "54511".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
        obsdata = cimiss_obs_by_period_and_id(2015, 2018, "0501", "0505", sta_ids="54511")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'minYear': str(int(minYear)),
              'maxYear': str(int(maxYear)),
              'minMD': str(minMD),
              'maxMD': str(maxMD),
              'orderby': order if order is not None else "Datetime:ASC",
              'elements': elements,
              'staIds': sta_ids}
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # Interface, refer to
    interface_id = "getSurfEleInHistoryBySamePeriodAndStaID"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_in_admin_by_period(minYear, maxYear, minMD, maxMD, admin="110000", data_code="SURF_CHN_MUL_HOR_N",
                                  ranges=None, order=None, count=None, trans_type=True,
                                  elements="Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM"):
    """
    Retrieve station records from CIMISS by same period in years and in provinces.
    
    Args:
        minYear (int): start year, like 2005
        maxYear (int): end year, like 2005
        minMD (str): start date, like "0125" is 01/25
        maxMD (str): end date, like "0205" is 02/25
        admin (str, optional):  administration(or province code), sperated by ",",
                      like "110000" is Beijing, "440000" is Guangdong. Defaults to "110000".
        data_code (str, optional): dataset code. Defaults to "SURF_CHN_MUL_HOR_N".
        ranges (str, optional): elements value ranges, seperated by ';'
            range: (a,) is >a, [a,) is >=a, (,a) is <a, (,a] is <=a, (a,b) is >a & <b, 
                   [a,b) is >=a & <b, (a,b] is >a & <=b, [a,b] is >=a & <=b
            list: a,b,c;
            e.g., "VIS:(,1000);RHU:(70,)", "Q_PRE_1h:0,3,4" is PRE quantity is credible.. Defaults to None.
        order (str, optional): elements order, seperated by ',', like
            "TEM:asc,SUM_PRE_1h:desc" is ascending order temperature first and descending PRE_1h. Defaults to None.
        count (int, optional): the number of maximum returned records. Defaults to None.
        trans_type (bool, optional): transform the return data frame's column type to datetime, numeric. Defaults to True.
        elements (str, optional): elements for retrieve, 'ele1,ele2,...'. 
            Defaults to "Station_Id_C,Station_Id_d,lat,lon,Datetime,TEM".
    
    Returns:
        pandas data frame: observation records.
    
    Examples:
        obsdata = cimiss_obs_in_admin_by_period(2015, 2018, "0501", "0505", admin="110000")
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'minYear': str(int(minYear)),
              'maxYear': str(int(maxYear)),
              'minMD': str(minMD),
              'maxMD': str(maxMD),
              'adminCodes': admin,
              'orderby': order if order is not None else "Datetime:ASC",
              'elements': elements}
    if ranges is not None: params['eleValueRanges'] = ranges
    if count is not None: params['limitCnt'] = str(count)

    # Interface, refer to
    interface_id = "getSurfEleInHistoryBySamePeriodAndRegion"

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    if trans_type: data = cimiss_obs_convert_type(data)

    # return
    return data


def cimiss_obs_grid_by_time(time_str, limit=None, data_code="SURF_CMPA_FRT_5KM",
                            fcst_ele="PRE", zoom=None, units=None, scale_off=None,
                            cache=True, cache_clear=True):
    """
    Retrieve surface analysis grid products, like CMPAS-V2.1融合降水分析实时数据产品（NC）.
    For SURF_CMPA_RT_NC, this function will retrieve the 0.01 resolution data and take long time.

    :param time_str: analysis time string, like "20171008000000", format: YYYYMMDDHHMISS
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param data_code: MUSIC data code, default is "SURF_CMPA_FRT_5KM"
        "SURF_CMPA_RT_NC": 实时产品滞后约50~60分钟,分两种空间分辨率：1小时/0.05°、1小时/0.01°
        "SURF_CMPA_FRT_5KM_10MIN"(PRE_10MIN): CMPAS-V2.1融合降水分析实时数据10分钟产品
        "SURF_CMPA_FAST_5KM": CMPAS-V2.1融合降水分析快速数据小时产品, 时效在15分钟以内，包含小时降水和3小时累计降水两种要素
        "SURF_CMPA_FRT_5KM": CMPAS-V2.1融合降水分析实时数据小时产品（GRIB，5km）
        "SURF_CMPA_FAST_5KM_DAY": CMPAS-V2.1融合降水分析快速数据日产品（GRIB，5km）
        "SURF_CMPA_FRT_5KM_DAY": CMPAS-V2.1融合降水分析实时数据日产品（GRIB，5km）
    :param fcst_ele: elements
    :param zoom: the zoom out integer > 1, like 2.
    :param units: forecast element's units, defaults to retrieved units.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param cache: cache retrieved data to local directory, default is True.
    :return: data, xarray type

    :Example:
    >>> time_str = "20171106120000"
    >>> data_code = "SURF_CMPA_FRT_5KM"
    >>> data = cimiss_obs_grid_by_time(time_str, data_code=data_code, fcst_ele="PRE")
    """

    # retrieve data from cached file
    if cache:
        directory = os.path.join(data_code, fcst_ele)
        filename = time_str
        if limit is not None:
            filename = filename + '.' + str(limit).replace(" ","")
        cache_file = CONFIG.get_cache_file(directory, filename, name="CIMISS", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # set retrieve parameters
    if limit is None:
        params = {'dataCode': data_code,
                  'time': time_str,
                  'fcstEle': fcst_ele}
        if zoom is not None: params['zoomOut'] = str(zoom)
        interface_id = "getSurfEleGridByTime"
    else:
        params = {'dataCode': data_code,
                  'time': time_str,
                  'minLat': '{:.10f}'.format(limit[0]),
                  "minLon": '{:.10f}'.format(limit[1]),
                  "maxLat": '{:.10f}'.format(limit[2]),
                  "maxLon": '{:.10f}'.format(limit[3]),
                  'fcstEle': fcst_ele}
        if zoom is not None: params['zoomOut'] = str(zoom)
        interface_id = "getSurfEleGridInRectByTime"
    
    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # get time information
    time = datetime.strptime(time_str, '%Y%m%d%H%M%S')
    time = np.array([time], dtype="datetime64[ms]")

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
    if units is None:
        units = contents['fieldUnits']
    
    # define coordinates and variables
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east',
        '_CoordinateAxisType':'Lon', 'axis':'X'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north',
        '_CoordinateAxisType':'Lat', 'axis':'Y'})
    varname = fcst_ele
    varattrs = {'long_name': name, 'units': units}

    # construct xarray
    data = np.array(contents['DS'], dtype=np.float32)
    data[data == 9999.] = np.nan
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]
    data = data[np.newaxis, ...]
    data = xr.Dataset({
        varname:(['time', 'lat', 'lon'], data, varattrs)},
        coords={'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add global attributes
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = 'CIMISS Server by MUSIC API'

    # cache data
    if cache:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # return data
    return data


def cimiss_obs_grid_by_times(times_str, pbar=True, allExists=True, **kargs):
    """
    Retrieve multiple surface analysis grid products, like CMPAS-V2.1融合降水分析实时数据产品（NC）.

    :param times_str: analysis time string list, like ["20200208000000", "20200209000000"], format: YYYYMMDDHHMISS
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_by_time function.
    :return: data, xarray type

    Examples:
    >>> times = ["20200208000000", "20200209000000"]
    >>> data_code = "SURF_CMPA_FRT_5KM"
    >>> data = cimiss_obs_grid_by_times(times, data_code=data_code, fcst_ele="PRE")
    """

    dataset = []
    tqdm_times_str = tqdm(times_str, desc=kargs['data_code'] + ": ") if pbar else times_str
    for time_str in tqdm_times_str:
        data = cimiss_obs_grid_by_time(time_str, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(kargs['data_code']+'/'+time_str))
                return None
    
    return xr.concat(dataset, dim='time')


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
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # return
    return contents


def cimiss_analysis_by_time(time_str, limit=None, data_code='NAFP_CLDAS2.0_RT_GRB',
                            levattrs={'long_name':'Height above Ground', 'units':'m'},
                            fcst_level=None, fcst_ele="TEF2", zoom=None, units=None, scale_off=None,
                            cache=True, cache_clear=True):
    """
    Retrieve CLDAS analysis data from CIMISS service.

    :param time_str: analysis time, like "20160817120000", format: YYYYMMDDHHMISS
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param data_code: MUSIC data code, default is "NAFP_CLDAS2.0_RT_GRB"
    :param fcst_level: vertical level, default is 2.
    :param fcst_ele: forecast element, default is 2m temperature "TEF2"
    :param zoom: the zoom out integer > 1, like 2.
    :param units: forecast element's units, defaults to retrieved units.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param cache: cache retrieved data to local directory, default is True.
    :return: xarray dataset.

    Examples:
    >>> data = cimiss_analysis_by_time("20200215120000", data_code="NAFP_CLDAS2.0_RT_GRB", 
                                        fcst_level=2, fcst_ele='TEF2', units="C", scale_off=[1.0, -273.15])
    """

    # retrieve data from cached file
    if cache:
        directory = os.path.join(data_code, fcst_ele, str(fcst_level))
        filename = time_str
        if limit is not None:
            filename = filename + '.' + str(limit).replace(" ","")
        cache_file = CONFIG.get_cache_file(directory, filename, name="CIMISS", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # set retrieve parameters
    if limit is None:
        params = {'dataCode': data_code,
                  'time': time_str,
                  'fcstEle': fcst_ele}
        if zoom is not None: params['zoomOut'] = str(zoom)
        interface_id = 'getNafpAnaEleGridByTimeAndLevel'
    else:
        params = {'dataCode': data_code,
                  'time': time_str,
                  'minLat': '{:.10f}'.format(limit[0]),
                  "minLon": '{:.10f}'.format(limit[1]),
                  "maxLat": '{:.10f}'.format(limit[2]),
                  "maxLon": '{:.10f}'.format(limit[3]),
                  'fcstEle': fcst_ele}
        interface_id = 'getNafpAnaEleGridInRectByTimeAndLevel'

    # add forecast level parameters
    if fcst_level is not None:
        params['fcstLevel'] = '{:d}'.format(fcst_level)

    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # get time information
    time = datetime.strptime(time_str, '%Y%m%d%H%M%S')
    time = np.array([time], dtype='datetime64[ms]')

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
    if units is None:
        units = contents['fieldUnits']

    # define coordinates and variables
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east',
        '_CoordinateAxisType':'Lon', 'axis':'X'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north',
        '_CoordinateAxisType':'Lat', 'axis':'Y'})
    if fcst_level != 0:
        level_coord = ('level', np.array([fcst_level]), levattrs)
    varname = fcst_ele
    varattrs = {'long_name': name, 'units': units}

    # construct xarray
    data = np.array(contents['DS'], dtype=np.float32)
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]
    if fcst_level == 0:
        data = data[np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
    else:
        data = data[np.newaxis, np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'level', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add attributes
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = 'CIMISS Server by MUSIC API'

    # cache data
    if cache:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # return data
    return data


def cimiss_analysis_by_times(times_str, pbar=True, allExists=True, **kargs):
    """
    Retrieve multiple CLDAS analysis data from CIMISS service.

    :param times_str: analysis time string list, like ["20200208000000", "20200209000000"], format: YYYYMMDDHHMISS
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_by_time function.
    :return: data, xarray type

    Examples:
    >>> times = ["20200208000000", "20200209000000"]
    >>> data_code = "NAFP_CLDAS2.0_RT_GRB"
    >>> data = cimiss_analysis_by_times(times, data_code=data_code)
    """

    dataset = []
    if pbar:
        tqdm_times_str = tqdm(times_str, desc=kargs['data_code'] + ": ")
    for time_str in tqdm_times_str:
        data = cimiss_analysis_by_time(time_str, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(kargs['data_code']+'/'+time_str))
                return None
    
    return xr.concat(dataset, dim='time')


def cimiss_model_grid(data_code, init_time_str, valid_time, fcst_ele, fcst_level, limit=None,
                      varname='data', units=None, scale_off=None, cache=True, cache_clear=True,
                      levattrs={'long_name':'height_above_ground', 'units':'m', '_CoordinateAxisType':'Height'}):
    """
    Retrieve model grid data from CIMISS service.
    refer to: http://10.20.76.55/cimissapiweb/apidataclassdefine_list.action

    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_time: forecast hour, like 0
    :param fcst_ele: forecast element, like 2m temperature "TEF2"
    :param fcst_level: vertical level, like 0
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param varname: set variable name, default is 'data'
    :param units: forecast element's units, defaults to retrieved units.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param cache: cache retrieved data to local directory, default is True.
    :param levattrs: level attributes, like:
                     {'long_name':'height_above_ground', 'units':'m', '_CoordinateAxisType':'Height'}, default
                     {'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'}
                     {'long_name':'geopotential_height', 'units':'gpm', '_CoordinateAxisType':'GeoZ'}
                     refer to https://www.unidata.ucar.edu/software/netcdf-java/current/reference/CoordinateAttributes.html
    :return: xarray dataset.

    Examples:
    >>> data = cimiss_model_grid("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", 24, 'TEM', 850, 1, units="C", scale_off=[1.0, -273.15], 
                                 levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    # retrieve data from cached file
    if cache:
        directory = os.path.join(data_code, fcst_ele, str(fcst_level))
        filename = init_time_str + '.' + str(valid_time).zfill(3)
        if limit is not None:
            filename = init_time_str + '_' +str(limit).replace(" ","") +'.' + str(valid_time).zfill(3)
        cache_file = CONFIG.get_cache_file(directory, filename, name="CIMISS", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

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
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # get time information
    init_time = datetime.strptime(init_time_str, '%Y%m%d%H')
    fhour = np.array([valid_time], dtype=np.float)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

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
    if units is None:
        units = contents['fieldUnits']

    # define coordinates and variables
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east',
        '_CoordinateAxisType':'Lon', 'axis':'X'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north',
        '_CoordinateAxisType':'Lat', 'axis':'Y'})
    if fcst_level != 0:
        level_coord = ('level', np.array([fcst_level]), levattrs)
    varattrs = {'short_name': fcst_ele, 'long_name': name, 'units': units}

    # construct xarray
    data = np.array(contents['DS'], dtype=np.float32)
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]
    if fcst_level == 0:
        data = data[np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
    else:
        data = data[np.newaxis, np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'level', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add time coordinates
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name':'forecast_period', 'units':'hour'})

    # add attributes
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = 'CIMISS Server by MUSIC API'

    # cache data
    if cache:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # return data
    return data


def cimiss_model_grids(data_code, init_time_str, valid_times, fcst_ele, fcst_level, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple valid time grids at the same initial time from CIMISS service.
    
    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_times: forecast hours, like [0, 6, 12, 15, 18, ...]
    :param fcst_ele: forecast element, like 2m temperature "TEF2"
    :param fcst_level: vertical level, like 0
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_grid function.

    Examples:
    >>> valid_times = [6*i for i in 0:13]
    >>> data = cimiss_model_grids("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", valid_times, 'TEM', 850, units="C", scale_off=[1.0, -273.15], 
                                 levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    dataset = []
    if pbar:
        tqdm_valid_times = tqdm(valid_times, desc=data_code + ": ")
    else:
        tqdm_valid_times = valid_times
    for valid_time in tqdm_valid_times:
        data = cimiss_model_grid(data_code, init_time_str, valid_time, fcst_ele, fcst_level, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(data_code+'/'+init_time_str+'.'+str(valid_time).zfill(3)))
                return None
    
    return xr.concat(dataset, dim='time')


def cimiss_model_points(data_code, init_time_str, valid_times, fcst_ele, fcst_level, points, **kargs):
    """
    Retrieve model point time series at the same initial time from CIMISS service.
    
    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_times: forecast hours, like [0, 6, 12, 15, 18, ...]
    :param fcst_ele: forecast element, like 2m temperature "TEF2", temperature "TEM"
    :param fcst_level: vertical level, like 0
    :param points: dictionary, {'lon':[...], 'lat':[...]}.
    :param **kargs: key arguments passed to cimiss_model_grids function.

    Examples:
    >>> valid_times = [6*i for i in 0:13]
    >>> points = {'lon':[116.3833, 110.0], 'lat':[39.9, 32]}
    >>> data = cimiss_model_points("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", valid_times, 'TEM', 850, points, units="C", scale_off=[1.0, -273.15], 
                                   levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    data = cimiss_model_grids(data_code, init_time_str, valid_times, fcst_ele, **kargs)
    if data:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    else:
        return None


def cimiss_model_3D_grid(data_code, init_time_str, valid_time, fcst_ele, fcst_levels, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple level grids at the same initial time from CIMISS service.
    
    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_time: forecast hour, like 0
    :param fcst_ele: forecast element, like 2m temperature "TEF2", temperature "TEM"
    :param fcst_levels: vertical levels, like [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_grid function.

    Examples:
    >>> levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    >>> data = cimiss_model_3D_grid("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", 24, 'TEM', levels, units="C", scale_off=[1.0, -273.15], 
                                    levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    dataset = []
    if pbar:
        tqdm_fcst_levels = tqdm(fcst_levels, desc=data_code + ": ")
    else:
        tqdm_fcst_levels = fcst_levels
    for fcst_level in tqdm_fcst_levels:
        data = cimiss_model_grid(data_code, init_time_str, valid_time, fcst_ele, fcst_level, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(data_code+'/'+init_time_str+'.'+str(valid_time).zfill(3)))
                return None
    
    return xr.concat(dataset, dim='level')


def cimiss_model_3D_grids(data_code, init_time_str, valid_times, fcst_ele, fcst_levels, allExists=True, pbar=False, **kargs):
    """
    Retrieve multiple time and level grids at the same initial time from CIMISS service.
    
    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_times: forecast hour, like  [0, 6, 12, 15, 18, ...]
    :param fcst_ele: forecast element, like 2m temperature "TEF2", temperature "TEM"
    :param fcst_levels: vertical levels, like [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_grid function.

    Examples:
    >>> valid_times = [6*i for i in range(13)]
    >>> levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    >>> data = cimiss_model_3D_grids("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", 24, 'TEM', levels, units="C", scale_off=[1.0, -273.15], 
                                     levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    dataset = []
    if pbar:
        tqdm_valid_times = tqdm(valid_times, desc=data_code + ": ")
    else:
        tqdm_valid_times = valid_times
    for valid_time in tqdm_valid_times:
        dataset_temp = []
        for fcst_level in fcst_levels:
            data = cimiss_model_grid(data_code, init_time_str, valid_time, fcst_ele, fcst_level, **kargs)
            if data:
                dataset_temp.append(data)
            else:
                if allExists:
                    warnings.warn("{} doese not exists.".format(data_code+'/'+init_time_str+'.'+str(valid_time).zfill(3)))
                    return None
        dataset.append(xr.concat(dataset_temp, dim='level'))
    
    return xr.concat(dataset, dim='time')


def cimiss_model_profiles(data_code, init_time_str, valid_times, fcst_ele, fcst_levels, points, **kargs):
    """
    Retrieve time series of vertical profile from 3D [time, level, lat, lon] grids
    at the same initial time from CIMISS service.
    
    :param data_code: MUSIC data code, 
                      "NAFP_FOR_FTM_HIGH_EC_GLB"(default): 欧洲中心数值预报产品-高分辨率C1D-全球
                      "NAFP_FOR_FTM_HIGH_EC_ASI": 欧洲中心数值预报产品-高分辨率C1D-亚洲地区
                      "NAFP_FOR_FTM_HIGH_EC_ANEA": 欧洲中心数值预报产品-高分辨率C1D-东北亚地区
                      ......
    :param init_time_str: model run time, like "2016081712"
    :param valid_times: forecast hour, like  [0, 6, 12, 15, 18, ...]
    :param fcst_ele: forecast element, like 2m temperature "TEF2", temperature "TEM"
    :param fcst_levels: vertical levels, like [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    :param point: dictionary, {'lon':[...], 'lat':[...]}.
    :param **kargs: key arguments passed to cimiss_model_grid function.

    Examples:
    >>> valid_times = [6*i for i in range(13)]
    >>> levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 100]
    >>> points = {'lon':[116.3833, 110.0], 'lat':[39.9, 32]}
    >>> data = cimiss_model_profiles("NAFP_FOR_FTM_HIGH_EC_ANEA", "2020021512", 24, 'TEM', levels, units="C", points, scale_off=[1.0, -273.15], 
                                     levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'})
    """

    data = cimiss_model_3D_grids(data_code, init_time_str, valid_times, fcst_ele, fcst_levels, **kargs)
    if data:
        return data.interp(lon=('points', points['lon']), lat=('points', points['lat']))
    else:
        return None


def cimiss_model_by_time(init_time_str, valid_time=0, limit=None,
                         data_code='NAFP_FOR_FTM_HIGH_EC_GLB',
                         levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'},
                         fcst_level=0, fcst_ele="TEF2", varname='data', units=None, scale_off=None,
                         cache=True, cache_clear=True):
    """
    Retrieve grid data from CIMISS service.

    :param init_time_str: model run time, like "2016081712"
    :param valid_time: forecast hour, default is 0
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param varname: set variable name, default is 'data'
    :param data_code: MUSIC data code, default is "NAFP_FOR_FTM_HIGH_EC_GLB"
    :param fcst_level: vertical level, default is 0.
    :param fcst_ele: forecast element, default is 2m temperature "TEF2"
    :param units: forecast element's units, defaults to retrieved units.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param cache: cache retrieved data to local directory, default is True.
    :return: xarray dataset.

    Examples:
    >>> data = cimiss_model_by_time("2020021512", data_code="NAFP_FOR_FTM_HIGH_EC_ANEA", 
                                    fcst_level=850, fcst_ele='TEM', units="C", scale_off=[1.0, -273.15])
    """

    # retrieve data from cached file
    if cache:
        directory = os.path.join(data_code, fcst_ele, str(fcst_level))
        filename = init_time_str + '.' + str(valid_time).zfill(3)
        if limit is not None:
            filename = init_time_str + '_' +str(limit).replace(" ","") +'.' + str(valid_time).zfill(3)
        cache_file = CONFIG.get_cache_file(directory, filename, name="CIMISS", cache_clear=cache_clear)
        if cache_file.is_file():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

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
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # get time information
    init_time = datetime.strptime(init_time_str, '%Y%m%d%H')
    fhour = np.array([valid_time], dtype=np.float)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

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
    if units is None:
        units = contents['fieldUnits']

    # define coordinates and variables
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east',
        '_CoordinateAxisType':'Lon', 'axis':'X'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north',
        '_CoordinateAxisType':'Lat', 'axis':'Y'})
    if fcst_level != 0:
        level_coord = ('level', np.array([fcst_level]), levattrs)
    varattrs = {'long_name': name, 'units': units}

    # construct xarray
    data = np.array(contents['DS'], dtype=np.float32)
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]
    if fcst_level == 0:
        data = data[np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
    else:
        data = data[np.newaxis, np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'level', 'lat', 'lon'], data, varattrs)},
            coords={'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add time coordinates
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name':'forecast_period', 'units':'hour'})

    # add attributes
    data.attrs['Conventions'] = "CF-1.6"
    data.attrs['Origin'] = 'CIMISS Server by MUSIC API'

    # cache data
    if cache:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # return data
    return data


def cimiss_model_by_times(init_time_str, valid_times=np.arange(0, 75, 6), pbar=True, allExists=True, **kargs):
    """
    Retrieve multiple model grids from CIMISS service.

    :param init_time_str: model run time, like "2016081712"
    :param limit: [min_lat, min_lon, max_lat, max_lon]
    :param valid_times: forecast hours, default is [0, 6, 12, ..., 72]
    :param allExists (boolean): all files should exist, or return None.
    :param pbar (boolean): Show progress bar, default to False.
    :param **kargs: key arguments passed to cimiss_model_by_time function.
    :return: xarray dataset.

    Examples:
    >>> data = cimiss_model_by_times("2020021512", data_code="NAFP_FOR_FTM_HIGH_EC_ANEA", time_range=[0, 72], 
                                     fcst_level=850, fcst_ele='TEM', units="C", scale_off=[1.0, -273.15])
    """

    dataset = []
    if pbar:
        tqdm_valid_times = tqdm(valid_times, desc=kargs['data_code'] + ": ")
    for valid_time in tqdm_valid_times:
        data = cimiss_model_by_time(init_time_str, valid_time=valid_time, **kargs)
        if data:
            dataset.append(data)
        else:
            if allExists:
                warnings.warn("{} doese not exists.".format(kargs['data_code']+'/'+init_time_str))
                return None
    
    return xr.concat(dataset, dim='time')


def cimiss_model_by_piont(init_time_str,
                          data_code='NAFP_FOR_FTM_HIGH_EC_ANEA',
                          fcst_level=850, time_range=[0, 72], 
                          points="39.90/116.40", fcst_ele="TEM"):
    """
    Retrieve grid point data from CIMISS service.

    :param init_time_str: model run time, like "2020020600"
    :param data_code: MUSIC data code, default is "NAFP_FOR_FTM_HIGH_EC_ANEA"
    :param fcst_level: vertical level, default is 850.
    :param time_range: [minimum, maximum] forecast hour, default is [0, 72]
    :param points: point location "latitude/longitude", also support
                   multiple points like "39.90/116.40,32.90/112.40"
    :param fcst_ele: forecast element, default is temperature "TEM"
    :return: pandas dataframe
    """

    # set retrieve parameters
    params = {'dataCode': data_code,
              'time': init_time_str + '0000',
              'fcstLevel': '{:d}'.format(fcst_level),
              'minVT': '{:d}'.format(time_range[0]),
              'maxVT': '{:d}'.format(time_range[1]),
              'latLons': points,
              'fcstEle': fcst_ele}
    interface_id = 'getNafpEleAtPointByTimeAndLevelAndValidtimeRange'


    # retrieve data contents
    contents = get_http_result(interface_id, params)
    if contents is None:
        return None
    contents = json.loads(contents.decode('utf-8'), strict=False)
    if contents['returnCode'] != '0':
        return None

    # convert to numeric
    data = pd.DataFrame(contents['DS'])
    data['Lat'] = pd.to_numeric(data['Lat'])
    data['Lon'] = pd.to_numeric(data['Lon'])
    data['Validtime'] = pd.to_datetime(data['Validtime'], format="%Y%m%d%H%M%S")
    data[fcst_ele] = pd.to_numeric(data[fcst_ele])

    return data


def cimiss_model_by_piont_levels(init_time_str,
                                 data_code='NAFP_FOR_FTM_HIGH_EC_ANEA',
                                 fcst_levels=[1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200],
                                 time_range=[0, 72], 
                                 point="39.90/116.40", fcst_ele="TEM"):
    """
    Retrieve grid point data from CIMISS service.

    :param init_time_str: model run time, like "2020020600"
    :param data_code: MUSIC data code, default is "NAFP_FOR_FTM_HIGH_EC_ANEA"
    :param fcst_levels: vertical levels, list like [1000, 950, 925, ...]
    :param time_range: [minimum, maximum] forecast hour, default is [0, 72]
    :param point: point location "latitude/longitude"
    :param fcst_ele: forecast element, default is temperature "TEM"
    :return: pandas dataframe
    """

    # loop every level
    data = None
    for fcst_level in fcst_levels:
        temp = cimiss_model_by_piont(
            init_time_str, data_code=data_code, fcst_level=fcst_level,
            time_range=time_range, points=point, fcst_ele=fcst_ele)
        if temp is None:
            return None
        
        temp['level'] = fcst_level
        if data is None:
            data = temp
        else:
            data = pd.concat([data, temp])

    data = data.pivot(index='Validtime', columns='level',values=fcst_ele)
    data = xr.DataArray(data, coords=[data.index.values, data.columns.values],
                        dims=['time', 'level'], name=fcst_ele)
    data = data.loc[{'level':sorted(data.coords['level'].values, reverse=True)}]
    data = data.loc[{'time':sorted(data.coords['time'].values)}]

    return data
