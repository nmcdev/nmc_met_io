# -*- coding: utf-8 -*-

# Copyright (c) 2021 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve historical data from CMADaSS service.
"""

import numpy as np
import pandas as pd
from tqdm import tqdm
from nmc_met_io.retrieve_cmadaas import (cmadaas_obs_by_time_range_and_id,
                                         cmadaas_obs_by_time_range,
                                         cmadaas_obs_in_rect_by_time_range)
from nmc_met_io.util import get_sub_stations


def get_hist_obs_id(years=np.arange(2000, 2011, 1), 
                    data_code='SURF_CHN_MUL_HOR_N', 
                    elements=None, sta_ids="54511"):
    """
    Retrieve hitory observations for sta_ids.
    从CMADaaS上获取指定站点的地面观测数据. 由于大数据云平台对一次性检索有数量限制,
    因此先逐年下载, 然后再联接成一张观测记录表.

    注:
    * 对于日值数据“SURF_CHN_MUL_DAY”, PRE_Time_0808表示当天08至次日08, PRE_Time_2020表示昨日20至当日20时.

    Args:
        years (np.array, optional): years for historical data. Defaults to np.arange(2000, 2011, 1).
        data_code (str, optional): dataset code. Defaults to 'SURF_CHN_MUL_DAY'.
        elements ([type], optional): elements for retrieve, 'ele1, ele2, ...'. Defaults to None.
        sta_ids (str, optional): station ids. Defaults to "54511".

    Returns:
        dataframe: station obervation records.
    """
    # check elements
    if elements is None:
        elements = 'Station_Id_d,Datetime,Lat,Lon,Alti,TEM,DPT,RHU,PRE_1h,PRE_6h,PRE_24h'

    # loop every yeas
    data_list = []
    tqdm_years = tqdm(years, desc="Years: ")
    for year in tqdm_years:
        start_time = str(year) + '0101000000'
        end_time = str(year) + '1231230000'
        time_range = "[" + start_time + "," + end_time + "]"
        df = cmadaas_obs_by_time_range_and_id(
            time_range, data_code=data_code, elements=elements,
            sta_ids=sta_ids, trans_type=True)
        if df is not None:
            df = df.drop_duplicates()
            data_list.append(df)
    
    # concentrate dataframes
    if len(data_list) == 0:
        return None
    else:
        return pd.concat(data_list, axis=0, ignore_index=True)
    

def get_accumulated_rainfall(time_range, data_code="SURF_CHN_MUL_HOR", 
                             accumulated=True, limit=None):
    """
    从CMADaaS上“中国地面逐小时资料”下载站点的逐小时降水观测, 并累加为一段时间的累积降水.
    例如, 需要24h的累积降水, 用站点观测的PRE_24h有误差, 需要直接用PRE_1h来进行累积.

    Args:
        time_range (str): 观测的时间区间，格式为 "[20210719010000,20210720000000]"
        data_code (str, optional): 资料代码, 默认为所有自动站观测, 如果只用国家站，设为"SURF_CHN_MUL_HOR_N".
        accumulated (bool, optional): 是否计算每个站的累积降水. Defaults to True.
        limit (tuple, optional): 指定返回数据的范围, [min_lat, min_lon, max_lat, max_lon]
    """
    
    # 读入数据
    elements = "Station_Name,Province,Station_Id_C,Lon,Lat,Datetime,PRE_1h"
    if limit is None:
        df = cmadaas_obs_by_time_range(time_range, data_code=data_code, elements=elements)
    else:
        df = cmadaas_obs_in_rect_by_time_range(time_range, limit, data_code=data_code, elements=elements)
        
    # 计算24h累积降水量
    if accumulated:
        df = df.groupby(by=['Station_Id_C','Lon','Lat','Station_Name','Province']).sum()
        df.reset_index(inplace=True)
        df.rename(columns={"PRE_1h":"PRE"}, inplace=True)
        
    # 返回计算值
    return df


def get_hist_obs_daily(years=np.arange(2000, 2011, 1), 
                       data_code='SURF_CHN_MUL_DAY_N', 
                       elements=None, limit=None,
                       sta_levels="011,012,013"):
    """
    从CMADaaS上逐日的观测数据.

    Args:
        years (np.array, optional): years for historical data. Defaults to np.arange(2000, 2011, 1).
        data_code (str, optional): dataset code. Defaults to 'SURF_CHN_MUL_DAY_N'.
        elements ([type], optional): elements for retrieve, 'ele1, ele2, ...'. Defaults to None.
        limit (tuple, optional): 指定返回数据的范围, [min_lat, min_lon, max_lat, max_lon]
        sta_levels(str, optional): 指定返回的站点级别, 默认"011,012,013", 即国家基准气候站, 国家基本气象站, 国家一般气象站
                                   若设为None值, 则返回全部级别的站点. 如果设置了limit, 该参数则不起作用.
    Returns:
        dataframe: station obervation records.
    """
    
    # check elements
    if elements is None:
        elements = 'Station_Id_d,Station_levl,Station_Name,Province,Lat,Lon,Alti,Datetime,PRE_Time_0808,Q_PRE_Time_0808'
        
    # loop every yeas
    data_list = []
    tqdm_years = tqdm(years, desc="Years: ")
    for year in tqdm_years:
        start_time = str(year) + '0101000000'
        end_time = str(year) + '1231230000'
        time_range = "[" + start_time + "," + end_time + "]"
        if limit is None:
            df = cmadaas_obs_by_time_range(
                time_range, data_code=data_code, elements=elements, 
                sta_levels=sta_levels)
        else:
            df = cmadaas_obs_in_rect_by_time_range(
                time_range, limit, data_code=data_code, elements=elements)
        if df is not None:
            df = df.drop_duplicates()
            data_list.append(df)
    
    # concentrate dataframes
    if len(data_list) == 0:
        return None
    else:
        return pd.concat(data_list, axis=0, ignore_index=True)
    
