# -*- coding: utf-8 -*-

# Copyright (c) 2021 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve historical data from CMADaSS service.
"""

import os
import calendar
import time
import urllib.request
import numpy as np
import pandas as pd
from tqdm import tqdm
from nmc_met_io.retrieve_cmadaas import cmadaas_obs_by_time_range_and_id


def get_hist_obs_id(years=np.arange(2000, 2011, 1), 
                    data_code='SURF_CHN_MUL_HOR_N', 
                    elements=None, sta_ids="54511"):
    """
    Retrieve hitory observations for sta_ids.
    从大数据云平台上获取指定站点的地面观测数据. 由于大数据云平台对一次性检索有数量限制,
    因此先逐年下载, 然后再联接成一张观测记录表.

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