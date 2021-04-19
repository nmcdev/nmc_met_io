# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve historical data from CIMISS service.
"""

import os
import calendar
import time
import urllib.request
import numpy as np
import pandas as pd
from tqdm import tqdm
from nmc_met_io.retrieve_cimiss_server import cimiss_obs_by_time_range
from nmc_met_io.retrieve_cimiss_server import cimiss_obs_in_rect_by_time_range
from nmc_met_io.retrieve_cimiss_server import cimiss_obs_file_by_time_range
from nmc_met_io.retrieve_cimiss_server import cimiss_obs_by_time_range_and_id


def get_hist_obs(years=np.arange(2000, 2011, 1), month_range=(1, 12),
                 data_code="SURF_CHN_MUL_DAY", elements=None, sta_levels=None,
                 outfname='day_rain_obs', outdir='.'):
    """
    Download historical daily observations and write to data files,
    each month a file.

    :param years: years for historical data
    :param month_range: month range each year, like (1, 12)
    :param elements: elements for retrieve, 'ele1, ele2, ...'
    :param sta_levels: station levels
    :param outfname: output file name + '_year' + '_month'
    :param outdir: output file directory
    :return: output file names.

    :Example:
    >>> get_day_hist_obs(years=np.arange(2000, 2016, 1), outdir="D:/")

    """

    # check elements
    if elements is None:
        elements = "Station_Id_C,Station_Name,Datetime,Lat,Lon,PRE_Time_0808"

    # check output directory
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # define months
    months = np.arange(1, 13, 1)

    # Because of the CIMISS data mount limit,
    # so loop every year to download the data.
    out_files = []
    for iy in years:
        if calendar.isleap(iy):
            last_day = ['31', '29', '31', '30', '31', '30',
                        '31', '31', '30', '31', '30', '31']
        else:
            last_day = ['31', '28', '31', '30', '31', '30',
                        '31', '31', '30', '31', '30', '31']

        for i, im in enumerate(months):
            # check month range
            if not (month_range[0] <= im <= month_range[1]):
                continue

            month = '%02d' % im
            start_time = str(iy) + month + '01' + '000000'
            end_time = str(iy) + month + last_day[i] + '230000'
            time_range = "[" + start_time + "," + end_time + "]"

            # retrieve observations from CIMISS server
            data = cimiss_obs_by_time_range(
                time_range, sta_levels=sta_levels,
                data_code=data_code, elements=elements)
            if data is None:
                continue

            # save observation data to file
            out_files.append(os.path.join(
                outdir, outfname + "_" + str(iy) + "_" + month + ".pkl"))
            data.to_pickle(out_files[-1])

    return out_files


def get_hist_obs_id(years=np.arange(2000, 2011, 1), 
                        data_code='SURF_CHN_MUL_DAY', 
                        elements=None, sta_ids="54511"):
    """
    Retrieve hitory observations for sta_ids.

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
        elements = 'Station_Id_d,Datetime,Lat,Lon,Alti,TEM_Max,TEM_Min,PRE_Time_0808'

    # loop every yeas
    data_list = []
    tqdm_years = tqdm(years, desc="Years: ")
    for year in tqdm_years:
        start_time = str(year) + '0101000000'
        end_time = str(year) + '1231230000'
        time_range = "[" + start_time + "," + end_time + "]"
        df = cimiss_obs_by_time_range_and_id(
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


def get_mon_hist_obs(years=np.arange(2000, 2011, 1),
                     limit=(3, 73, 54, 136),
                     elements=None,
                     outfname='mon_surface_obs',
                     outdir='.'):
    """
    Download historical monthly observations and write to data files,
    each year a file.

    :param years: years for historical data
    :param limit: spatial limit [min_lat, min_lon, max_lat, max_lon]
    :param elements: elements for retrieve, 'ele1, ele2, ...'
    :param outfname: output file name + 'year'
    :param outdir: output file directory
    :return: Output filenames
    """

    # check elements
    if elements is None:
        elements = ("Station_Id_C,Station_Name,Year,"
                    "Mon,Lat,Lon,Alti,PRE_Time_0808")

    # check output directory
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Loop every year to download the data.
    out_files = []
    for iy in years:
        # check out file
        outfile = os.path.join(outdir, outfname + "_" + str(iy) + ".pkl")
        if os.path.isfile(outfile):
            continue

        # set time range
        start_time = str(iy) + '0101' + '000000'
        end_time = str(iy) + '1201' + '000000'
        time_range = "[" + start_time + "," + end_time + "]"

        # retrieve observations from CIMISS server
        data = cimiss_obs_in_rect_by_time_range(
            time_range, limit, data_code='SURF_CHN_MUL_MON',
            elements=elements)
        if data is None:
            continue

        # save observation data to file
        out_files.append(outfile)
        data.to_pickle(out_files[-1])

    return out_files


def get_cmpas_hist_files(time_range, outdir='.', resolution=None):
    """
    Download CMAPS QPE gridded data files.
    注: CIMISS对于下载访问次数进行了访问限制, 最好使用cmadaas_get_obs_files.
    
    Arguments:
        time_range {string} -- time range for retrieve,
                              "[YYYYMMDDHHMISS,YYYYMMDDHHMISS]"
        outdir {string} -- output directory.
        resolution {string} -- data resolution, 0P01 or 0P05

    :Exampels:
    >>> time_range = "[20180101000000,20180331230000]"
    >>> get_cmpas_hist_files(time_range, outdir='G:/CMAPS', resolution='0P05')
    """

    # check output directory
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    files = cimiss_obs_file_by_time_range(
        time_range, data_code="SURF_CMPA_NRT_NC")
    filenames = files['DS']
    for file in filenames:
        if resolution is not None:
            if not resolution in file['FILE_NAME']:
                continue
        outfile = os.path.join(outdir, file['FILE_NAME'])
        if not os.path.isfile(outfile):
            # 服务器对短时间内访问次数进行了限制,
            # 相应策略是出现下载错误时, 等待10秒钟后重新下载
            try:
                time.sleep(2)
                urllib.request.urlretrieve(file['FILE_URL'], outfile)
            except:
                time.sleep(60)
                urllib.request.urlretrieve(file['FILE_URL'], outfile)
