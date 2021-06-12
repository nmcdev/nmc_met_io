# _*_ coding: utf-8 _*_

# Copyright (c) 2020 NMC Developers.
# Distributed under the terms of the GPL V3 License.

import json
import requests
import pandas as pd
from datetime import datetime
from pandas.io.json._normalize import nested_to_record
import nmc_met_io.config as CONFIG


def get_caiy_weather(lon=116.4667, lat=39.8, begin_time=None):
    """
    retrieve weather hourly and daily forecast data from caiyun api.
    refer to https://open.caiyunapp.com/%E5%BD%A9%E4%BA%91%E5%A4%A9%E6%B0%94_API/v2.5
    
    Args:
        lon (float, optional): city longitude
        lat (float, optional): city latitude
        begin_time (datetime, optional): begin time, should not extend one day before current time.
                                         Like datetime(2020, 3, 6, 8), Defaults to None.

    Returns:
       dictionary contains two DataFrame: {"hourly", "daily", "info"}.
    """

    # construct url
    url = 'https://api.caiyunapp.com/v2.5/{}/{:.4f},{:.4f}/weather.json'.format(
        CONFIG.CONFIG['CAIY']['token'], lon, lat)
    if begin_time is not None:
        url = url + '?begin='+str(int(begin_time.timestamp()))

    # retrieve the weather forecast information
    info = requests.get(url)
    info = json.loads(info.text, strict=False)
    if info['status'] == 'failed':
        print("Can not retrieve the weather information, please check input parameters.")
        return None

    # extract hourly information
    record = pd.DataFrame(info['result']['hourly']['precipitation'])
    record.rename(columns={"value":"precipitation"}, inplace=True)
    data = record

    record = pd.DataFrame(info['result']['hourly']['temperature'])
    record.rename(columns={"value":"temperature"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['wind'])
    record.rename(columns={"value":"wind"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['humidity'])
    record.rename(columns={"value":"humidity"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['cloudrate'])
    record.rename(columns={"value":"cloudrate"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['skycon'])
    record.rename(columns={"value":"skycon"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['pressure'])
    record.rename(columns={"value":"pressure"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['visibility'])
    record.rename(columns={"value":"visibility"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['dswrf'])
    record.rename(columns={"value":"dswrf"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(nested_to_record(info['result']['hourly']['air_quality']['aqi']))
    record = record.drop(['value.usa'], axis=1)
    record.rename(columns={"value.chn":"air_quality"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    record = pd.DataFrame(info['result']['hourly']['air_quality']['pm25'])
    record.rename(columns={"value":"pm25"}, inplace=True)
    data = pd.merge(data, record, on='datetime')

    data['datetime'] = pd.to_datetime(data['datetime']).dt.tz_convert('Asia/Shanghai')

    # extract daily information
    record = pd.DataFrame(nested_to_record(info['result']['daily']['astro']))
    data_daily = record

    record = pd.DataFrame(info['result']['daily']['precipitation'])
    record = record.drop(['min'], axis=1)
    record['avg'] = record['avg'] * 24
    record.rename(columns={"max":"precipitation_max", "avg":"precipitation_24h"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['temperature'])
    record.rename(columns={"max":"temperature_max", "min":"temperature_min", "avg":"temperature_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(nested_to_record(info['result']['daily']['wind']))
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['humidity'])
    record.rename(columns={"max":"humidity_max", "min":"humidity_min", "avg":"humidity_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['cloudrate'])
    record.rename(columns={"max":"cloudrate_max", "min":"cloudrate_min", "avg":"cloudrate_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['pressure'])
    record.rename(columns={"max":"pressure_max", "min":"pressure_min", "avg":"pressure_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['visibility'])
    record.rename(columns={"max":"visibility_max", "min":"visibility_min", "avg":"visibility_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['dswrf'])
    record.rename(columns={"max":"dswrf_max", "min":"dswrf_min", "avg":"dswrf_avg"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(nested_to_record(info['result']['daily']['air_quality']['aqi']))
    record = record.add_prefix('aqi.')
    record.rename(columns={"aqi.date":"date"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(nested_to_record(info['result']['daily']['air_quality']['pm25']))
    record = record.add_prefix('pm25.')
    record.rename(columns={"pm25.date":"date"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['skycon'])
    record.rename(columns={"value":"skycon"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['skycon_08h_20h'])
    record.rename(columns={"value":"skycon_08h_20h"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['skycon_20h_32h'])
    record.rename(columns={"value":"skycon_20h_32h"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['life_index']['ultraviolet'])
    record.rename(columns={"index":"ultraviolet.index", "desc":"ultraviolet.desc"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['life_index']['carWashing'])
    record.rename(columns={"index":"carWashing.index", "desc":"carWashing.desc"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['life_index']['dressing'])
    record.rename(columns={"index":"dressing.index", "desc":"dressing.desc"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['life_index']['comfort'])
    record.rename(columns={"index":"comfort.index", "desc":"comfort.desc"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    record = pd.DataFrame(info['result']['daily']['life_index']['coldRisk'])
    record.rename(columns={"index":"coldRisk.index", "desc":"coldRisk.desc"}, inplace=True)
    data_daily = pd.merge(data_daily, record, on='date')

    data_daily['date'] = pd.to_datetime(data_daily['date']).dt.tz_convert('Asia/Shanghai')

    return {"hourly":data, "daily":data_daily, "info":info}
 