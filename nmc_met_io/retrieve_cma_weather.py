# _*_ coding: utf-8 _*_

# Copyright (c) 2020 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve current weather from CMA restful API.
"""

from datetime import datetime
import urllib3
import hashlib
import json
import pandas as pd


def get_current_weather(lon, lat, apikey, pwd, elements=None, url_only=False):
    """
    通过基于位置的天气实况服务接口（简称“位置服务接口”）调用实况天气.
    位置服务接口支持两种应用场景：“点”的应用场景和“线”的应用场景。
    根据点或线的经纬度信息，获取气温、相对湿度、风速、风向、天气现象、能见度
    总云量、海表温度以及降水等气象要素数据。
    2021/02/20, revist by Guo Yunqian.

    在中国气象数据网（http://data.cma.cn）上申请API账户，审核通过后
    获得API账户, 加上用户注册密码即可获得数据.

    Args:
        lon (float or list): longitudes
        lat (float or list): latitudes
        apikey (str): apikey.
        pwd (str): user password.
        elements (str, optional): weather elements. Defaults to None.
        url_only: only retur url string.
    
    """

    # construct parameters
    params = {}

    # weather elements
    if elements is None:
        params['elements'] = 'TEM,RHU,WINS,WIND,WEA,VIS,TCDC,SST,PRE_1H,PRE_3H,PRE_6H,PRE_12H,PRE_24H'
    else:
        params['elements'] = elements

    # interface id
    params['interfaceId'] = 'getWeatherLBS'

    # get point longitude and latitude information
    try:
        params['lon'] = ",".join([str(ilon).strip() for ilon in lon])
        params['lat'] = ",".join([str(ilat).strip() for ilat in lat])
    except:
        params['lon'] = str(lon).strip()
        params['lat'] = str(lat).strip()

    # time stamp
    params['timestamp'] = str(int(datetime.now().timestamp()*1000))

    # user information
    params['apikey'] = apikey
    params['pwd'] = pwd

    # construct sign string with hashlib md5 code
    sign_str = ""
    for key in ['elements','interfaceId','lat','lon','timestamp','apikey','pwd']:
        sign_str = sign_str + key + "=" + str(params.get(key)).strip() + "&"
    sign_str = sign_str[:-1]
    sign_str = sign_str + '&sign=' + hashlib.md5(sign_str.encode(encoding='UTF-8')).hexdigest().upper()
    sign_str = sign_str.replace('&pwd='+params['pwd'].strip(), '')

    # construct url
    url_str = 'https://music.data.cma.cn/lbs/api?' + sign_str
    if url_only:
        return url_str

    # request http contents
    http = urllib3.PoolManager()
    req = http.request('GET', url_str)
    if req.status != 200:
        print('Can not access the url: ' + url_str)
        return None

    # convert to data frame
    contents = json.loads(req.data.decode('utf-8').replace('\x00', ''), strict=False)
    data = pd.DataFrame(contents['DS'])
    return data

