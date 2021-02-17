# _*_ coding: utf-8 _*_

# Copyright (c) 2020 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Retrieve current weather from CMA restful API.
"""

from datetime import datetime
import hashlib


def get_current_weather(lon, lat, apikey, pwd, elements=None, url_only=False):
    """
    通过基于位置的天气实况服务接口（简称“位置服务接口”）调用实况天气.

    Args:
        points ([type]): [description]
        apikey ([type], optional): [description]. Defaults to None.
        elements (str, optional): [description]. Defaults to None.
    """

    # construct parameters
    params = {}

    # weather elements
    if elements is None:
        params['elements'] = 'TEM,RHU,WINS,WIND,WEA,VIS,TCDC,SST,PRE_1H,PRE_3H,PRE_6H,PRE_12H,PRE_24H'

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
    keys = sorted(params)
    for key in keys:
        sign_str = sign_str + key + "=" + str(params.get(key)).strip() + "&"
    sign_str = sign_str[:-1]
    sign_str = sign_str + '&sign=' + hashlib.md5(sign_str.encode(encoding='UTF-8')).hexdigest().upper()
    sign_str = sign_str.replace('&pwd='+params['pwd'].strip(), '')

    # construct url
    url_str = 'https://music.data.cma.cn/lbs/api?' + sign_str
    if url_only:
        return url_str
