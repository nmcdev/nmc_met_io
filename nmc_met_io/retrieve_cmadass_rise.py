# _*_ coding: utf-8 _*_

# Copyright (c) 2022 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
从CMADaaS 大数据云平台读取睿思模式数据.

Writed by Tangjian in 2022/2/5.
"""

import json
import urllib3
import urllib.request
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from nmc_met_io.retrieve_cmadaas import get_rest_result, _load_contents
import nmc_met_io.config as CONFIG


def _load_rise_contents(contents):
    """
    Extract information from contents.

    Args:
        contents (string): [description]

    Returns:
        [type]: [description]
    """
    if contents is None:
        print('Return None.')
        return None
    
    try:
        contents = json.loads(contents.decode('utf-8').replace('\x00', ''), strict=False)
    except Exception as e:
        print(e)
        print(contents)
        return None
    
    return contents


def cmadaas_get_rise_model_file(time, data_code="NAFP_WOG_ANA_100M", 
                                fcst_ele=None, userId=None, pwd=None,
                                out_dir=None, pbar=False, just_url=False):

    if out_dir is None:
        out_dir = CONFIG.get_cache_file(data_code, "", name="CMADaaS")

    time = time.strip()
    if fcst_ele is None:
        params = {'dataCode': data_code}
        if time[0] == '[':
            # set retrieve parameters
            params['timeRange'] = time
            interface_id = "getNafpFileByTimeRange"
        else:
            # set retrieve parameters
            params['time'] = time
            interface_id = "getNafpFileByTime"
    else:
        params = {'dataCode': data_code,
                  'fcstEle': fcst_ele.strip(),
                  'userId': userId,
                  'pwd': pwd}
        if time[0] == '[':
            # set retrieve parameters
            params['timeRange'] = time
            interface_id = "getNafpFileByElementAndTimeRange"
        else:
            # set retrieve parameters
            params['time'] = time
            interface_id = "getNafpFileByElementAndTime"
    params = {'dataCode': data_code,
              'userId': userId,
              'pwd': pwd}
    params['time'] = time
    interface_id = "getNafpFileByTime"

    # retrieve data contents
    contents = get_rest_result(interface_id, params)
    contents = _load_contents(contents)
    if contents is None:
        return None

    # just return the url
    if just_url:
        return contents['DS']
    # loop every file and download
    out_files = []
    files = tqdm(contents['DS']) if pbar else contents['DS']
    for file in files:
        out_file = Path(out_dir) / file['FILE_NAME']
        if not out_file.is_file():
            if 'RMAPS-RISE' in file['FILE_NAME']:
                urllib.request.urlretrieve(file['FILE_URL'], out_file)
        out_files.append(out_file)

    return out_files


def get_rise_rest_result(interface_id, params, url_only=False,
                         dns=None, port=None, data_format='json'):

    # set MUSIC server dns port
    if dns is None:
        dns  = CONFIG.CONFIG['BJDaaS']['DNS']
    if port is None:
        port = CONFIG.CONFIG['BJDaaS']['PORT']

    # construct complete parameters
    sign_params = params.copy()

    # user information
    if 'serviceNodeId' not in sign_params:
        sign_params['serviceNodeId'] = CONFIG.CONFIG['BJDaaS']['serviceNodeId']
    if 'userId' not in sign_params:
         sign_params['userId'] = CONFIG.CONFIG['BJDaaS']['USER_ID']
    if 'pwd' not in sign_params:
        sign_params['pwd'] = CONFIG.CONFIG['BJDaaS']['PASSWORD']

    # data interface Id and out data format
    sign_params['interfaceId'] = interface_id.strip()

    # construct sign string with hashlib md5 code
    sign_str = ""
    keys = sorted(sign_params)
    for key in keys:
        sign_str = sign_str + key + "=" + str(sign_params.get(key)).strip() + "&"
    sign_str = sign_str[:-1]

    # construct url
    url_str = 'http://' + dns + ':' + port + '/services/api/meteodata/data?' + sign_str
    print(url_str)
    if url_only:
        return url_str

    # request http contents
    http = urllib3.PoolManager()
    req = http.request('GET', url_str)
    if req.status != 200:
        print('Can not access the url: ' + url_str)
        return None

    return req.data


def rise5_model_by_pionts(init_time_str, data_code='RMAPSRISE5',
                          time_range=[0, 24], 
                          points="39.90/116.40", fcst_ele="2T,apcp_1hr,RH,10U,10V,10FG1"):

    # set retrieve parameters
    params = {'datacode': data_code,
              'time': init_time_str,
              'fcstLevel': '2,0,10',
              'minvalidtime': '{:d}'.format(time_range[0]),
              'maxvalidtime': '{:d}'.format(time_range[1]),
              'latlons': points,
              'elements': fcst_ele}
    interface_id = 'getNafpTimeSerialByPoint'


    # retrieve data contents
    contents = get_rise_rest_result(interface_id, params)
    contents = _load_rise_contents(contents)
    
    df = pd.DataFrame(contents['data'][0]['DS'])
    df = df.rename(columns={0:'lat',1:'lon',2:'level',3:'dtime',4:'var',5:'value',6:'time'})
    #2T,apcp_1hr,RH,10U,10V,10FG1
    df_2t = df.loc[df['var']=='2T'].rename(columns={'value':'2T'}).drop(['level','var'],axis=1)
    df_2rh = df.loc[df['var']=='RH'].rename(columns={'value':'2RH'}).drop(['level','var'],axis=1)
    
    df_10U = df.loc[df['var']=='10U'].rename(columns={'value':'10U'}).drop(['level','var'],axis=1)
    df_10V = df.loc[df['var']=='10V'].rename(columns={'value':'10V'}).drop(['level','var'],axis=1)
    df_10FG1 = df.loc[df['var']=='10FG1'].rename(columns={'value':'10FG1'}).drop(['level','var'],axis=1)
    df_APCP_1H = df.loc[df['var']=='apcp_1hr'].rename(columns={'value':'APCP_1HR'}).drop(['level','var'],axis=1)
    
    cob1=pd.merge(df_2t, df_2rh, how='left', on=['dtime','lat','lon','time'])
    cob2=pd.merge(df_10U, df_10V, how='left', on=['dtime','lat','lon','time'])
    cob3=pd.merge(df_10FG1, df_APCP_1H, how='left', on=['dtime','lat','lon','time'])
    cob4=pd.merge(cob1, cob2, how='left', on=['dtime','lat','lon','time'])
    cob5=pd.merge(cob3, cob4, how='left', on=['dtime','lat','lon','time'])
    return cob5


def rise_model_by_pionts(init_time_str, data_code='RMAPSRISE',
                        time_range=[0, 24], 
                        points="39.90/116.40", fcst_ele="2T,RH,10U,10V,10FG1"):

    # set retrieve parameters
    params = {'datacode': data_code,
              'time': init_time_str,
              'fcstLevel': '2,0,10',
              'minvalidtime': '{:d}'.format(time_range[0]),
              'maxvalidtime': '{:d}'.format(time_range[1]),
              'latlons': points,
              'elements': fcst_ele}
    interface_id = 'getNafpTimeSerialByPoint'

    # retrieve data contents
    contents = get_rise_rest_result(interface_id, params)
    contents = _load_rise_contents(contents)
    
    df = pd.DataFrame(contents['data'][0]['DS'])
    df = df.rename(columns={0:'lat',1:'lon',2:'level',3:'dtime',4:'var',5:'value',6:'time'})
    #2T,RH,10U,10V,10FG1
    df_2t = df.loc[df['var']=='2T'].rename(columns={'value':'2T'}).drop(['level','var'],axis=1)
    df_2rh = df.loc[df['var']=='RH'].rename(columns={'value':'2RH'}).drop(['level','var'],axis=1)
    
    df_10U = df.loc[df['var']=='10U'].rename(columns={'value':'10U'}).drop(['level','var'],axis=1)
    df_10V = df.loc[df['var']=='10V'].rename(columns={'value':'10V'}).drop(['level','var'],axis=1)
    df_10FG1 = df.loc[df['var']=='10FG1'].rename(columns={'value':'10FG1'}).drop(['level','var'],axis=1)
    
    cob1=pd.merge(df_2t, df_2rh, how='left', on=['dtime','lat','lon','time'])
    cob2=pd.merge(df_10U, df_10V, how='left', on=['dtime','lat','lon','time'])
    cob3=pd.merge(cob2, cob1, how='left', on=['dtime','lat','lon','time'])
    cob4=pd.merge(df_10FG1, cob3, how='left', on=['dtime','lat','lon','time'])
    
    return cob4