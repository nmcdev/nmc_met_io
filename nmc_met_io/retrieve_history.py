# _*_ coding: utf-8 _*_

# Copyright (c) 2021 NMC Developers.
# Distributed under the terms of the GPL V3 License.

import os
import tempfile
import requests
import nmc_met_io.config as CONFIG


def retrieve_data_file(product, date, ele, level, step, time, 
                       format="nc", area="WHOLE", outfile=None):
    """
    Retrieve data file from history cases database.

    Args:
        product (str): like "ECMWF-HR", 请求数据类型 一次只能请求一种数据
        date (str): like "20200101", 多个日期可以在程序外部加入循环 避免一次请求发送多个日期
        ele (str): like "TMP", 多个要素可以在程序外部加入循环
        level (str): like "500", 多个层次可以直接用|分隔  "500|850"
        step (str): like "0to9by3", 三个数字为 起始预报时效 终止预报时效 时效间隔
        time (str): like "08", 多个时次可以在程序外部加入循环
        area (str): "80|70|10|170", 裁剪的经纬度范围 按照 北纬、西经、南纬、东经的顺序 中间用|分隔 如果不需要裁剪则填写WHOLE
        format (str): "nc", 返回数据格式 dimaond mdfs nc
        outfile (str, optional): output file name. if None, a temporary filename will be given.
    """

    # construct sever url
    server_url = "http://{}:{}/hcdata?username={}&password={}".format(
        CONFIG.CONFIG['HISTORYCASESDATA']['HC_IP'], 
        CONFIG.CONFIG['HISTORYCASESDATA']['HC_PORT'],
        CONFIG.CONFIG['HISTORYCASESDATA']['USER_ID'],
        CONFIG.CONFIG['HISTORYCASESDATA']['PASSWORD'])
    server_url = server_url + \
         '&product={}&date={}&ele={}&level={}&step={}&time={}&area={}&format={}'.format(
             product, date, ele, level, step, time, area, format)

    # retireve data file
    res = requests.get(server_url)
    content_type = res.headers.get('Content-Type')

    # write to local file
    if content_type.find('json') != -1:
        print(res.text)
    else:
        if outfile is None:
            outfile = os.path.join(
                os.getcwd(), next(tempfile._get_candidate_names())+'.7z')
        with open(outfile, 'wb') as local_file:
            buffer = 1024
            for chunk in res.iter_content(buffer):
                local_file.write(chunk)
        print('文件%s已成功保存到本地' % outfile)
        local_file.close()

    res.close()
    return outfile
