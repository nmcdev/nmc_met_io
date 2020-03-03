# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read micaps data file.
"""

import os.path
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta


def read_micaps_3(fname, limit=None):
    """
    Read Micaps 3 type file (general scatter point)

    1 此类数据主要用于非规范的站点填图。填图目前是单要素的。
    2 此类数据除用于填图外，还可根据站点数据用有限元法直接画等值线
      (只要等值线条数大于 0)。各等值线的值由文件头中的等值线值1、
      等值线值2 ...来决定。在这些等值线值中可选出一个为加粗线值。
    3 等值线可以被限制在一个剪切区域内。剪切区域由一个闭合折线定义
      该折线构成剪切区域的边缘。这个折线由剪切区域边缘线上的点数及
      各点的经纬度决定。
    4 当填的是地面要素时，文件头中的“层次”变为控制填图格式的标志：
        -1 填6小时降水量。当降水量为0.0mm时填T，当降水量为0.1-0.9时
           填一位小数，当降水量大于1时只填整数。
        -2 填24小时降水量。当降水量小于1mm时不填，大于等于1mm时只填整数。
        -3 填温度。只填整数。

    注意按照MICAPS3.2扩展的数据格式定义，在6小时雨量中，0.0表示微量降水
    而不是无降水，上述类别数据填图属性中设置小数位数不起作用。考虑到实际
    业务中使用的数据格式，修改为0.0时表示无降水，大于0并且小于0.1为微量降水
    任意使用负值或9999表示降水为0，可能会导致数据分析中出现异常结果。

    :param fname: micaps file name.
    :param limit: region limit, [min_lat, min_lon, max_lat, max_lon]
    :return: data, pandas type

    :Examples:
    >>> data = read_micaps_3('Z:/data/surface/jiany_rr/r20/17032908.000')

    """

    # check file exist
    if not os.path.isfile(fname):
        return None

    # read contents
    encodings = ['utf-8', 'gb18030', 'GBK']
    for encoding in encodings:
        txt = None
        try:
            with open(fname, 'r', encoding=encoding) as f:
                txt = f.read().replace('\n', ' ').split()
        except Exception:
            pass
    if txt is None:
        print("Micaps 3 file error: " + fname)
        return None

    # head information
    head_info = txt[2]

    # date and time
    year = int(txt[3]) if len(txt[3]) == 4 else int(txt[3]) + 2000
    month = int(txt[4])
    day = int(txt[5])
    hour = int(txt[6])
    time = datetime(year, month, day, hour)

    # level
    level = int(txt[7])

    # contour information
    n_contour = int(txt[8])
    pt = 9
    if n_contour > 0:
        contours = np.array(txt[pt:(pt + n_contour - 1)])
        pt += n_contour

    # smooth and bold value
    smoothCeof = float(txt[pt])
    pt += 1
    boldCeof = float(txt[pt])
    pt += 1

    # boundary
    n_bound = int(txt[pt])
    pt += 1
    if n_bound > 0:
        bound = np.array(txt[pt:(pt + 2 * n_bound - 1)])
        pt += 2 * n_bound

    # number of elements and data
    n_elements = int(txt[pt])
    pt += 1
    number = int(txt[pt])
    pt += 1

    # cut data
    txt = np.array(txt[pt:])
    txt.shape = [number, n_elements + 4]

    # initial data
    columns = list(['ID', 'lon', 'lat', 'alt'])
    columns.extend(['Var%s' % x for x in range(n_elements)])
    data = pd.DataFrame(txt, columns=columns)

    # convert column type
    data = data.iloc[:, 1:].apply(pd.to_numeric)

    # cut the region
    if limit is not None:
        data = data[(limit[0] <= data['lat']) & (data['lat'] <= limit[2]) &
                    (limit[1] <= data['lon']) & (data['lon'] <= limit[3])]

    # check records
    if len(data) == 0:
        return None

    # add time
    data['time'] = time
    data['level'] = level

    # return
    return data


def read_micaps_4(fname, limit=None, varname='data', varattrs={'units':''}, scale_off=None,
                  levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'}):
    """
    Read Micaps 4 type file (grid)

    :param fname: micaps file name.
    :param limit: region limit, [min_lat, min_lon, max_lat, max_lon]
    :param varname: set variable name.
    :param varattrs: set variable attributes, dictionary type.
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param levattrs: set level coordinate attributes, diectionary type.
    :return: data, xarray type

    :Examples:
    >>> data = read_micaps_4('Z:/data/newecmwf_grib/pressure/17032008.006')

    """

    # check file exist
    if not os.path.isfile(fname):
        return None

    # read contents
    encodings = ['utf-8', 'gb18030', 'GBK']
    for encoding in encodings:
        txt = None
        try:
            with open(fname, 'r', encoding=encoding) as f:
                txt = f.read().replace('\n', ' ').split()
        except Exception:
            pass
    if txt is None:
        print("Micaps 4 file error: " + fname)
        return None

    # head information
    head_info = txt[2]

    # date and time
    year = int(txt[3]) if len(txt[3]) == 4 else int(txt[3]) + 2000
    month = int(txt[4])
    day = int(txt[5])
    hour = int(txt[6])
    fhour = int(txt[7])
    if hour >= 24:    # some times, micaps file head change the order.
        hour = int(txt[7])
        fhour = int(txt[6])
    fhour = np.array([fhour], dtype=np.float)
    init_time = datetime(year, month, day, hour)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

    # vertical level
    level = np.array([float(txt[8])])

    # grid information
    xint = float(txt[9])
    yint = float(txt[10])
    slon = float(txt[11])
    slat = float(txt[13])
    nlon = int(txt[15])
    nlat = int(txt[16])
    lon = slon + np.arange(nlon) * xint
    lat = slat + np.arange(nlat) * yint

    # contour information
    cnInterval = float(txt[17])
    cnStart = float(txt[18])
    cnEnd = float(txt[19])

    # smooth and bold value
    smoothCeof = float(txt[20])
    boldCeof = float(txt[21])

    # extract data
    data = (np.array(txt[22:])).astype(np.float)
    data.shape = [nlat, nlon]

    # check latitude order
    if lat[0] > lat[1]:
        lat = lat[::-1]
        data = data[::-1, :]

    # scale and offset the data, if necessary.
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]

    # define coordinates
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})
    if level[0] != 0:
        level_coord = ('level', level, levattrs)

    # create xarray dataset
    if level[0] == 0:
        data = data[np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'lat', 'lon'], data, varattrs)},
            coords={
                'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
    else:
        data = data[np.newaxis, np.newaxis, ...]
        data = xr.Dataset({
            varname:(['time', 'level', 'lat', 'lon'], data, varattrs)},
            coords={
                'time':time_coord, 'level':level_coord, 
                'lat':lat_coord, 'lon':lon_coord})

    # add time coordinates
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name':'forecast_period', 'units':'hour'})

    # subset data
    if limit is not None:
        lat_bnds, lon_bnds = [limit[0], limit[2]], [limit[1], limit[3]]
        data = data.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))

    # return
    return data


def read_micaps_11(fname, limit=None, scale_off=None, no_level=False,
                   levattrs={'long_name':'pressure_level', 'units':'hPa', '_CoordinateAxisType':'Pressure'}):
    """
    Read Micaps 11 type file (grid u, v vector data)

    :param fname: micaps file name.
    :param limit: region limit, [min_lat, min_lon, max_lat, max_lon]
    :param scale_off: [scale, offset], return values = values*scale + offset.
    :param no_level: sometimes, there is no level information in the file, so just ignore.
    :param levattrs: set level coordinate attributes, diectionary type.
    :return: data, xarray type

    :Examples:
    >>> data = read_micaps_4('Z:/data/newecmwf_grib/stream/850/17032008.006')

    """

    # check file exist
    if not os.path.isfile(fname):
        return None

    # read contents
    encodings = ['utf-8', 'gb18030', 'GBK']
    for encoding in encodings:
        txt = None
        try:
            with open(fname, 'r', encoding=encoding) as f:
                txt = f.read().replace('\n', ' ').split()
        except Exception:
            pass
    if txt is None:
        print("Micaps 11 file error: " + fname)
        return None

    # head information
    head_info = txt[2]

    # date and time
    year = int(txt[3]) if len(txt[3]) == 4 else int(txt[3]) + 2000
    month = int(txt[4])
    day = int(txt[5])
    hour = int(txt[6])
    fhour = np.array([int(txt[7])], dtype=np.float)
    init_time = datetime(year, month, day, hour)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ms]')
    time = np.array([time], dtype='datetime64[ms]')

    # vertical level
    if no_level:
        level = np.array([0.0])
        ind = 8
    else:
        level = np.array([float(txt[8])])
        ind=9

    # grid information
    xint = float(txt[ind])
    ind += 1
    yint = float(txt[ind])
    ind += 1
    slon = float(txt[ind])
    ind += 2
    slat = float(txt[ind])
    ind += 2
    nlon = int(txt[ind])
    ind += 1
    nlat = int(txt[ind])
    ind += 1
    lon = slon + np.arange(nlon) * xint
    lat = slat + np.arange(nlat) * yint

    # extract data
    data = (np.array(txt[ind:])).astype(np.float)
    data.shape = [2, nlat, nlon]

    # check latitude order
    if lat[0] > lat[1]:
        lat = lat[::-1]
        data = data[:, ::-1, :]

    # scale and offset the data, if necessary.
    if scale_off is not None:
        data = data * scale_off[0] + scale_off[1]

    # define coordinates
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})
    if level[0] != 0:
        level_coord = ('level', level, levattrs)

    # create xarray data
    uwind = np.squeeze(data[0, :, :])
    vwind = np.squeeze(data[1, :, :])
    speed = np.sqrt(uwind*uwind + vwind*vwind)
    # create xarray dataset
    if level[0] == 0:
        uwind = uwind[np.newaxis, ...]
        vwind = vwind[np.newaxis, ...]
        speed = speed[np.newaxis, ...]
        data = xr.Dataset({
            'uwind':(['time', 'lat', 'lon'], uwind, {"long_name":"u-component of wind", "units":"m/s"}),
            'vwind':(['time', 'lat', 'lon'], vwind, {"long_name":"v-component of wind", "units":"m/s"}),
            'speed':(['time', 'lat', 'lon'], speed, {"long_name":"wind speed", "units":"m/s"})},
            coords={
                'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})
    else:
        uwind = uwind[np.newaxis, np.newaxis, ...]
        vwind = vwind[np.newaxis, np.newaxis, ...]
        speed = speed[np.newaxis, np.newaxis, ...]
        data = xr.Dataset({
            'uwind':(['time', 'level', 'lat', 'lon'], uwind, {"long_name":"u-component of wind", "units":"m/s"}),
            'vwind':(['time', 'level', 'lat', 'lon'], vwind, {"long_name":"v-component of wind", "units":"m/s"}),
            'speed':(['time', 'level', 'lat', 'lon'], speed, {"long_name":"wind speed", "units":"m/s"})},
            coords={
                'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add time coordinates
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name':'forecast_period', 'units':'hour'})

    # subset data
    if limit is not None:
        lat_bnds, lon_bnds = [limit[0], limit[2]], [limit[1], limit[3]]
        data = data.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))

    # return
    return data


def read_micaps_14(fname):
    """
    Read micaps 14 file (编辑图象的图元数据, 即交互操作结果数据).

    :param fname: micaps 14 filename.
    :return: data dictionary

    :Examples:
    >>> data = read_micaps_14("Z:/diamond/update/rr082008.024")

    """

    # check file exist
    if not os.path.isfile(fname):
        return None

    # read contents
    encodings = ['utf-8', 'gb18030', 'GBK']
    for encoding in encodings:
        txt = None
        try:
            with open(fname, 'r', encoding=encoding) as f:
                txt = f.read().replace('\n', ' ').split()
        except Exception:
            pass
    if txt is None:
        print("Micaps 14 file error: " + fname)
        return None

    # head information
    head_info = txt[2]

    # date and time
    year = int(txt[3]) if len(txt[3]) == 4 else int(txt[3]) + 2000
    month = int(txt[4])
    day = int(txt[5])
    hour = int(txt[6])
    time = datetime(year, month, day, hour)
    fhour = int(txt[7])

    # ======================================================
    # read lines
    # ======================================================
    if 'LINES:' in txt:
        # get the start position
        idx = txt.index('LINES:')

        # number of lines
        number = int(txt[idx+1])
        idx += 2

        # loop every line
        if number > 0:
            # define data
            line_width = []
            line_xyz_num = []
            line_xyz = []
            line_label_num = []
            line_label = []
            line_label_xyz = []

            for i in range(number):
                # line width
                width = float(txt[idx])
                line_width.append(width)
                idx += 1

                # line xyz point number
                xyz_num = int(txt[idx])
                line_xyz_num.append(xyz_num)
                idx += 1

                # line xyz
                xyz = np.array(txt[idx:(idx + 3*xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                line_xyz.append(xyz)
                idx += xyz_num * 3

                # line label
                label = txt[idx]
                line_label.append(label)
                idx += 1

                # line label number
                label_num = int(txt[idx])
                line_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        txt[idx:(idx + 3*label_num)]).astype(np.float)
                    label_xyz.shape = [label_num, 3]
                    line_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    line_label_xyz.append([])

                # construct line data type
                lines = {
                    "line_width": line_width, "line_xyz_num": line_xyz_num,
                    "line_xyz": line_xyz, "line_label_num": line_label_num,
                    "line_label": line_label, "line_label_xyz": line_label_xyz}
        else:
            lines = {}
    else:
        lines = {}

    # ======================================================
    # read line symbols
    # ======================================================
    if 'LINES_SYMBOL:' in txt:
        # get the start position
        idx = txt.index('LINES_SYMBOL:')

        # number of line symbols
        number = int(txt[idx + 1])
        idx += 2

        # loop every line symbol
        if number > 0:
            # define data
            linesym_code = []
            linesym_width = []
            linesym_xyz_num = []
            linesym_xyz = []

            for i in range(number):
                # line symbol code
                code = int(txt[idx])
                linesym_code.append(code)
                idx += 1

                # line width
                width = float(txt[idx])
                linesym_width.append(width)
                idx += 1

                # line symbol xyz point number
                xyz_num = int(txt[idx])
                linesym_xyz_num.append(xyz_num)
                idx += 1

                # line symbol xyz
                xyz = np.array(txt[idx:(idx + 3*xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                linesym_xyz.append(xyz)
                idx += xyz_num * 3

                # line symbol label
                label = txt[idx]
                idx += 1

                # line symbol label number
                label_num = int(txt[idx])
                idx += label_num * 3 + 1

            lines_symbol = {"linesym_code": linesym_code,
                            "linesym_width": linesym_width,
                            "linesym_xyz_num": linesym_xyz_num,
                            "linesym_xyz": linesym_xyz}
        else:
            lines_symbol = {}
    else:
        lines_symbol = {}

    # ======================================================
    # read symbol
    # ======================================================
    if "SYMBOLS:" in txt:
        # start position of symbols
        idx = txt.index("SYMBOLS:")

        # number of lines
        number = int(txt[idx + 1])
        idx += 2

        # loop every symbol
        if number > 0:
            # define data
            symbol_code = []
            symbol_xyz = []
            symbol_value = []

            for i in range(number):
                # symbol code
                code = int(txt[idx])
                symbol_code.append(code)
                idx += 1

                # symbol xyz
                xyz = np.array(txt[idx:(idx + 3)]).astype(np.float)
                symbol_xyz.append(xyz)
                idx += 3

                # symbol value
                value = txt[idx]
                symbol_value.append(value)
                idx += 1

            symbols = {"symbol_code": symbol_code,
                       "symbol_xyz": symbol_xyz,
                       "symbol_value": symbol_value}
        else:
            symbols = {}
    else:
        symbols = {}

    # ======================================================
    # read closed contours
    # ======================================================
    if "CLOSED_CONTOURS:" in txt:
        # get the start position
        idx = txt.index('CLOSED_CONTOURS:')

        # number of lines
        number = int(txt[idx + 1])
        idx += 2

        # loop every closed contour
        if number > 0:
            # define data
            cn_width = []
            cn_xyz_num = []
            cn_xyz = []
            cn_label_num = []
            cn_label = []
            cn_label_xyz = []

            for i in range(number):
                # line width
                width = float(txt[idx])
                cn_width.append(width)
                idx += 1

                # line xyz point number
                xyz_num = int(txt[idx])
                cn_xyz_num.append(xyz_num)
                idx += 1

                # line xyz
                xyz = np.array(txt[idx:(idx + 3 * xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                cn_xyz.append(xyz)
                idx += 3 * xyz_num

                # line label
                label = txt[idx]
                cn_label.append(label)
                idx += 1

                # line label number
                label_num = int(txt[idx])
                cn_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        txt[idx:(idx + 3 * label_num)]).astype(np.float)
                    label_xyz.shape = [3, label_num]
                    cn_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    cn_label_xyz.append([])

            closed_contours = {
                "cn_width": cn_width, "cn_xyz_num": cn_xyz_num,
                "cn_xyz": cn_xyz, "cn_label": cn_label,
                "cn_label_num": cn_label_num, "cn_label_xyz": cn_label_xyz}
        else:
            closed_contours = {}
    else:
        closed_contours = {}

    # ======================================================
    # read station situation
    # ======================================================
    if "STATION_SITUATION" in txt:
        # get the start position
        idx = txt.index('STATION_SITUATION')

        # find data subscript
        end_idx = idx + 1
        while txt[end_idx].isdigit() and end_idx < len(txt):
            end_idx += 1
        if end_idx > idx + 1:
            stations = np.array(txt[(idx+1):(end_idx)])
            stations.shape = [len(stations)//2, 2]
        else:
            stations = None
    else:
        stations = None

    # ======================================================
    # read weather regions
    # ======================================================
    if "WEATHER_REGION:" in txt:
        # get the start position
        idx = txt.index('WEATHER_REGION:')

        # number of regions
        number = int(txt[idx + 1])
        idx += 2

        # loop every region
        if number > 0:
            # define data
            weather_region_code = []
            weather_region_xyz_num = []
            weather_region_xyz = []

            for i in range(number):
                # region code
                code = int(txt[idx])
                weather_region_code.append(code)
                idx += 1

                # region xyz point number
                xyz_num = int(txt[idx])
                weather_region_xyz_num.append(xyz_num)
                idx += 1

                # region xyz point
                xyz = np.array(
                    txt[idx:(idx + 3*xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                weather_region_xyz.append(xyz)
                idx += 3 * xyz_num

            weather_region = {
                "weather_region_code": weather_region_code,
                "weather_region_xyz_num": weather_region_xyz_num,
                "weather_region_xyz": weather_region_xyz}
        else:
            weather_region = {}
    else:
        weather_region = {}

    # ======================================================
    # read fill area
    # ======================================================
    if "FILLAREA:" in txt:
        # get the start position
        idx = txt.index('FILLAREA:')

        # number of regions
        number = int(txt[idx + 1])
        idx += 2

        # loop every fill area
        if number > 0:
            # define data
            fillarea_code = []
            fillarea_num = []
            fillarea_xyz = []
            fillarea_type = []
            fillarea_color = []
            fillarea_frontcolor = []
            fillarea_backcolor = []
            fillarea_gradient_angle = []
            fillarea_graphics_type = []
            fillarea_frame = []

            for i in range(number):
                # code
                code = int(txt[idx])
                fillarea_code.append(code)
                idx += 1

                # xyz point number
                xyz_num = int(txt[idx])
                fillarea_num.append(xyz_num)
                idx += 1

                # xyz point
                xyz = np.array(
                    txt[idx:(idx + 3 * xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                fillarea_xyz.append(xyz)
                idx += 3 * xyz_num

                # fill type
                ftype = int(txt[idx])
                fillarea_type.append(ftype)
                idx += 1

                # line color
                color = np.array(txt[idx:(idx + 4)]).astype(np.int)
                fillarea_color.append(color)
                idx += 4

                # front color
                front_color = np.array(txt[idx:(idx + 4)]).astype(np.int)
                fillarea_frontcolor.append(front_color)
                idx += 4

                # background color
                back_color = np.array(txt[idx:(idx + 4)]).astype(np.int)
                fillarea_backcolor.append(back_color)
                idx += 4

                # color gradient angle
                gradient_angle = float(txt[idx])
                fillarea_gradient_angle.append(gradient_angle)
                idx += 1

                # graphics type
                graphics_type = int(txt[idx])
                fillarea_graphics_type.append(graphics_type)
                idx += 1

                # draw frame or not
                frame = int(txt[idx])
                fillarea_frame.append(frame)
                idx += 1

            fill_area = {
                "fillarea_code": fillarea_code, "fillarea_num": fillarea_num,
                "fillarea_xyz": fillarea_xyz, "fillarea_type": fillarea_type,
                "fillarea_color": fillarea_color,
                "fillarea_frontcolor": fillarea_frontcolor,
                "fillarea_backcolor": fillarea_backcolor,
                "fillarea_gradient_angle": fillarea_gradient_angle,
                "fillarea_graphics_type": fillarea_graphics_type,
                "fillarea_frame": fillarea_frame}
        else:
            fill_area = {}
    else:
        fill_area = {}

    # ======================================================
    # read notes symbol
    # ======================================================
    if "NOTES_SYMBOL:" in txt:
        # get the start position
        idx = txt.index('NOTES_SYMBOL:')

        # number of regions
        number = int(txt[idx + 1])
        idx += 2

        # loop every notes symbol
        if number > 0:
            # define data
            nsymbol_code = []
            nsymbol_xyz = []
            nsymbol_charLen = []
            nsymbol_char = []
            nsymbol_angle = []
            nsymbol_fontLen = []
            nsymbol_fontName = []
            nsymbol_fontSize = []
            nsymbol_fontType = []
            nsymbol_color = []

            for i in range(number):
                # code
                code = int(txt[idx])
                nsymbol_code.append(code)
                idx += 1

                # xyz
                xyz = np.array(txt[idx:(idx + 3)]).astype(np.float)
                nsymbol_xyz.append([xyz])
                idx += 3

                # character length
                char_len = int(txt[idx])
                nsymbol_charLen.append(char_len)
                idx += 1

                # characters
                char = txt[idx]
                nsymbol_char.append(char)
                idx += 1

                # character angle
                angle = txt[idx]
                nsymbol_angle.append(angle)
                idx += 1

                # font length
                font_len = txt[idx]
                nsymbol_fontLen.append(font_len)
                idx += 1

                # font name
                font_name = txt[idx]
                nsymbol_fontName.append(font_name)
                idx += 1

                # font size
                font_size = txt[idx]
                nsymbol_fontSize.append(font_size)
                idx += 1

                # font type
                font_type = txt[idx]
                nsymbol_fontType.append(font_type)
                idx += 1

                # color
                color = np.array(txt[idx:(idx + 4)]).astype(np.int)
                nsymbol_color.append(color)
                idx += 4

            notes_symbol = {
                "nsymbol_code": nsymbol_code,
                "nsymbol_xyz": nsymbol_xyz,
                "nsymbol_charLen": nsymbol_charLen,
                "nsymbol_char": nsymbol_char,
                "nsymbol_angle": nsymbol_angle,
                "nsymbol_fontLen": nsymbol_fontLen,
                "nsymbol_fontName": nsymbol_fontName,
                "nsymbol_fontSize": nsymbol_fontSize,
                "nsymbol_fontType": nsymbol_fontType,
                "nsymbol_color": nsymbol_color}
        else:
            notes_symbol = {}
    else:
        notes_symbol = {}

    # ======================================================
    # read lines symbols with property
    # ======================================================
    if "WITHPROP_LINESYMBOLS:" in txt:
        # get the start position
        idx = txt.index('WITHPROP_LINESYMBOLS:')

        # number of regions
        number = int(txt[idx + 1])
        idx += 2

        # loop every line symbol
        if number > 0:
            # define data
            plinesym_code = []
            plinesym_width = []
            plinesym_color = []
            plinesym_type = []
            plinesym_shadow = []
            plinesym_xyz_num = []
            plinesym_xyz = []
            plinesym_label = []
            plinesym_label_num = []
            plinesym_label_xyz = []

            for i in range(number):
                # line symbol code
                code = int(txt[idx])
                plinesym_code.append(code)
                idx += 1

                # line width
                width = float(txt[idx])
                plinesym_width.append(width)
                idx += 1

                # line color
                color = np.array(txt[idx:(idx + 3)]).astype(np.int)
                plinesym_color.append([color])
                idx += 3

                # line type
                ltype = int(txt[idx])
                plinesym_type.append(ltype)
                idx += 1

                # line shadow
                shadow = int(txt[idx])
                plinesym_shadow.append(shadow)
                idx += 1

                # line symbol xyz point number
                xyz_num = int(txt[idx])
                plinesym_xyz_num.append(xyz_num)
                idx += 1

                # line symbol xyz
                xyz = np.array(txt[idx:(idx + 3 * xyz_num)]).astype(np.float)
                xyz.shape = [xyz_num, 3]
                plinesym_xyz.append(xyz)
                idx += 3 * xyz_num

                # line symbol label
                label = txt[idx]
                plinesym_label.append(label)
                idx += 1

                # line label number
                label_num = int(txt[idx])
                plinesym_label_num.append(label_num)
                idx += 1

                # label xyz
                if label_num > 0:
                    label_xyz = np.array(
                        txt[idx:(idx + 3 * label_num)]).astype(np.float)
                    label_xyz.shape = [label_num, 3]
                    plinesym_label_xyz.append(label_xyz)
                    idx += label_num * 3
                else:
                    plinesym_label_xyz.append([])

            plines_symbol = {
                "plinesym_code": plinesym_code,
                "plinesym_width": plinesym_width,
                "plinesym_color": plinesym_color,
                "plinesym_type": plinesym_type,
                "plinesym_shadow": plinesym_shadow,
                "plinesym_xyz_num": plinesym_xyz_num,
                "plinesym_xyz": plinesym_xyz,
                "plinesym_label": plinesym_label,
                "plinesym_label_num": plinesym_label_num,
                "plinesym_label_xyz": plinesym_label_xyz}
        else:
            plines_symbol = {}
    else:
        plines_symbol = {}

    # return data contents
    return {"file_type": 14,
            "time": time,
            "fhour": fhour,
            "lines": lines,
            "lines_symbol": lines_symbol,
            "symbols": symbols,
            "closed_contours": closed_contours,
            "stations": stations,
            "weather_region": weather_region,
            "fill_area": fill_area,
            "notes_symbol": notes_symbol,
            "plines_symbol": plines_symbol}

