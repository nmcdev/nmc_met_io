# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read FY satellite awx format file.
"""

from datetime import datetime, timedelta
import numpy as np
import xarray as xr


def resolve_awx_bytearray(btarray, units=''):
    """
    解析awx文件的字节内容, 返回数据信息

    Args:
        btarray: byte array.
    """
    
    # the first class file head  一级文件头记录采用定长方式, 共40字节
    head1_dtype = [
        ('SAT96', 'S12'),                    # SAT96 filename
        ('byteSequence', 'i2'),              # 整型数的字节顺序, 0 低字节在前, 高字节在后; !=0 高字节在前, 低字节在后.
        ('firstClassHeadLength', 'i2'),      # 第一节文件头长度
        ('secondClassHeadLength', 'i2'),     # 第二节文件头长度
        ('padDataLength', 'i2'),             # 填充段数据长度
        ('recordLength', 'i2'),              # 记录长度(字节), 图像产品: 记录长度=图形宽度, 格点场产品: 记录长度=横向格点数x格点数据字长
        ('headRecordNumber', 'i2'),          # 文件头占用记录数, 一级文件头、二填充段扩展以及的所占用总记录个数
        ('dataRecordNumber', 'i2'),          # 产品数据占用记录数
        ('productCategory', 'i2'),           # 产品类别, 1：静止, 2：极轨, 3：格点定量, 4：离散, 5: 图形和分析
        ('compressMethod', 'i2'),            # 压缩方式, 0: 未压缩; 1 行程编码压缩; 2 LZW方式压缩; 3 特点方式压缩
        ('formatString', 'S8'),              # 格式说明字符串, 'SAT2004', 'SAT98'为早期版本
        ('qualityFlag', 'i2')]               # 产品数据质量标记, 1 完全可靠; 2 基本可靠; 3 有缺值, 可用; 4 不可用
    head1_info = np.frombuffer(btarray[0:40], dtype=head1_dtype)
    ind = 40

    if head1_info['productCategory'] == 1:
        # the second class file head  二级文件头采用不定长方式，内容依据产品的不同而不同.
        head2_dtype = [
            ('satelliteName', 'S8'),                 # 卫星名
            ('year', 'i2'), ('month', 'i2'),
            ('day', 'i2'), ('hour', 'i2'),
            ('minute', 'i2'),
            ('channel', 'i2'),                       # 通道号, 1红外, 2水汽, 3红外分裂, 4可见光, 5中红外, 6备用
            ('flagOfProjection', 'i2'),              # 投影, 0为投影, 1兰勃托, 2麦卡托, 3极射, 4等经纬度, 5等面积
            ('widthOfImage', 'i2'),
            ('heightOfImage', 'i2'),
            ('scanLineNumberOfImageTopLeft', 'i2'),
            ('pixelNumberOfImageTopLeft', 'i2'),
            ('sampleRatio', 'i2'),
            ('latitudeOfNorth', 'i2'),
            ('latitudeOfSouth', 'i2'),
            ('longitudeOfWest', 'i2'),
            ('longitudeOfEast', 'i2'),
            ('centerLatitudeOfProjection', 'i2'),
            ('centerLongitudeOfProjection', 'i2'),
            ('standardLatitude1', 'i2'),
            ('standardLatitude2', 'i2'),
            ('horizontalResolution', 'i2'),
            ('verticalResolution', 'i2'),
            ('overlapFlagGeoGrid', 'i2'),
            ('overlapValueGeoGrid', 'i2'),
            ('dataLengthOfColorTable', 'i2'),
            ('dataLengthOfCalibration', 'i2'),
            ('dataLengthOfGeolocation', 'i2'),
            ('reserved', 'i2')]
        head2_info = np.frombuffer(btarray[ind:(ind+64)], dtype=head2_dtype)
        ind += 64

        # color table
        if head2_info['dataLengthOfColorTable'] != 0:
            table_R =  np.frombuffer(btarray[ind:(ind + 256)], dtype='u1')
            ind += 256
            table_G =  np.frombuffer(btarray[ind:(ind + 256)], dtype='u1')
            ind += 256
            table_B =  np.frombuffer(btarray[ind:(ind + 256)], dtype='u1')
            ind += 256
        
        # calibration table  定标数据块, 描述的是图像灰度值与探测物理量之间的关系
        # refer to
        #    http://bbs.06climate.com/forum.php?mod=viewthread&tid=89296
        #    https://github.com/myyd/China_AWX/blob/master/awx/FY4A_AWX.py
        # 2022/3/31日更改, 采用无符号整型'u2'读取, 并且区分了可见光和红外, 水汽的定标表
        calibration_table = None
        if head2_info['dataLengthOfCalibration'] != 0:
            calibration_table = np.frombuffer(btarray[ind:(ind + 2048)], dtype='u2')
            if head2_info['channel'] == 4:
                # 对于可见光通道图像, 定标数据在0-63的范围内查找
                calibration_table = calibration_table * 0.01
                calibration_table = calibration_table[0:64]
                calibration_table = calibration_table.repeat(4)
            else:
                # 对于红外,水汽图像, 在全局范围内查找, 但要隔4进行跳点
                calibration_table = calibration_table * 0.01
                calibration_table = calibration_table[0::4]
            ind += 2048

        # geolocation table
        if head2_info['dataLengthOfGeolocation'] != 0:
            geolocation_dtype = [
                    ('coordinate', 'i2'),
                    ('source', 'i2'),
                    ('delta', 'i2'),
                    ('left_top_lat', 'i2'),
                    ('left_top_lon', 'i2'),
                    ('horizontalNumber', 'i2'),
                    ('verticalNumber', 'i2'),
                    ('reserved', 'i2')]
            geolocation_info = np.frombuffer(btarray[ind:(ind+16)], dtype=geolocation_dtype)
            ind += 16
            geolocation_length = geolocation_info['horizontal_number'][0] * geolocation_info['vertical_number'][0] * 2
            geolocation_table = np.frombuffer(btarray[ind:(ind+geolocation_length)], dtype='i2')
            ind += geolocation_length

        # pad field
        pad_field = np.frombuffer(btarray[ind:(ind+head1_info['padDataLength'][0])], dtype='u1')
        ind += head1_info['padDataLength'][0]

        # retrieve data records
        data_len = (head2_info['heightOfImage'][0].astype(int) *
                    head2_info['widthOfImage'][0])
        data = np.frombuffer(btarray[ind:(ind + data_len)], dtype='u1', count=data_len)
        if calibration_table is not None:
            data = calibration_table[data]
        data.shape = (head2_info['heightOfImage'][0], head2_info['widthOfImage'][0])
        
        # 由于数据是按照左上角开始放置, 为此需要对纬度顺序进行反转
        data = np.flip(data, axis=0)

        # construct longitude and latitude coordinates
        # if use the verticalResolution and horizontalResolution, lon and lat will not be correct.
        #lat = (
        #    head2_info['latitudeOfNorth'][0]/100. - 
        #    np.arange(head2_info['heightOfImage'][0])*head2_info['verticalResolution'][0]/100.)
        #lon = (
        #    head2_info['longitudeOfWest'][0]/100. + 
        #    np.arange(head2_info['widthOfImage'][0])*head2_info['horizontalResolution'][0]/100.)
        lat = np.linspace(
            head2_info['latitudeOfSouth'][0]/100., head2_info['latitudeOfNorth'][0]/100.,
            num=head2_info['heightOfImage'][0])
        lon = np.linspace(
            head2_info['longitudeOfWest'][0]/100., head2_info['longitudeOfEast'][0]/100.,
            num=head2_info['widthOfImage'][0])
        
        # construct time
        time = datetime(
            head2_info['year'][0], head2_info['month'][0],
            head2_info['day'][0], head2_info['hour'][0], head2_info['minute'][0])
        time = np.array([time], dtype='datetime64[ms]')

        # define coordinates
        time_coord = ('time', time)
        lon_coord = ('lon', lon, {
            'long_name':'longitude', 'units':'degrees_east',
            '_CoordinateAxisType':'Lon', 'axis': "X"})
        lat_coord = ('lat', lat, {
            'long_name':'latitude', 'units':'degrees_north',
            '_CoordinateAxisType':'Lat', 'axis': "Y"})
        channel_coord = ('channel', [head2_info['channel'][0]],
                            {'long_name':'channel', 'units':''})

        # create xarray
        data = data[np.newaxis, np.newaxis, ...]
        varattrs = {
            'productCategory': head1_info['productCategory'][0],   # 产品类型, 1:静止, 2:极轨, 3:格点, 4:离散, 5:图形和分析
            'formatString': head1_info['formatString'][0],         # 产品格式名称
            'qualityFlag': head1_info['qualityFlag'][0],           # 产品质量标识
            'satelliteName': head2_info['satelliteName'][0],       # 卫星名称
            'flagOfProjection': head2_info['flagOfProjection'][0], # 投影方式, 0:未投影, 1:兰勃托, 2:麦卡托, 3:极射, 4:等经纬, 5:等面积
            'units': units}
        data = xr.Dataset({
            'image':(['time', 'channel', 'lat', 'lon'], data, varattrs)},
            coords={ 'time':time_coord, 'channel':channel_coord,
            'lat':lat_coord, 'lon':lon_coord})

        # add attributes
        data.attrs['Conventions'] = "CF-1.6"
        data.attrs['Origin'] = 'MICAPS Cassandra Server'

        # return
        return data
    
    elif head1_info['productCategory'] == 3:
        # 参考https://github.com/wqshen/AwxReader 读取格点定量数据
        # the second class file head  二级文件头采用不定长方式，内容依据产品的不同而不同.
        head2_dtype = [
            ('satelliteName', 'S8'),                 # 卫星名
            ('element', 'i2'),                       # 格点场要素
            ('byte', 'i2'),                          # 格点数据字节
            ('base', 'i2'),                          # 格点数据基准值
            ('scale', 'i2'),                         # 格点数据比例因子
            ('timeScale', 'i2'),                     # 时间范围代码
            ('startYear', 'i2'), ('startMonth', 'i2'),
            ('startDay', 'i2'), ('startHour', 'i2'), 
            ('startMinute', 'i2'), ('endYear', 'i2'),
            ('endMonth', 'i2'), ('endDay', 'i2'),
            ('endHour', 'i2'), ('endMinute', 'i2'),
            ('leftupLat', 'i2'),                     # 网格左上角纬度
            ('leftupLon', 'i2'),                     # 网格左上角经度
            ('rightdownLat', 'i2'),                 # 网格右下角纬度
            ('rightdownLon', 'i2'),                 # 网格右下角经度
            ('resolutionUnit', 'i2'),                # 格距单位
            ('horizontalResolution', 'i2'),          # 横向格距
            ('verticalResolution', 'i2'),            # 纵向格距
            ('widthOfImage', 'i2'),                  # 横行格点数
            ('heightOfImage', 'i2'),                 # 纵向格点数
            ('hasLand', 'i2'),                       # 有无陆地判释值
            ('land', 'i2'),                          # 陆地具体判释值
            ('hasCloud', 'i2'),                      # 有无云判释值
            ('cloud', 'i2'),                         # 云具体判释值
            ('hasWater', 'i2'),                      # 有无水体判释值
            ('water', 'i2'),                         # 水体具体判释值
            ('hasIce', 'i2'),                        # 有无冰体判释值
            ('ice', 'i2'),                           # 冰体具体判释值
            ('hasQuality', 'i2'),                    # 是否有质量控制值
            ('qualityUp', 'i2'),                     # 质量控制值上限
            ('qualityDown', 'i2'),                   # 质量控制值下限
            ('reserved', 'i2')]        
        head2_info = np.frombuffer(btarray[ind:(ind+80)], dtype=head2_dtype)
        ind += 80
        
        # 读入数据      
        hgrid_num, vgrid_num = head2_info['widthOfImage'][0].astype(int), head2_info['heightOfImage'][0].astype(int)
        data_len = hgrid_num * vgrid_num
        data = np.frombuffer(btarray[ind:(ind + data_len)], dtype='u1', count=data_len)
        data.shape = (vgrid_num, hgrid_num)
        data = (data.astype(float) + head2_info['base'][0]) / head2_info['scale'][0]
        
        # 由于数据是按照左上角开始放置, 为此需要对纬度顺序进行反转
        data = np.flip(data, axis=0)
        
        # construct longitude and latitude coordinates
        latmax = head2_info['leftupLat'][0] / 100.
        lonmin = head2_info['leftupLon'][0] / 100.
        latmin = head2_info['rightdownLat'][0] / 100.
        lonmax = head2_info['rightdownLon'][0] / 100.
        hreso  = head2_info['horizontalResolution'][0] / 100.
        vreso  = head2_info['verticalResolution'][0] / 100.
        lon = np.arange(lonmin, lonmax + hreso, hreso)
        lat = np.arange(latmin, latmax + vreso, vreso)
        
        # construct time
        time = datetime(
            head2_info['startYear'][0], head2_info['startMonth'][0],
            head2_info['startDay'][0], head2_info['startHour'][0], head2_info['startMinute'][0])
        time = np.array([time], dtype='datetime64[ms]')
        
        # define element
        _element = {0: '数值预报', 1: '海面温度（K）', 2: '海冰分布', 3: '海冰密度', 4: '射出长波辐射（W/m2）',
                    5: '归一化植被指数', 6: '比值植被指数', 7: '积雪分布', 8: '土壤湿度（kg/m3）',
                    9: '日照（小时）', 10: '云顶高度（hPa）', 11: '云顶温度（K）', 12: '低云云量', 13: '高云云量',
                    14: '降水指数（mm/1小时）', 15: '降水指数（mm/6小时）', 16: '降水指数（mm/12小时）',
                    17: '降水指数（mm/24小时）', 18: '对流层中上层水汽量（相对湿度）', 19: '亮度温度',
                    20: '云总量（百分比）', 21: '云分类', 22: '降水估计（mm/6小时）', 23: '降水估计（mm/24小时）',
                    24: '晴空大气可降水（mm）', 25: '备用', 26: '地面入射太阳辐射（W/m2）', 27: '备用', 28: '备用',
                    29: '备用', 30: '备用', 31: '1000hPa相对湿度', 32: '850hPa相对湿度', 33: '700hPa相对湿度',
                    34: '600hPa相对湿度', 35: '500hPa相对湿度', 36: '400hPa相对湿度', 37: '300hPa相对湿度'}
        
        # define coordinates
        time_coord = ('time', time)
        lon_coord = ('lon', lon, {
            'long_name':'longitude', 'units':'degrees_east',
            '_CoordinateAxisType':'Lon', 'axis': "X"})
        lat_coord = ('lat', lat, {
            'long_name':'latitude', 'units':'degrees_north',
            '_CoordinateAxisType':'Lat', 'axis': "Y"})

        # create xarray
        data = data[np.newaxis, ...]
        varattrs = {
            'productCategory': head1_info['productCategory'][0],   # 产品类型, 1:静止, 2:极轨, 3:格点, 4:离散, 5:图形和分析
            'formatString': head1_info['formatString'][0],         # 产品格式名称
            'qualityFlag': head1_info['qualityFlag'][0],           # 产品质量标识
            'satelliteName': head2_info['satelliteName'][0],       # 卫星名称
            'element': _element[head2_info['element'][0]],         # 格点场要素
            'units': units}
        data = xr.Dataset({
            'image':(['time', 'lat', 'lon'], data, varattrs)},
            coords={ 'time':time_coord, 'lat':lat_coord, 'lon':lon_coord})

        # add attributes
        data.attrs['Conventions'] = "CF-1.6"
        data.attrs['Origin'] = 'MICAPS Cassandra Server'

        # return
        return data
    else:
        print("The productCategory is not supported.")
        return None


def read_fy_awx(fname, units=''):
    """
    Read satellite awx format file.

    :param fname: file pathname.
    :return: data list

    :Example:
    >>> data = read_fy_awx("./examples/samples/ANI_IR1_R04_20220331_2100_FY2G.AWX")
    >>> data = read_fy_awx("./examples/samples/FY2E_CTA_MLT_OTG_20170126_0130.AWX")
    """

    # read part of binary
    # refer to
    # https://stackoverflow.com/questions/14245094/how-to-read-part-of-binary-file-with-numpy

    # open file
    with open(fname, 'rb') as fh:
        # read file content
        ba = bytearray(fh.read())       
        return resolve_awx_bytearray(ba, units=units)


def read_himawari(fname, resolution):
    """
    Read the japan himawari satellite standard data file.
    refere to
      https://github.com/smft/Read_Himawari_binary_data/blob/master/read_Himawari.py

    :param fname: data file pathname.
    :param resolution: data resolution.

    :return: data list.
    """

    # define resolution
    if resolution == 1:
        res = 12100000
        nlin = 1100
        ncol = 11000
    elif resolution == 2:
        res = 3025000
        nlin = 550
        ncol = 5500
    else:
        res = 48400000
        nlin = 2200
        ncol = 22000

    # define binary file format
    formation = [('bn', 'i1', 1),
                 ('bl', 'i2', 1),
                 ('thb', 'i2', 1),
                 ('bo', 'i1', 1),
                 ('sn', 'S1', 16),
                 ('pcn', 'S1', 16),
                 ('oa', 'S1', 4),
                 ('obf', 'S1', 2),
                 ('ot', 'i2', 1),
                 ('ost', 'float64', 1),
                 ('oet', 'float64', 1),
                 ('fct', 'float64', 1),
                 ('thl', 'i4', 1),
                 ('tdl', 'i4', 1),
                 ('qf1', 'i1', 1),
                 ('qf2', 'i1', 1),
                 ('qf3', 'i1', 1),
                 ('qf4', 'i1', 1),
                 ('ffv', 'S1', 32),
                 ('fn', 'S1', 128),
                 ('null1', 'S1', 40),
                 ('bn2', 'i1', 1),
                 ('bl2', 'i2', 1),
                 ('nbpp', 'i2', 1),
                 ('noc', 'i2', 1),
                 ('nol', 'i2', 1),
                 ('cffdb', 'i1', 1),
                 ('null2', 'S1', 40),
                 ('bn3', 'i1', 1),
                 ('bl3', 'i2', 1),
                 ('sl', 'float64', 1),
                 ('CFAC', 'i4', 1),
                 ('LFAC', 'i4', 1),
                 ('COFF', 'float32', 1),
                 ('LOFF', 'float32', 1),
                 ('dfectvs', 'float64', 1),
                 ('eer', 'float64', 1),
                 ('epr', 'float64', 1),
                 ('var1', 'float64', 1),
                 ('var2', 'float64', 1),
                 ('var3', 'float64', 1),
                 ('cfsd', 'float64', 1),
                 ('rt', 'i2', 1),
                 ('rs', 'i2', 1),
                 ('null3', 'S1', 40),
                 ('bn4', 'i1', 1),
                 ('bl4', 'i2', 1),
                 ('ni', 'float64', 1),
                 ('ssplon', 'float64', 1),
                 ('ssplat', 'float64', 1),
                 ('dfects4', 'float64', 1),
                 ('nlat', 'float64', 1),
                 ('nlon', 'float64', 1),
                 ('sp', 'float64', 3),
                 ('mp', 'float64', 3),
                 ('null4', 'S1', 40),
                 ('bn5', 'i1', 1),
                 ('bl5', 'i2', 1),
                 ('bdn', 'i2', 1),
                 ('cwl', 'float64', 1),
                 ('vnobpp', 'i2', 1),
                 ('cvoep', 'uint16', 1),
                 ('cvoposa', 'uint16', 1),
                 ('gfcce', 'float64', 1),
                 ('cfcce', 'float64', 1),
                 ('c0', 'float64', 1),
                 ('c1', 'float64', 1),
                 ('c2', 'float64', 1),
                 ('C0', 'float64', 1),
                 ('C1', 'float64', 1),
                 ('C2', 'float64', 1),
                 ('sol', 'float64', 1),
                 ('pc', 'float64', 1),
                 ('bc', 'float64', 1),
                 ('null5', 'S1', 40),
                 ('b06n01', 'i1', 1),
                 ('b06n02', 'i2', 1),
                 ('b06n03', 'float64', 1),
                 ('b06n04', 'float64', 1),
                 ('b06n05', 'float64', 1),
                 ('b06n06', 'float64', 1),
                 ('b06n07', 'float64', 1),
                 ('b06n08', 'float64', 1),
                 ('b06n09', 'float64', 1),
                 ('b06n10', 'float64', 1),
                 ('b06n11', 'float32', 1),
                 ('b06n12', 'float32', 1),
                 ('b06n13', 'S1', 128),
                 ('b06n14', 'S1', 56),
                 ('b07n01', 'i1', 1),
                 ('b07n02', 'i2', 1),
                 ('b07n03', 'i1', 1),
                 ('b07n04', 'i1', 1),
                 ('b07n05', 'i2', 1),
                 ('b07n06', 'S1', 40),
                 ('b08n01', 'i1', 1),
                 ('b08n02', 'i2', 1),
                 ('b08n03', 'float32', 1),
                 ('b08n04', 'float32', 1),
                 ('b08n05', 'float64', 1),
                 ('b08n06', 'i2', 1),
                 ('b08n07', 'i2', 1),
                 ('b08n08', 'float32', 1),
                 ('b08n09', 'float32', 1),
                 ('b08n10', 'S1', 50),
                 ('b09n01', 'i1', 1),
                 ('b09n02', 'i2', 1),
                 ('b09n03', 'i2', 1),
                 ('b09n04', 'i2', 1),
                 ('b09n05', 'float64', 1),
                 ('b09n06', 'S1', 70),
                 ('b10n01', 'i1', 1),
                 ('b10n02', 'i4', 1),
                 ('b10n03', 'i2', 1),
                 ('b10n04', 'i2', 1),
                 ('b10n05', 'i2', 1),
                 ('b10n06', 'S1', 36),
                 ('b11n01', 'i1', 1),
                 ('b11n02', 'i2', 1),
                 ('b11n03', 'S1', 256),
                 ('b12n01', 'i2', res)]

    data = np.fromfile(fname, dtype=formation)['b12n01'].reshape(nlin, ncol)

    return list(data)
