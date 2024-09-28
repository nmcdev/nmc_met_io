#!/usr/bin/python3.5
# -*- coding:UTF-8 -*-
import numpy as np
import os
import nmc_met.api.DataBlock_pb2 as DataBlock_pb2
import nmc_met.api.GDS_data_service as GDS_data_service
import struct
import math
import xarray as xr
import pandas as pd

def grid_ragular(slon,dlon,elon,slat,dlat,elat):
    slon1 = slon
    dlon1 = dlon
    elon1 = elon
    slat1 = slat
    dlat1 = dlat
    elat1 = elat
    nlon = 1 + (elon - slon) / dlon
    error = abs(round(nlon) - nlon)
    if (error > 0.01):
        nlon1 = math.ceil(nlon)
    else:
        nlon1 = int(round(nlon))
    elon1 = slon1 + (nlon - 1) * dlon
    nlat = 1 + (elat - slat) / dlat
    error = abs(round(nlat) - nlat)
    if (error > 0.01):
        nlat1 = math.ceil(nlat)
    else:
        nlat1 = int(round(nlat))
    elat1 = slat1 + (nlat - 1) * dlat
    return slon1,dlon1,elon1,slat1,dlat1,elat1,nlon1,nlat1

def read_from_micaps4(filename):
    try:
        if not os.path.exists(filename):
            print(filename + " is not exist")
            return None
        try:
            file = open(filename,'r')
            str1 = file.read()
            file.close()
        except:
            file = open(filename,'r',encoding='utf-8')
            str1 = file.read()
            file.close()
        strs = str1.split()
        dlon = float(strs[9])
        dlat = float(strs[10])
        slon = float(strs[11])
        elon = float(strs[12])
        slat = float(strs[13])
        elat = float(strs[14])
        nlon = int(strs[15])
        nlat = int(strs[16])
        elon = slon + dlon * (nlon -1)
        elat = slat + dlat * (nlat -1)
        slon1, dlon1, elon1, slat1, dlat1, elat1, nlon1, nlat1 = grid_ragular(slon,dlon,elon,slat,dlat,elat)
        if len(strs) - 22 >= nlon1 * nlat1 :
            k=22
            dat = (np.array(strs[k:])).astype(float).reshape((1,1,1,nlat1,nlon1))
            lon = np.arange(nlon1) * dlon1 + slon1
            lat = np.arange(nlat1) * dlat1 + slat1
            times = pd.date_range('2000-01-01', periods=1)
            da = xr.DataArray(dat,coords = {'member':[0],'time':times,'level':[0],
                                             'latitude':lat,'longitude':lon},
                               dims= ['member','time','level','latitude','longitude'])
            return da
        else:
            return None
    except:
        print(filename + "'s format is wrong")
        return None

def read_from_gds_file(filename):
    print("a")
    try:
        if not os.path.exists(filename):
            print(filename + " is not exist")
            return None
        file = open(filename, 'rb')
        byteArray = file.read()
        discriminator = struct.unpack("4s", byteArray[:4])[0].decode("gb2312")
        t = struct.unpack("h", byteArray[4:6])
        mName = struct.unpack("20s", byteArray[6:26])[0].decode("gb2312")
        eleName = struct.unpack("50s", byteArray[26:76])[0].decode("gb2312")
        description = struct.unpack("30s", byteArray[76:106])[0].decode("gb2312")
        level, y, m, d, h, timezone, period = struct.unpack("fiiiiii", byteArray[106:134])
        startLon, endLon, lonInterval, lonGridCount = struct.unpack("fffi", byteArray[134:150])
        startLat, endLat, latInterval, latGridCount = struct.unpack("fffi", byteArray[150:166])
        isolineStartValue, isolineEndValue, isolineInterval = struct.unpack("fff", byteArray[166:178])
        gridCount = lonGridCount * latGridCount
        description = mName.rstrip('\x00') + '_' + eleName.rstrip('\x00') + "_" + str(
            level) + '(' + description.rstrip('\x00') + ')' + ":" + str(period)
        if (gridCount == (len(byteArray) - 278) / 4):
            if (startLat > 90): startLat = 90.0
            if (startLat < -90): startLat = -90.0
            if (endLat > 90): endLat = 90.0
            if (endLat < -90): endLat = -90.0
            slon1, dlon1, elon1, slat1, dlat1, elat1, nlon1, nlat1 = grid_ragular(startLon, lonInterval, endLon,
                                                                                   startLat, latInterval, endLat)
            dat = np.frombuffer(byteArray[278:], dtype='float32').reshape((1, 1, 1, nlat1, nlon1))
            lon = np.arange(nlon1) * dlon1 + slon1
            lat = np.arange(nlat1) * dlat1 + slat1
            times = pd.date_range('2000-01-01', periods=1)
            da = xr.DataArray(dat, coords={'member': [0], 'time': times, 'level': [0],
                                           'latitude': lat, 'longitude': lon},
                              dims=['member', 'time', 'level', 'latitude', 'longitude'])
            return da
    except Exception as e:
        print(e)
        return None

def read_from_gds(filename,service = None):

    try:
        if(service is None):service = GDS_data_service.service
        directory,fileName = os.path.split(filename)
        status, response = byteArrayResult = service.getData(directory, fileName)
        ByteArrayResult = DataBlock_pb2.ByteArrayResult()
        if status == 200:
            ByteArrayResult.ParseFromString(response)
            if ByteArrayResult is not None:
                byteArray = ByteArrayResult.byteArray

                file_type = os.path.splitext(filename)
                if file_type[1] == '.AWX':
                    return explain_awx_bytes(byteArray)
                else:
                    #print(len(byteArray))
                    discriminator = struct.unpack("4s", byteArray[:4])[0].decode("gb2312")
                    t = struct.unpack("h", byteArray[4:6])
                    mName = struct.unpack("20s", byteArray[6:26])[0].decode("gb2312")
                    eleName = struct.unpack("50s", byteArray[26:76])[0].decode("gb2312")
                    description = struct.unpack("30s", byteArray[76:106])[0].decode("gb2312")
                    level, y, m, d, h, timezone, period = struct.unpack("fiiiiii", byteArray[106:134])
                    startLon, endLon, lonInterval, lonGridCount = struct.unpack("fffi", byteArray[134:150])
                    startLat, endLat, latInterval, latGridCount = struct.unpack("fffi", byteArray[150:166])
                    isolineStartValue, isolineEndValue, isolineInterval = struct.unpack("fff", byteArray[166:178])
                    gridCount = lonGridCount * latGridCount
                    description = mName.rstrip('\x00') + '_' + eleName.rstrip('\x00') + "_" + str(
                        level) + '(' + description.rstrip('\x00') + ')' + ":" + str(period)
                    if (gridCount == (len(byteArray) - 278) / 4):
                        if(startLat > 90):startLat = 90.0
                        if(startLat < -90) : startLat = -90.0
                        if(endLat > 90) : endLat = 90.0
                        if(endLat < -90): endLat = -90.0

                        slon1, dlon1, elon1, slat1, dlat1, elat1, nlon1, nlat1 = grid_ragular(startLon,lonInterval,endLon,startLat,latInterval,endLat)
                        dat = np.frombuffer(byteArray[278:], dtype='float32').reshape((1, 1, 1, nlat1, nlon1))
                        lon = np.arange(nlon1) * dlon1 + slon1
                        lat = np.arange(nlat1) * dlat1 + slat1
                        times = pd.date_range('2000-01-01', periods=1)
                        da = xr.DataArray(dat, coords={'member': [0], 'time': times, 'level': [0],
                                                       'latitude': lat, 'longitude': lon},
                                          dims=['member', 'time', 'level', 'latitude', 'longitude'])
                        return da
    except Exception as e:
        print(e)
        return None
def read_from_awx(filename):
    if os.path.exists(filename):
        file = open(filename,'rb')
        byte_array = file.read()
        return explain_awx_bytes(byte_array)
    else:
        return None
def explain_awx_bytes(byteArray):
    sat96 = struct.unpack("12s", byteArray[:12])[0]
    levl = np.frombuffer(byteArray[12:30], dtype='int16').astype(dtype = "int32")
    formatstr = struct.unpack("8s", byteArray[30:38])[0]
    qualityflag = struct.unpack("h", byteArray[38:40])[0]
    satellite = struct.unpack("8s", byteArray[40:48])[0]
    lev2 = np.frombuffer(byteArray[48:104], dtype='int16').astype(dtype = "int32")

    recordlen = levl[4]
    headnum = levl[5]
    datanum = levl[6]
    timenum =lev2[0:5]
    nlon = lev2[7]
    nlat = lev2[8]
    range =lev2[12:16].astype("float32")
    slat = range[0]/100
    elat = range[1]/100
    slon = range[2]/100
    elon = range[3]/100

    #nintels=lev2[20:22].astype("float32")
    dlon = (elon - slon) / (nlon-1)
    dlat = (elat - slat) / (nlat-1)

    slon1, dlon1, elon1, slat1, dlat1, elat1, nlon1, nlat1 = grid_ragular(slon, dlon, elon, slat, dlat, elat)

    colorlen = lev2[24]
    caliblen = lev2[25]
    geololen = lev2[26]

    #print(levl)
    #print(lev2)
    head_lenght = headnum * recordlen
    data_lenght = datanum * recordlen
    #print(head_lenght  + data_lenght)
    #print( data_lenght)
    #print(grd.nlon * grd.nlat)
    #headrest = np.frombuffer(byteArray[:head_lenght], dtype='int8')
    data_awx = np.frombuffer(byteArray[head_lenght:(head_lenght+data_lenght)], dtype='int8')
    #print(headrest)

    if colorlen<=0:
        calib = np.frombuffer(byteArray[104:(104+2048)], dtype='int16').astype(dtype="float32")
    else:
        #color = np.frombuffer(byteArray[104:(104+colorlen*2)], dtype='int16')
        calib = np.frombuffer(byteArray[(104+colorlen*2):(104+colorlen*2+ 2048)], dtype='int16').astype(dtype="float32")

    realcalib = calib /100.0
    realcalib[calib<0] = (calib[calib<0] + 65536) /100.0

    awx_index = np.empty(len(data_awx),dtype = "int32")
    awx_index[:] = data_awx[:]
    awx_index[data_awx <0] = data_awx[data_awx <0] +256
    awx_index *= 4
    real_data_awx = realcalib[awx_index]

    dat = real_data_awx.astype(float).reshape((1, 1, 1,  nlat1, nlon1))
    lon = np.arange(nlon1) * dlon1 + slon1
    lat = np.arange(nlat1) * dlat1 + slat1
    times = pd.date_range('2000-01-01', periods=1)
    da = xr.DataArray(dat, coords={'member': [0], 'time': times, 'level': [0],
                                   'latitude': lat, 'longitude': lon},
                      dims=['member', 'time', 'level', 'latitude', 'longitude'])
    return da

