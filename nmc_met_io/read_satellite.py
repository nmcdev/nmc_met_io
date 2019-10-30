# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read FY satellite awx format file.
"""

import numpy as np


def read_awx_cloud(fname):
    """
    Read satellite awx format file.

    :param fname: file pathname.
    :return: data list

    :Example:
    >>> headinfo, data = read_awx_cloud("./data/ANI_IR1_R04_20191026_2100_FY2G.AWX")
    """

    # read part of binary
    # refer to
    # https://stackoverflow.com/questions/14245094/how-to-read-part-of-binary-file-with-numpy

    # open file
    with open(fname, 'rb') as fh:
        # read file content
        ba = bytearray(fh.read())

        # define head structure
        head_dtype = [
            ('SAT96', 'S12'),    # SAT96 filename
            ('byteSequence', 'i2'),    # integer number byte sequence
            ('firstClassHeadLength', 'i2'),
            ('secondClassHeadLength', 'i2'),
            ('padDataLength', 'i2'),
            ('recordLength', 'i2'),
            ('headRecordNumber', 'i2'),
            ('dataRecordNumber', 'i2'),
            ('productCategory', 'i2'),
            ('compressMethod', 'i2'),
            ('formatString', 'S8'),
            ('qualityFlag', 'i2'),
            ('satelliteName', 'S8'),
            ('year', 'i2'), ('month', 'i2'),
            ('day', 'i2'), ('hour', 'i2'),
            ('minute', 'i2'),
            ('channel', 'i2'),
            ('flagOfProjection', 'i2'),
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

        head_info = np.frombuffer(ba[0:104], dtype=head_dtype)
        ind = 104

        # head rest information
        head_rest_len = (head_info['recordLength'][0].astype(np.int) *
                        head_info['headRecordNumber'][0] - ind)
        head_rest = np.frombuffer(
            ba[ind:(ind + head_rest_len)],
            dtype='u1', count=head_rest_len)
        ind += head_rest_len

        # retrieve data records
        data_len = (head_info['dataRecordNumber'][0].astype(np.int) *
                    head_info['recordLength'][0])
        data = np.frombuffer(
            ba[ind:(ind + data_len)], dtype='u1',
            count=data_len)
        data.shape = (head_info['dataRecordNumber'][0],
                    head_info['recordLength'][0])

        # return
        return head_info, data


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
