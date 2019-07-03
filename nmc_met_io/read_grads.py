# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read grads data file.
"""

import os
import re
from datetime import datetime
import numpy as np


def read_cmp_pre_hour_grid(files, start_lon=70.05, start_lat=15.05):
    """
    Read SURF_CLI_CHN_MERGE_CMP_PRE_HOUR_GRID_0.1 data file

    :param files: a single or multiple data filenames.
    :param start_lon: region lower left corner longitude.
    :param start_lat: region lower left corner latitude.
    :return: data and time

    :Examples:
    >>> files = ("F:/201607/SURF_CLI_CHN_MERGE_CMP_"
                 "PRE_HOUR_GRID_0.10-2016070100.grd")
    >>> data, time, lon, lat = read_cmp_pre_hour_grid(files)
    """

    # sort and count data files
    if isinstance(files, str):
        files = np.array([files])
    else:
        files = np.array(files)

    # define coordinates
    lon = np.arange(700) * 0.1 + start_lon
    lat = np.arange(440) * 0.1 + start_lat

    # define variables
    data = np.full((len(files), lat.size, lon.size), np.nan)
    time = []

    # loop every data file
    for i, f in enumerate(files):
        # check file exist
        if not os.path.isfile(f):
            return None, None, None, None

        # extract time information
        ttime = re.search('\d{10}', os.path.basename(f))
        time.append(datetime.strptime(ttime.group(0), "%Y%m%d%H"))

        # read data
        try:
            tdata = np.fromfile(
                f, dtype=np.dtype('float32')).reshape(2, len(lat), len(lon))
            tdata[tdata == -999.0] = np.nan    # missing value
            data[i, :, :] = tdata[0, :, :]
        except IOError:
            print("Can not read data from "+f)
            continue

    # return value
    return data, time, lon, lat
