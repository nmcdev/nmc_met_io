# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

import xarray as xr
import cfgrib


def read_fnl_grib2(filename):
    """
    Read fnl analysis data file.
    
    Args:
        filename (string): file path name.

    Return:
        A list of xarray object.
    """

    return cfgrib.open_datasets(filename)
