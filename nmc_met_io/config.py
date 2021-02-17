# _*_ coding: utf-8 _*_

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read configure file.
"""

import os
import datetime
import shutil
import configparser
from pathlib import Path


def _get_config_dir():
    """
    Get default configuration directory.
    """
    config_dir = Path.home() / ".nmcdev"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

# Global Variables
CONFIG_DIR = _get_config_dir()


def _ConfigFetchError(Exception):
    pass


def _get_config_from_rcfile():
    """
    Get configure information from config_dk_met_io.ini file.
    """
    rc = CONFIG_DIR / "config.ini"
    if not rc.is_file():
        rc = Path("~/config_met_io.ini").expanduser()
    try:
        config = configparser.ConfigParser()
        config.read(rc)
    except IOError as e:
        raise _ConfigFetchError(str(e))
    except Exception as e:
        raise _ConfigFetchError(str(e))

    return config

# Global Variables
CONFIG = _get_config_from_rcfile()


def get_cache_file(sub_dir, filename, name=None, cache_clear=True):
    """
    Get the cache file pathname.

    :param sub_dir: sub directory string.
    :param filename: cache filename
    :param name: cache name, like "MICAPS_DATA"
    :param cache_clear: if True, clear old cache folder
    """
    # get cache file directory
    # 检查配置文件中是否配置了CACHE参数, 获得缓存目录;
    # 如果没有, 默认为配置文件所在的目录.
    if CONFIG.has_option('CACHE', 'CACHE_DIR'):
        cache_dir = Path(CONFIG['CACHE']['CACHE_DIR']).expanduser() / "cache"
    else:
        cache_dir = CONFIG_DIR / "cache"
    
    # Add cache name, if neccessary
    if name is not None:
        cache_dir = cache_dir / name

    # clear old cache folders
    # 如果设置了清除缓存, 则会将缓存文件逐周存放, 并删除过去的周文件夹. 
    if cache_clear:
        # Use the week number of year as subdir
        cache_subdir1 = cache_dir / datetime.date.today().strftime("%Y%U")
        cache_subdir2 = cache_subdir1 / sub_dir
        cache_subdir2.mkdir(parents=True, exist_ok=True)

        for f in cache_dir.iterdir():
            if f != cache_subdir1:
                shutil.rmtree(f)
    else:
        cache_subdir2 = cache_dir / sub_dir

    # return cache file pathname
    cache_file = cache_subdir2 / filename
    return cache_file
