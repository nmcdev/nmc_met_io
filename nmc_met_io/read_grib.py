# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

import os
import sys
import pickle
import bz2
from pathlib import Path
import xarray as xr

try:
    import cfgrib
except ImportError:
    print("cfgrib not installed (conda install -c conda-forge cfgrib)")
    sys.exit(1)


def read_fnl_grib2(filename):
    """
    Read fnl analysis data file.
    
    Args:
        filename (string): file path name.

    Return:
        A list of xarray object.
    """

    return cfgrib.open_datasets(filename)


def read_ecmwf_ens_efi(filename, short_name='tpi', init_hour=0,
                       data_type='efi', number=0, cache_dir=None):
    """
    Read ECMWF ensemble extreme forecast index grib file.
    https://confluence.ecmwf.int/display/FUG/EFI+Charts
    
    Explore the Grib file with:
      grib_ls -w dataType=tpi,stepRange='12-36',number=90 2020092812.EFI.240
      grib_ls -w dataType=sot,stepRange='12-36',number=90 2020092812.EFI.240
    
    Args:
        filename (str): Grib file path name.

        shortName (str): variable name string,
          tpi         total precipiation EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7, 
                        1-3, 2-4, 3-5, 4-6, 5-7, 6-8, 7-9, 1-5, 2-6, 3-7, 4-8, 5-9, 1-10
          2ti         mean 2m temperature EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7, 
                        1-3, 2-4, 3-5, 4-6, 5-7, 6-8, 7-9, 1-5, 2-6, 3-7, 4-8, 5-9, 1-10
          10wsi       wind speed EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7,
                        1-3, 2-4, 3-5, 4-6, 5-7, 6-8, 7-9, 1-5, 2-6, 3-7, 4-8, 5-9, 1-10
          capesi      CAPE-shear EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7.
          capei       CAPE EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7.
          10fgi       maximum wind gust EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7.
          sfi         snowfall EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7.
          mn2ti       minimum 2m temperature EFI  for the periods: Days 1, 2, 3, 4, 5, 6, 7.
          mx2ti       maximum 2m temperature EFI for the periods: Days 1, 2, 3, 4, 5, 6, 7.

        init_hour (int): initial hour, 0 or 12.

        dataType (str): data type, efi:  extreme forecast index; sot: shift of tail.

        number (int): efi is set to 0, but for sot it has important meaning, 
                      10(90) for 10th(90th) percentile of the forecast distribution.
          
    Return:
        xarray object.
    """
    
    # check the number
    if data_type == 'efi':
        number = 0

    # check cache_dir
    if cache_dir is not None:
        cache_file = os.path.join(
            cache_dir, os.path.basename(filename) + '_' + short_name + '_' + 
            str(init_hour).zfill(2) + '_' + data_type + '_' + str(number).zfill(2)+'.pkl')
        if os.path.isfile(cache_file):
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
                return data

    # set step range
    if short_name in ['tpi', '2ti', '10wsi']:
        if init_hour == 0:
            step_range = [
                '0-24', '24-48', '48-72', '72-96', '96-120', '120-144', '144-168',
                '0-72', '24-96', '48-120', '72-144', '96-168', '120-192', '144-216',
                '0-120', '24-144', '48-168', '72-192', '96-216', '0-240']
        else:
            step_range = [
                '12-36', '36-60', '60-84', '84-108', '108-132', '132-156', '156-180',
                '12-84', '36-108', '60-132', '84-156', '108-180', '132-204', '156-228',
                '12-132', '36-156', '60-180', '84-204', '108-228', '0-240']
    else:
        if init_hour == 0:
            step_range = [
                '0-24', '24-48', '48-72', '72-96', '96-120', '120-144', '144-168']
        else:
            step_range = [
                '12-36', '36-60', '60-84', '84-108', '108-132', '132-156', '156-180']
    
    # read data to list
    data = []
    for srange in step_range:
        # set filter keys
        try:
            filter_by_keys = {
                'shortName': short_name, 'dataType': data_type,
                'number': number, 'stepRange': srange}

            ds = xr.open_dataset(
                filename, engine='cfgrib',
                backend_kwargs={'filter_by_keys': filter_by_keys, 'read_keys': ['stepRange']})
        except:
            print('Can not read data from file '+ filename)
            return None
        ds.coords['stepRange'] = srange
        data.append(ds)
    
    # concatenate the xarray list
    data = xr.concat(data, dim='stepRange')

    # cache data
    if cache_dir is not None:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    return data


def read_ecmwf_C1D(filename, shortName='2t', typeOfLevel='surface'):
    """
    利用cfgrib读取ECMWF数值模式的C1D数据.
    
    grib_ls -w typeOfLevel=surface filename    # 地面变量
    edition      centre       typeOfLevel  level        dataDate     stepRange    dataType     shortName     
    1            ecmf         surface      0            20210710     3            fc           100u         
    1            ecmf         surface      0            20210710     3            fc           cp           
    1            ecmf         surface      0            20210710     3            fc           tcc          
    1            ecmf         surface      0            20210710     3            fc           tp           
    1            ecmf         surface      0            20210710     3            fc           10u          
    1            ecmf         surface      0            20210710     3            fc           10v          
    1            ecmf         surface      0            20210710     3            fc           sst          
    1            ecmf         surface      0            20210710     3            fc           sp           
    1            ecmf         surface      0            20210710     3            fc           cape         
    1            ecmf         surface      0            20210710     3            fc           lsp          
    1            ecmf         surface      0            20210710     3            fc           sd           
    1            ecmf         surface      0            20210710     3            fc           lcc          
    1            ecmf         surface      0            20210710     3            fc           sf           
    1            ecmf         surface      0            20210710     3            fc           skt          
    1            ecmf         surface      0            20210710     3            fc           100v         
    1            ecmf         surface      0            20210710     3            fc           msl          
    1            ecmf         surface      0            20210710     3            fc           2d           
    1            ecmf         surface      0            20210710     3            fc           2t           
    1            ecmf         surface      0            20210710     3            fc           vis          
    1            ecmf         surface      0            20210710     0-3          fc           10fg3        
    1            ecmf         surface      0            20210710     3            fc           tcwv         
    1            ecmf         surface      0            20210710     3            fc           deg0l        
    1            ecmf         surface      0            20210710     3            fc           rsn          
    2            ecmf         surface      0            20210710     3            fc           ptype        
    1            ecmf         surface      0            20210710     0-3          fc           mx2t3        
    1            ecmf         surface      0            20210710     0-3          fc           mn2t3        
    1            ecmf         surface      0            20210710     3            fc           fal          
    1            ecmf         surface      0            20210710     3            fc           tcw          
    1            ecmf         surface      0            20210710     3            fc           fzra         
    1            ecmf         surface      0            20210710     3            fc           capes  
    
    grib_ls -w typeOfLevel=isobaricInhPa,level=500 W_NAFP_C_ECMF_20210710054805_P_C1D07100000071003001    # 高空变量
    edition      centre       typeOfLevel  level        dataDate     stepRange    dataType     shortName    packingType  gridType     
    1            ecmf         isobaricInhPa  500          20210710     3            fc           r            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           t            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           gh           grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           v            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           u            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           w            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           q            grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           pv           grid_simple  regular_ll  
    1            ecmf         isobaricInhPa  500          20210710     3            fc           d            grid_simple  regular_ll  

    Args:
        filename (str): grib文件路径名称
        shortName (str, optional): 变量名称, 可以用grib_ls或grib_dump命令查询. Defaults to '2t'.
        typeOfLevel (str, optional): 层次类型, 分为"surface"和"isobaricInhPa"两种. Defaults to 'surface'.
        
    Return:
        Xarray dataset.
    """
    
    # 检查文件是否是bz2的压缩格式, 解压缩为bz2格式
    if Path(filename).suffix == '.bz2':
        outfile = filename[:-4]
        if not os.path.exists(outfile):    # 如果解压缩文件不存在, 则解压文件
            with open(filename, 'rb') as source, open(outfile, 'wb') as dest:
                print("解压缩bz2文件为{}".format(outfile))
                dest.write(bz2.decompress(source.read()))
    else:
        outfile = filename
    
    # 读取变量数据
    ds = xr.open_dataset(
        outfile, engine='cfgrib', 
        backend_kwargs={'filter_by_keys': {'typeOfLevel': typeOfLevel, 'shortName':shortName},
                        'read_keys': ['stepRange', 'dataDate']})
    return ds
    