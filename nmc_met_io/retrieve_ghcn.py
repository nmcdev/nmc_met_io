# _*_ coding: utf-8 _*_

# Copyright (c) 2020 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
A set of Python tools to make it earsier to extract weather station data from
the Global Historical Climatology Network Daily (GHCND).

refer to:
https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt or
ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
https://github.com/scott-hosking/get-station-data
"""

import numpy as np
import pandas as pd
from datetime import datetime

missing_id = '-9999'


def get_ghcnd_data(my_stns):
    """
    Retrieve ghcnd data from 
    
    Args:
        my_stns ([type]): [description]
    
    Raises:
        ValueError: [description]
    
    Returns:
        [type]: [description]
    """

    stn_md = get_ghcnd_stn_metadata()
    dfs = []

    for stn_id in pd.unique(my_stns['station']):

        stn_md1 = stn_md[ stn_md['station'] == stn_id ]
        lat     = stn_md1['lat'].values[0]
        lon     = stn_md1['lon'].values[0]
        elev    = stn_md1['elev'].values[0]
        name    = stn_md1['name'].values[0]

        file = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/all/'+stn_id+'.dly'
        df   = _create_DataFrame_1stn(file)

        if len(pd.unique(df['station'])) == 1:
            df['lon']  = lon
            df['lat']  = lat
            df['elev'] = elev
            df['name'] = name
        else:
            raise ValueError('more than one station ID in file')

        dfs.append(df)

    df = pd.concat(dfs)

    df = df.replace(-999.0, np.nan)

    return df


def get_ghcnd_stn_metadata(fname=None):
    """
    Get the ghcnd station metadata from ghcnd-stations.txt.
    China station start with "CHM000...", like "CHM00054511"
    
    Args:
        fname ([type], optional): [description]. Defaults to None.
    
    Returns:
        [type]: [description]
    """
    url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
    if fname == None:
        fname = url
    md = pd.read_fwf(fname, colspecs=[(0,12), (12,21), (21,31), 
                                            (31,38), (38,69)], 
                        names=['station','lat','lon','elev','name'])
    return md


def _create_DataFrame_1stn(filename, verbose=False):

    ### read all data
    lines     = np.genfromtxt(filename, delimiter='\n', dtype='str')
    nlines    = len(lines)
    linewidth = lines.dtype.itemsize

    ### initialise arrays & lists
    year    = np.zeros( nlines*31 ).astype(int)
    month   = np.zeros( nlines*31 ).astype(int)
    day     = np.zeros( nlines*31 ).astype(int)
    value   = np.zeros( nlines*31 )
    stn_id  = []
    element = []
    mflag   = []
    qflag   = []
    sflag   = []

    ### Loop through all lines in input file
    i = 0 ### start iteration from zero

    warnings = []

    for line_tmp in lines:

        ### return a string of the correct width, left-justified
        line = line_tmp.ljust(linewidth)

        ### extract values from original line
        ### each new index (i) represents a different day for this 
        ### line (i.e., year and station)
        for d in range(0,31):

            stn_id.append(line[0:11])
            year[i]    = line[11:15]
            month[i]   = line[15:17]
            day[i]     = d+1
            element_tmp = line[17:21]
            element.append(element_tmp)

            ### get column positions for daily data
            cols = np.array([21, 26, 27, 28]) + (8*d)
        
            val_tmp  = line[ cols[0]:cols[1] ]

            if val_tmp == missing_id: 
                value[i] = val_tmp
            elif element_tmp in ['PRCP', 'TMAX', 'TMIN', 'AWND', 'EVAP', 
                                'MDEV', 'MDPR', 'MDTN', 'MDTX', 
                                'MNPN', 'MXPN']:
                ### these are in tenths of a UNIT
                ### (e.g., tenths of degrees C)
                warnings.append(element_tmp+\
                        ' values have been divided by ten' + \
                        ' as specified by readme.txt')
                value[i] = np.float(val_tmp) / 10.
            else:
                value[i] = np.float(val_tmp)
            
            mflag.append(line[ cols[1] ])
            qflag.append(line[ cols[2] ])
            sflag.append(line[ cols[3] ])

            i = i + 1 ### interate by line and by day

    ### Print any warnings
    warnings = np.unique(np.array(warnings))
    if verbose ==True:
        for w in warnings: print(w)

    ### Convert to Pandas DataFrame
    df = pd.DataFrame(columns=['station', 'year', 'month', 'day',
                                'element', 'value',
                                'mflag', 'qflag', 'sflag'])

    df['station'] = stn_id
    df['year']    = year
    df['month']   = month
    df['day']     = day
    df['element'] = element
    df['value']   = value
    df['mflag']   = mflag
    df['qflag']   = qflag
    df['sflag']   = sflag

    df = df.replace(-9999.0, np.nan)

    ### Test validity of dates and add datetime column
    dt = []
    for index, row in df.iterrows():
        try:
            dt.append( datetime(row['year'], row['month'], row['day']) )
        except ValueError:
            # print('Date does not exist:'+\
            #                     str(row['year'])+'-'+\
            #                     str(row['month'])+'-'+\
            #                     str(row['day']) )
            dt.append( np.nan )

    df['date'] = dt
    df = df.dropna( subset=['date'] )

    return df
