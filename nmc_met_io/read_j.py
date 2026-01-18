# -*- coding: utf-8 -*-

# Copyright (c) 2023 longtsing.
# Distributed under the terms of the GPL V3 License.

"""
The Jfile Reader module which get data from Jfile(only work with the standard J file)

Change Log:
    - Created by 王清龙/湖北/宜昌, 2023/06/08, e-mail:songofsongs@vip.qq.com
"""
import numpy as np
import pandas as pd 
import calendar
from datetime import datetime,timedelta
import dateutil.rrule


def ReadJfile(jfile:str):
    """
    读取J文件,取得内部元数据和气象观测数据
    返回值为存储了J文件元数据和观测数据的dict
    
    Args:
        jfile (string): J文件的地址.

    Return:
        {
            metadata：J文件中的元数据(dict 格式)
            minutely_data:分钟级数据（pandas.DataFrame 格式）
        }

    """
    #region 读取J文件文本
    f=open(jfile,encoding='gb18030', errors='ignore')
    line=f.readline()
    lines=[]
    while(line):
        line=line.strip()
        if(len(line)>0):
            lines.append(line)
        line=f.readline()
    f.close()
    #endregion
    
    #region 常量及函数定义区
    #J文件的气象要素标识符，一个字符表示一个气象要素
    OBstrs='PTURF'
    metaline=lines[0]
    sep=' '

    def txt2int(txt):
        try:
            return int(txt)
        except (ValueError, TypeError):
            return np.nan

    def txt2float(txt):
        try:
            return float(txt)
        except (ValueError, TypeError):
            return np.nan  
        

    def PT(Olines,Otype=float,dlen=60*24*28):
        ots=sep.join(Olines)
        ots=ots.replace(',',sep)
        ots=ots.replace('.',sep)
        ots=ots.replace('=',sep)
        obs=list(map(lambda x:txt2int(x),filter(lambda x:len(x)>0,ots.split(sep))))
        if(len(obs)<dlen):
            obs.extend([np.nan]*(dlen-len(obs)))
        return np.array(obs)

    def U(Olines,dlen=60*24*28):
        ots=sep.join(Olines)
        ots=ots.replace(',',sep)
        ots=ots.replace('.',sep)
        ots=ots.replace('=',sep)    
        obs=list(map(lambda x:100 if x=='%%' else txt2int(x),filter(lambda x:len(x)>0,ots.split(sep))))
        if(len(obs)<dlen):
            obs.extend([np.nan]*(dlen-len(obs)))
        return np.array(obs)
    
    def R(Olines,dlen=60*24*28):
        Rs=[]
        last_dayendi=-1
        for i in range(len(Olines)):
            line=Olines[i]
            linecount=len(line)
            if(linecount>0):
                line_d_len=len(line)//2
                line_obs=list(map(lambda x:txt2int(line[x*2:x*2+2])/10,range(line_d_len)))
                if(line_d_len<60):
                    if(line[-1] in ',.='):
                        line_obs.extend([0.0]*(60-line_d_len))                    
                Rs.extend(line_obs)       
                if(line[-1] in '.='):
                    Rs.extend([0.0]*(60*(24-(i-last_dayendi))))
                    last_dayendi=i     
        if(len(Rs)<dlen):
            Rs.extend([np.nan]*(dlen-len(Rs)))
        return np.array(Rs)          

    def F(Olines,dlen=60*24*28):
        ots=sep.join(Olines)
        ots=ots.replace(',',sep)
        ots=ots.replace('.',sep)
        ots=ots.replace('=',sep)    
        obs=list(map(lambda x:(txt2int(x[:3]),txt2int(x[3:])/10.0),filter(lambda x:len(x)>0,ots.split(sep))))
        if(len(obs)>0):
            wd,ws=zip(*obs)
        else:
            wd,ws=[],[]        
        if(len(wd)<dlen):
            wd.extend([np.nan]*(dlen-len(wd)))
        if(len(ws)<dlen):
            ws.extend([np.nan]*(dlen-len(ws)))
        return np.array(wd),np.array(ws)
        
    #endregion

    #region 读取元数据
    metadata={
        'stationcode':'#####',
        'lat':0,
        'latstr':'',
        'lon':0,
        'lonstr':'',
        'alti':0,
        'PRS_alti':0,
        'Wind_height':0,
        'height':0,
        'observation_str':'',
        'station_typestr':'',
        'ob_T':'',        
        'OBstrs':OBstrs,
        'OB_units':[
            'hPa',
            'degeeC',
            'percent',
            'mm',
            'degree, m/s'
        ],        
        'year':0,
        'month':0

    }
    stationcode,latstr,lonstr,alti,PRS_alti,Wind_height,height,observation_str,ob_T,year,month=metaline.split(' ')
    lat=txt2float(latstr[:-1])/100
    lon=txt2float(lonstr[:-1])/100
    alti=txt2float(alti)/10
    PRS_alti=txt2float(PRS_alti)/10
    Wind_height=txt2float(Wind_height)/10
    height=txt2float(height)/10
    station_typestr=observation_str[1:]
    observation_str=observation_str[0]
    year=txt2int(year)
    month=txt2int(month)
    metadata.update({
        'stationcode':stationcode,
        'lat':lat,
        'latstr':latstr,
        'lon':lon,
        'lonstr':lonstr,
        'alti':alti,
        'PRS_alti':PRS_alti,
        'Wind_height':Wind_height,
        'height':height,
        'observation_str':observation_str,
        'station_typestr':station_typestr,
        'ob_T':ob_T,        
        'OBstrs':OBstrs,
        'OB_units':[
            'hPa',
            'degeeC',
            'percent',
            'mm',
            'degree, m/s'
        ],        
        'year':year,
        'month':month,
        'Data_Time_Start':datetime(year,month,1,0,0,0)-timedelta(hours=4)+timedelta(minutes=1),
        'Data_counts':60*24*calendar.monthrange(year,month)[1]
    })
    #endregion
    
    #region 读取数据
    Dts=list(dateutil.rrule.rrule(dateutil.rrule.MINUTELY,dtstart=metadata['Data_Time_Start'],count=metadata['Data_counts']))
    OB_istarts=[]
    for i in range(1,len(lines)):
        if(lines[i][0] in OBstrs and len(lines[i])<10):
            OB_istarts.append(i)
    
    Ps=PT(lines[OB_istarts[0]+1:OB_istarts[1]],dlen=metadata['Data_counts'])/10.0
    Ts=PT(lines[OB_istarts[1]+1:OB_istarts[2]],dlen=metadata['Data_counts'])/10.0
    Us=U(lines[OB_istarts[2]+1:OB_istarts[3]],dlen=metadata['Data_counts'])
    Rs=R(lines[OB_istarts[3]+1:OB_istarts[4]],dlen=metadata['Data_counts'])
    Wd,Ws=F(lines[OB_istarts[4]+1:len(lines)-1],dlen=metadata['Data_counts'])
    
    ds=pd.DataFrame({
        'Datetime':Dts,
        'PRS':Ps,
        'TEM':Ts,
        'RHU':Us,
        'PRE':Rs,
        'WIN_D_Avg_1mi':Wd,
        'WIN_S_Avg_1mi':Ws
    })
    #endregion
    
    return {
            'metadata':metadata,
            'minutely_data':ds
        }
