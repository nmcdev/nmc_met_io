# 气象数据读写及访问程序库
提供对MICAPS文件, 卫星云图, 天气雷达等数据的读写, 并访问CIMISS和MICAPS CASSANDRA数据库文件等.

Only Python 3 is supported.

## Dependencies
Other required packages:

- numpy
- scipy
- xarray
- pandas
- protobuf
- urllib3
- tqdm
- python-dateutil

## Install
Using the fellowing command to install packages:
```
  pip install git+git://github.com/nmcdev/nmc_met_io.git
```

or download the package and install:
```
  git clone --recursive https://github.com/nmcdev/nmc_met_io.git
  cd nmc_met_io
  python setup.py install
```

## 设置CIMISS和MICAPS服务器的地址及用户信息
在系统用户目录下("C:\Users\用户名\\.nmcdev\\"或"/home/用户名/.nmcdev/"), 新建文本文件config.ini, 里面内容模板为:
```
[CIMISS]
DNS = xx.xx.xx.xx
USER_ID = xxxxxxxxx
PASSWORD = xxxxxxxx

[MICAPS]
GDS_IP = xx.xx.xx.xx
GDS_PORT = xxxx
# Cached file directory, if not set,
#   /home/user/.nmcdev/cache will be used.
# CACHE_DIR = ~ 
```
这里xxxx用相应的地址, 接口和用户信息代替.