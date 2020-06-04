# 气象数据读写及访问程序库
提供对MICAPS文件, 卫星云图, 天气雷达等数据的读写, 并访问CMADaaS, CIMISS和MICAPS CASSANDRA数据库文件等.

* 相应的Jupyter例子文件请见[Examples](https://nbviewer.jupyter.org/github/nmcdev/nmc_met_io/tree/master/examples/)
* 若有问题或需求, 请在[登录留言](https://github.com/nmcdev/nmc_met_io/issues)
* 程序库更新, 请见[更新日志](https://github.com/nmcdev/nmc_met_io/wiki/%E6%9B%B4%E6%96%B0%E6%97%A5%E5%BF%97)

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

若需要实现对grib格式数据的读取, 请安装:
- eccodes, 使用`conda install -c conda-forge eccodes`命令(ECMWF的grib工具库, 支持Windows和Linux)
- cfgrib, 使用`conda install -c conda-forge cfgrib`命令

若需要使用将标准雷达格式转化为pyart格式程序`standard_data_to_pyart`, 请安装:
- arm_pyart, conda install -c conda-forge arm_pyart

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


## 设置CIMISS、CMADaaS或MICAPS服务器的地址及用户信息
若要访问CIMISS、CMADaaS或MICAPS服务器, 在配置文件中设置地址和用户信息(若不需要, 则相应项无需配置). 在系统用户目录下("C:\Users\用户名"(windows)或"/home/用户名/"(Linux)), 建立文件夹".nmcdev", 并在里面创建文本文件"config.ini", 内容模板为:
```
[CIMISS]
DNS = xx.xx.xx.xx
USER_ID = xxxxxxxxx
PASSWORD = xxxxxxxx

[CMADaaS]
DNS = xx.xx.xx.xx
PORT = xx
USER_ID = xxxxxxxxx
PASSWORD = xxxxxxxx
serviceNodeId = NMIC_MUSIC_CMADAAS

[MICAPS]
GDS_IP = xx.xx.xx.xx
GDS_PORT = xxxx

# Cached file directory, if not set,
#   /home/USERNAME/.nmcdev/cache (linux) or C:/Users/USERNAME/.nmcdev/cache (windows) will be used.
[CACHE]
# CACHE_DIR = ~ 

[MAPBOX]
token = pk.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
这里xxxx用相应的地址, 接口和用户信息代替. 如果要用到MAPBOX地图, 可以申请[access token](https://docs.mapbox.com/help/glossary/access-token).

---
