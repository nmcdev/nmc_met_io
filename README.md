# 气象数据读写及访问程序库
提供对MICAPS文件, 卫星云图, 天气雷达等数据的读写, 并访问CMADaaS, CIMISS和MICAPS CASSANDRA数据库文件等.

* 相应的Jupyter例子文件请见[Examples](https://nbviewer.jupyter.org/github/nmcdev/nmc_met_io/tree/master/examples/)
* 若有问题或需求, 请在[登录留言](https://github.com/nmcdev/nmc_met_io/issues)
* 程序库更新, 请见[更新日志](https://github.com/nmcdev/nmc_met_io/wiki/%E6%9B%B4%E6%96%B0%E6%97%A5%E5%BF%97)

Only Python 3 is supported.
建议安装[Anaconda](https://www.anaconda.com/products/individual)数据科学工具库,
已包括scipy, numpy, matplotlib等大多数常用科学程序库.

## Dependencies
请预先安装下列程序库:

- [Numpy](https://numpy.org/), `conda install -c conda-forge numpy`
- [Scipy](http://www.scipy.org/), `conda install -c conda-forge scipy`
- [Xarray](https://github.com/pydata/xarray), `conda install -c conda-forge xarray`
- [Pandas](http://pandas.pydata.org/), `conda install -c conda-forge pandas`
- [protobuf](https://developers.google.com/protocol-buffers/), `conda install -c conda-forge protobuf`
- [urllib3](https://urllib3.readthedocs.io/), `conda install -c conda-forge urllib3`
- [tqdm](https://github.com/tqdm/tqdm), `conda install -c conda-forge tqdm`
- [Python-dateutil](https://pypi.org/project/python-dateutil/), `conda install -c conda-forge python-dateutil`

若需要实现对grib格式数据的读取, 请安装:
- [eccodes](https://software.ecmwf.int/wiki/display/ECC/ecCodes+Home), 使用`conda install -c conda-forge eccodes`命令(ECMWF的grib工具库, 支持Windows和Linux)
- [cfgrib](https://github.com/ecmwf/cfgrib), 使用`conda install -c conda-forge cfgrib`命令

若需要使用将标准雷达格式转化为pyart格式程序`standard_data_to_pyart`, 请安装:
- [arm_pyart](http://arm-doe.github.io/pyart/), `conda install -c conda-forge arm_pyart`

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
