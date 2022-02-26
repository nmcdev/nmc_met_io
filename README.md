# 气象数据读写及访问程序库

提供对MICAPS文件, 卫星云图, 天气雷达等数据的读写程序, 并访问CMADaaS, CIMISS和MICAPS CASSANDRA数据库文件等.

* 相应的Jupyter例子文件请见[Examples](https://nbviewer.jupyter.org/github/nmcdev/nmc_met_io/tree/master/examples/)
* 若有问题或需求, 请在[登录留言](https://github.com/nmcdev/nmc_met_io/issues)
* 程序库更新, 请见[更新日志](https://github.com/nmcdev/nmc_met_io/wiki/%E6%9B%B4%E6%96%B0%E6%97%A5%E5%BF%97)

Only Python 3 is supported.
建议安装[Anaconda](https://www.anaconda.com/products/individual)数据科学工具库,
已包括scipy, numpy, matplotlib等大多数常用科学程序库.

## Install

Using the fellowing command to install packages:

* 使用pypi安装源安装(https://pypi.org/project/nmc-met-io/)
```
  pip install nmc-met-io
```
* 若要安装Github上的开发版(请先安装[Git软件](https://git-scm.com/)):
```
  pip install git+git://github.com/nmcdev/nmc_met_io.git
```
* 或者下载软件包进行安装:
```
  git clone --recursive https://github.com/nmcdev/nmc_met_io.git
  cd nmc_met_io
  python setup.py install
```

### 可选支持库:

* 若需要实现对grib格式数据的读取, 请用conda安装:
  - [eccodes](https://software.ecmwf.int/wiki/display/ECC/ecCodes+Home), `conda install -c conda-forge eccodes`(ECMWF的grib工具库, 支持Windows和Linux)
  - [cfgrib](https://github.com/ecmwf/cfgrib), `conda install -c conda-forge cfgrib`

* 若需要使用将标准雷达格式转化为pyart格式程序`standard_data_to_pyart`, 请安装:
  - [arm_pyart](http://arm-doe.github.io/pyart/), `conda install -c conda-forge arm_pyart`

* 若需要对Cassandra集群数据库进行访问, 请安装:
  - [cassandra-driver](https://pypi.org/project/cassandra-driver/), 'pip install cassandra-driver'

## 设置配置文件
若要访问CMADaaS(大数据云), MICAPS服务器等, 需在配置文件中设置地址和用户信息(若不需要, 则相应项无需配置).

* 在系统用户目录下("C:\Users\用户名"(windows)或"/home/用户名/"(Linux)), 建立文件夹".nmcdev"(若Windows下无法直接创建, 在命令窗口中输入`mkdir .nmcdev`创建)
* 在".nmcdev"中创建文本文件"config.ini", 内容模板为:
  
```
# 用于nmc_met_io读取大数据云, MICAPS服务器等的配置文件.
# 若用不到某个服务器, 则不设置或删除改段落即可.
# 注意设置IP地址时, 不要加http等前缀信息.

# CMADaaS大数据云平台配置:
#     DNS为IP地址, PORT为端口
#     USER_ID和PASSWORD分别为用户名和密码
#     serviceNodeId为服务节点名称(一般为 NMIC_MUSIC_CMADAAS)
[CMADaaS]
DNS = xx.xx.xx.xx
PORT = xx
USER_ID = xxxxxxxx
PASSWORD = xxxxxxxx
serviceNodeId = NMIC_MUSIC_CMADAAS

# MICAPS Cassandra服务器配置(一般需要联系运维开通访问权限)
#     GDS_IP为IP地址, GDS_PORT为端口
#     可以人为设置本地数据缓存的地址CACHE_DIR, 默认为配置文件夹目录下的cache文件夹
[MICAPS]
GDS_IP = xx.xx.xx.xx
GDS_PORT = 8080
# Cached file directory, if not set,
#   /user_home/.nmcdev/cache will be used.
# CACHE_DIR = ~

# CIMISS网址及用户ID和PASSWORD, 2021年年底CIMISS停止提供服务
#     DNS为IP地址, PORT为端口
#     USER_ID和PASSWORD分别为用户名和密码
[CIMISS]
DNS = xx.xx.xx.xx
USER_ID = xxxxxxxxx
PASSWORD = xxxxxxxx

# 彩云天气API的访问口令
[CAIY]
token = xxxxxxxxxxxxxx

# MAPBOX地图数据的访问口令(nmc_met_graphics绘图可以用)
# https://docs.mapbox.com/help/glossary/access-token 申请
[MAPBOX]
token = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 天地地图数据的访问口令(nmc_met_graphics绘图可以用)
# http://lbs.tianditu.gov.cn/server/MapService.html 申请
[TIANDITU]
token = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 配置Cassandra集群访问接口
[Cassandra]
ClusterIPAddresses=Cassandra集群IP地址以“,”分隔，可以参考MICAPS4的配置文件配置
ClusterPort=Cassandra集群服务端口
KeySpace=Cassandra上数据存储的主键名

```

---
