# -*- coding: utf-8 -*-

# Copyright (c) 2019 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read CMA radar format file.

Adopt from https://github.com/CyanideCN/PyCINRAD
"""

import warnings
import abc
import datetime
from pathlib import Path
from collections import namedtuple, defaultdict
from array import array
import numpy as np
from numpy import ndarray
from typing import Union, Optional, Any, List, Generator
import re
from copy import deepcopy as dc
import bz2
import gzip

# constants
deg2rad = 3.141592653589793 / 180
rm = 8500
con = (180 / 4096) * 0.125
con2 = 0.001824  # calculated manually
vil_const = 3.44e-6


def read_cma_sa_radar(fname):
    """
    Read CMA SA radar format file.
    refer to
      https://github.com/smft/CMA_SA_RADAR_READER/blob/master/read_RADAR.py
      https://github.com/smft/CMA_SA_RADAR_READER/blob/master/barb_plot.py
    for plot map.

    :param fname: file path name.
    :return: data dictionary.

    """

    # read data
    flag = open(fname, "r")
    data = np.asarray(array("B", flag.read()))
    data = data.reshape([len(data)/2432, 2432])
    flag.close()

    # find elevation angle
    if data[0, 72] == 11:
        phi = [0.50, 0.50, 1.45, 1.45, 2.40, 3.35, 4.30,
               5.25, 6.2, 7.5, 8.7, 10, 12, 14, 16.7, 19.5]
    if data[0, 72] == 21:
        phi = [0.50, 0.50, 1.45, 1.45, 2.40, 3.35, 4.30,
               6.00, 9.00, 14.6, 19.5]
    if data[0, 72] == 31:
        phi = [0.50, 0.50, 1.50, 1.50, 2.50, 2.50, 3.50, 4.50]
    if data[0, 72] == 32:
        phi = [0.50, 0.50, 2.50, 3.50, 4.50]

    # define data
    g1 = np.zeros([len(data), 460])
    h1 = np.zeros([len(data), 460])
    i1 = np.zeros([len(data), 460])
    j1 = np.zeros([len(data), 460])

    # process data
    count = 0
    while count < len(data):
        print("径向数据编号 : ", count)
        b1 = data[count, 44] + 256 * data[count, 45]  # 仰角序数
        c1 = ((data[count, 36] + 256 * data[count, 37]) /
              8 * 180 / 4096)  # 方位角
        d1 = data[count, 54] + 256 * data[count, 55]  # 径向库
        print("仰角序数,方位角,径向库 : ", b1, c1, d1)
        if d1 == 0:
            count += 1
            continue
        else:
            count += 1
        i = 0
        while i < 460:
            g1[count - 1, i] = phi[b1 - 1]  # 仰角
            h1[count - 1, i] = c1  # 方位角
            i1[count - 1, i] = 0.5 + i - 1  # 径向
            if i > d1:  # 反射率
                j1[count - 1, i] = 0
            else:
                if data[count - 1, 128 + i] == 0:  # 无数据
                    j1[count - 1, i] = 0
                else:
                    if data[count - 1, 128 + i] == 1:  # 距离模糊
                        j1[count - 1, i] = 0
                    else:  # 数据正常
                        j1[count - 1, i] = ((data[count - 1, 128 + i] - 2) /
                                            2 - 32)
            i += 1

    # calculate angle index
    n = 3
    a2 = 0  # 仰角序数
    while a2 < len(data):
        if data[a2, 44] > (n - 1):
            break
        a2 += 1
    a3 = a2
    while a3 < len(data):
        if data[a3, 44] > n:
            break
        a3 += 1

    # put data
    yj = g1[a2:a3, :]        # 仰角
    fwj = h1[a2:a3, :]       # 方位角
    jx = i1[a2:a3, :]        # 径向
    fsl = j1[a2:a3, :]       # 反射率

    # return data
    return yj, fwj, jx, fsl


def sph2cart(elevation, azimuth, r):
    """
    Convert spherical coordinates to cartesian.
    """

    ele, a = np.deg2rad([elevation, azimuth])
    x = r * np.cos(ele) * np.cos(a)
    y = r * np.cos(ele) * np.sin(a)
    z = r * np.sin(ele)
    return x, y, z


def prepare_file(file: Any) -> Any:
    if hasattr(file, "read"):
        return file
    f = open(file, "rb")
    magic = f.read(3)
    f.close()
    if magic.startswith(b"\x1f\x8b"):
        return gzip.GzipFile(file, "rb")
    if magic.startswith(b"BZh"):
        return bz2.BZ2File(file, "rb")
    return open(file, "rb")


def merge_bytes(byte_list: List[bytes]) -> bytes:
    return b"".join(byte_list)


class Radial(object):
    r"""Structure for data arranged by radials"""

    __slots__ = [
        "data",
        "drange",
        "elev",
        "reso",
        "code",
        "name",
        "scantime",
        "dtype",
        "include_rf",
        "lon",
        "lat",
        "height",
        "a_reso",
        "stp",
        "geoflag",
        "dist",
        "az",
        "scan_info",
    ]

    def __init__(
        self,
        data: ndarray,
        drange: Union[float, int],
        elev: float,
        reso: float,
        code: str,
        name: str,
        scantime: datetime,
        dtype: str,
        stlon: float,
        stlat: float,
        lon: Optional[ndarray] = None,
        lat: Optional[ndarray] = None,
        height: Optional[ndarray] = None,
        a_reso: Optional[int] = None,
        **scan_info
    ):
        r"""
        Parameters
        ----------
        data: np.ndarray
            wrapped data
        drange: float
            radius of this data
        elev: float
            elevation angle of this data
        reso: float
            radial resolution of this data
        code: str
            code for this radar
        name: str
            name for this radar
        scantime: str
            scan time for this radar
        dtype: str
            product type
        stlon: float
            radar longitude
        stlat: float
            radar latitude
        lon: np.ndarray / bool
            longitude array for wrapped data
        lat: np.ndarray / bool
            latitude array for wrapped data
        height: np.ndarray / bool
            height array for wrapped data
        a_reso: int
            radial resolution of this data
        scan_info: dict
            scan parameters of radar
        """
        self.data = data
        self.drange = drange
        self.elev = elev
        self.reso = reso
        self.code = code
        self.name = name
        self.scantime = scantime
        self.dtype = dtype
        self.scan_info = scan_info
        if dtype == "VEL":
            if len(data) == 2:
                self.include_rf = True
            else:
                self.include_rf = False
        self.lon = lon
        self.lat = lat
        self.height = height
        self.a_reso = a_reso
        self.stp = {"lon": stlon, "lat": stlat}
        nonetype = type(None)
        if isinstance(lon, nonetype) and isinstance(lat, nonetype):
            self.geoflag = False
        else:
            self.geoflag = True

    def __repr__(self):
        repr_s = (
            "<Datatype: {} Station name: {} Scan time: {} Elevation angle: "
            + "{:.2f} Range: {}>"
        )
        return repr_s.format(
            self.dtype.upper(), self.name, self.scantime, self.elev, self.drange
        )

    def add_geoc(self, lon: ndarray, lat: ndarray, height: ndarray):
        if not lon.shape == lat.shape == height.shape:
            raise ValueError("Coordinate sizes are incompatible")
        self.lon = lon
        self.lat = lat
        self.height = height
        self.geoflag = True

    def add_polarc(self, distance: ndarray, azimuth: ndarray):
        self.dist = distance
        self.az = azimuth

    def __deepcopy__(self, memo: Any):
        r"""Used if copy.deepcopy is called"""
        r = Radial(
            dc(self.data),
            dc(self.drange),
            dc(self.elev),
            dc(self.reso),
            dc(self.code),
            dc(self.name),
            dc(self.scantime),
            dc(self.dtype),
            dc(self.stp["lon"]),
            dc(self.stp["lat"]),
            dc(self.scan_info),
        )
        if self.geoflag:
            r.add_geoc(dc(self.lon), dc(self.lat), dc(self.height))
        if hasattr(self, "dist"):
            r.add_polarc(dc(self.dist), dc(self.az))
        return r


class Slice_(object):
    r"""Structure for slice data"""

    __slots__ = ["data", "xcor", "ycor", "scantime", "dtype", "code", "name", "geoinfo"]

    def __init__(
        self,
        data: ndarray,
        xcor: ndarray,
        ycor: ndarray,
        scantime: datetime,
        code: str,
        name: str,
        dtype: str,
        **geoinfo
    ):
        self.data = data
        self.xcor = xcor
        self.ycor = ycor
        self.geoinfo = geoinfo
        self.scantime = scantime
        self.code = code
        self.name = name
        self.dtype = dtype


class Grid(object):
    r"""Structure for processed grid data"""

    __slots__ = [
        "data",
        "drange",
        "reso",
        "code",
        "name",
        "scantime",
        "dtype",
        "stp",
        "lon",
        "lat",
        "geoflag",
        "elev",
        "scan_info",
    ]

    def __init__(
        self,
        data: ndarray,
        drange: Union[float, int],
        reso: float,
        code: str,
        name: str,
        scantime: datetime,
        dtype: str,
        stlon: Union[float, int],
        stlat: Union[float, int],
        lon: ndarray,
        lat: ndarray,
        **scan_info
    ):
        self.data = data
        self.drange = drange
        self.reso = reso
        self.code = code
        self.name = name
        self.scantime = scantime
        self.dtype = dtype
        self.stp = {"lon": stlon, "lat": stlat}
        self.lon = lon
        self.lat = lat
        self.geoflag = True
        self.elev = 0
        self.scan_info = scan_info

    def __repr__(self):
        repr_s = "Datatype: {}\nStation name: {}\nScan time: {}\n"
        return repr_s.format(self.dtype.upper(), self.name, self.scantime)


# define type
Array_T = Union[list, ndarray]
Volume_T = List[Radial]
Boardcast_T = Union[int, float, ndarray]
Number_T = Union[int, float]


def height(distance: Boardcast_T, elevation: Union[int, float], radarheight: Number_T) -> np.ndarray:
    r"""
    Calculate height of radar beam considering atmospheric refraction.
    Parameters
    ----------
    distance: int or float or numpy.ndarray
        distance in kilometer
    elevation: int or float
        elevation angle in degree
    radarheight: int or float
        height of radar in kilometer
    Returns
    -------
    height
    """
    return (
        distance * np.sin(elevation * deg2rad)
        + distance ** 2 / (2 * rm)
        + radarheight / 1000
    )


def get_coordinate(distance: Boardcast_T, azimuth: Boardcast_T, elevation: Number_T, 
                   centerlon: Number_T, centerlat: Number_T, h_offset: bool = True,) -> tuple:
    r"""
    Convert polar coordinates to geographic coordinates with the given radar station position.
    Parameters
    ----------
    distance: int or float or numpy.ndarray
        distance in kilometer in terms of polar coordinate
    azimuth: int or float or numpy.ndarray
        azimuth in radian in terms of polar coordinate
    elevation: int or float
        elevation angle in degree
    centerlon: int or float
        longitude of center point
    centerlat: int or float
        latitude of center point
    Returns
    -------
    actuallon: float or numpy.ndarray
        longitude value
    actuallat: float or numpy.ndarray
        latitude value
    """
    elev = elevation if h_offset else 0
    if isinstance(azimuth, np.ndarray):
        deltav = np.cos(azimuth[:, np.newaxis]) * distance * np.cos(elev * deg2rad)
        deltah = np.sin(azimuth[:, np.newaxis]) * distance * np.cos(elev * deg2rad)
    else:
        deltav = np.cos(azimuth) * distance * np.cos(elev * deg2rad)
        deltah = np.sin(azimuth) * distance * np.cos(elev * deg2rad)
    deltalat = deltav / 111
    actuallat = deltalat + centerlat
    deltalon = deltah / (111 * np.cos(actuallat * deg2rad))
    actuallon = deltalon + centerlon
    return actuallon, actuallat


class RadarBase(abc.ABC):
    """
    Base class for readers in `cinrad.io`.
    Only used when subclassed
    """

    # Same methods for all radar classes
    def set_code(self, code: str):
        self.code = code

    def get_nscans(self) -> int:
        return len(self.el)

    def available_product(self, tilt: int) -> list:
        r"""Get all available products in given tilt"""
        return list(self.data[tilt].keys())

    @staticmethod
    def get_range(drange: Number_T, reso: Number_T) -> np.ndarray:
        return np.arange(reso, drange + reso, reso)


class RadarDecodeError(Exception):
    r"""Unable to decode radar files correctly"""
    pass


class StandardData(RadarBase):
    """
    Class handling new cinrad standard data reading
    Attributes
    ----------
    scantime: datetime.datetime
        time of scan for this data
    code: str
        code for this radar
    angleindex_r: list
        indices of tilts which have reflectivity data
    angleindex_v: list
        indices of tilts which have velocity data
    stationlon: float
        logitude of this radar
    stationlat: float
        latitude of this radar
    radarheight: float
        height of this radar
    name: str
        name of this radar
    a_reso: int
        number of radials in one scan
    el: np.ndarray
        elevation angles for tilts
    drange: float
        current selected radius of data
    elev: float
        elevation angle of current selected tilt
    tilt: int
        current selected tilt
    """
    # fmt: off
    dtype_corr = {1:'TREF', 2:'REF', 3:'VEL', 4:'SW', 5:'SQI', 6:'CPA', 7:'ZDR', 8:'LDR',
                  9:'RHO', 10:'PHI', 11:'KDP', 12:'CP', 14:'HCL', 15:'CF', 16:'SNRH',
                  17:'SNRV', 32:'Zc', 33:'Vc', 34:'Wc', 35:'ZDRc'}
    # fmt: on
    def __init__(self, file: Any):
        r"""
        Parameters
        ----------
        file: str
            path directed to the file to read
        """
        self.f = prepare_file(file)
        self._parse()
        self.f.close()
        # In standard data, station information stored in file
        # has higher priority, so we override some information.
        self.stationlat = self.geo["lat"][0]
        self.stationlon = self.geo["lon"][0]
        self.radarheight = self.geo["height"][0]
        self.angleindex_r = self.available_tilt("REF")  # API consistency
        del self.geo

    def _parse(self):
        # define structure
        SDD_header = np.dtype([
            ("magic_number", "i4"),
            ("major_version", "i2"),
            ("minor_version", "i2"),
            ("generic_type", "i4"),
            ("product_type", "i4"),
            ("res1", "16c")])
        SDD_site = np.dtype([
            ("site_code", "8c"),
            ("site_name", "S32"),
            ("Latitude", "f4"),
            ("Longitude", "f4"),
            ("antenna_height", "i4"),
            ("ground_height", "i4"),
            ("frequency", "f4"),
            ("beam_width_hori", "f4"),
            ("beam_width_vert", "f4"),
            ("RDA_version", "i4"),
            ("radar_type", "i2"),
            ("antenna_gain", "i2"),
            ("trans_loss", "i2"),
            ("recv_loss", "i2"),
            ("other_loss", "i2"),
            ("res2", "46c")])
        SDD_task = np.dtype([
            ("task_name", "S32"),
            ("task_dsc", "128c"),
            ("polar_type", "i4"),
            ("scan_type", "i4"),
            ("pulse_width", "i4"),
            ("scan_start_time", "i4"),
            ("cut_number", "i4"),
            ("hori_noise", "f4"),
            ("vert_noise", "f4"),
            ("hori_cali", "f4"),
            ("vert_cali", "f4"),
            ("hori_tmp", "f4"),
            ("vert_tmp", "f4"),
            ("ZDR_cali", "f4"),
            ("PHIDP_cali", "f4"),
            ("LDR_cali", "f4"),
            ("res3", "40c"),]) 
        SDD_cut = np.dtype([
            ("process_mode", "i4"),
            ("wave_form", "i4"),
            ("PRF1", "f4"),
            ("PRF2", "f4"),
            ("dealias_mode", "i4"),
            ("azimuth", "f4"),
            ("elev", "f4"),
            ("start_angle", "f4"),
            ("end_angle", "f4"),
            ("angular_reso", "f4"),
            ("scan_spd", "f4"),
            ("log_reso", "i4"),
            ("dop_reso", "i4"),
            ("max_range1", "i4"),
            ("max_range2", "i4"),
            ("start_range", "i4"),
            ("sample1", "i4"),
            ("sample2", "i4"),
            ("phase_mode", "i4"),
            ("atmos_loss", "f4"),
            ("nyquist_spd", "f4"),
            ("moments_mask", "i8"),
            ("moments_size_mask", "i8"),
            ("misc_filter_mask", "i4"),
            ("SQI_thres", "f4"),
            ("SIG_thres", "f4"),
            ("CSR_thres", "f4"),
            ("LOG_thres", "f4"),
            ("CPA_thres", "f4"),
            ("PMI_thres", "f4"),
            ("DPLOG_thres", "f4"),
            ("res_thres", "4V"),
            ("dBT_mask", "i4"),
            ("dBZ_mask", "i4"),
            ("vel_mask", "i4"),
            ("sw_mask", "i4"),
            ("DP_mask", "i4"),
            ("res_mask", "12V"),
            ("scan_sync", "i4"),
            ("direction", "i4"),
            ("ground_clutter_classifier_type", "i2"),
            ("ground_clutter_filter_type", "i2"),
            ("ground_clutter_filter_notch_width", "i2"),
            ("ground_clutter_filter_window", "i2"),
            ("res4", "72V"),])
        SDD_rad_header = np.dtype([
            ("radial_state", "i4"),
            ("spot_blank", "i4"),
            ("seq_number", "i4"),
            ("radial_number", "i4"),
            ("elevation_number", "i4"),
            ("azimuth", "f4"),
            ("elevation", "f4"),
            ("seconds", "i4"),
            ("microseconds", "i4"),
            ("data_length", "i4"),
            ("moment_number", "i4"),
            ("res5", "i2"),
            ("hori_est_noise", "i2"),
            ("vert_est_noise", "i2"),
            ("zip_type", "c"),
            ("res6", "13c"),])
        SDD_mom_header = np.dtype([
            ("data_type", "i4"),
            ("scale", "i4"),
            ("offset", "i4"),
            ("bin_length", "i2"),
            ("flags", "i2"),
            ("block_length", "i4"),
            ("res", "12c"),])
        SDD_pheader =  np.dtype([
            ("product_type", "i4"),
            ("product_name", "32c"),
            ("product_gentime", "i4"),
            ("scan_start_time", "i4"),
            ("data_start_time", "i4"),
            ("data_end_time", "i4"),
            ("proj_type", "i4"),
            ("dtype_1", "i4"),
            ("dtype_2", "i4"),
            ("res", "64c"),])

        header = np.frombuffer(self.f.read(32), SDD_header)
        if header["magic_number"] != 0x4D545352:
            raise RadarDecodeError("Invalid standard data")
        site_config = np.frombuffer(self.f.read(128), SDD_site)
        self.code = merge_bytes(site_config["site_code"][0])[:5].decode()
        self.name = site_config["site_name"][0].decode('ascii', errors='ignore').split('\x00')[0]
        self.geo = geo = dict()
        geo["lat"] = site_config["Latitude"]
        geo["lon"] = site_config["Longitude"]
        geo["height"] = site_config["ground_height"]
        task = np.frombuffer(self.f.read(256), SDD_task)
        self.task_name = task["task_name"][0].decode('ascii', errors='ignore').split('\x00')[0]
        self.scantime = datetime.datetime(1970, 1, 1) + datetime.timedelta(
            seconds=int(task["scan_start_time"])
        )
        cut_num = task["cut_number"][0]
        scan_config = np.frombuffer(self.f.read(256 * cut_num), SDD_cut)
        ScanConfig = namedtuple("ScanConfig", SDD_cut.fields.keys())
        self.scan_config = [ScanConfig(*i) for i in scan_config]
        # TODO: improve repr
        data = dict()
        # `aux` stores some auxiliary information, including azimuth angles, elevation angles,
        # and scale and offset of data.
        aux = dict()
        if task["scan_type"] == 2:  # Single-layer RHI
            self.scan_type = "RHI"
        else:
            # There are actually some other scan types, however, they are not currently supported.
            self.scan_type = "PPI"
        # Some attributes that are used only for converting to pyart.core.Radar instances
        self._time_radial = list()
        self._sweep_start_ray_index = list()
        self._sweep_end_ray_index = list()
        # Time for each radial
        radial_count = 0
        while 1:
            radial_header = np.frombuffer(self.f.read(64), SDD_rad_header)
            if radial_header["zip_type"][0] == 1:  # LZO compression
                raise NotImplementedError("LZO compressed file is not supported")
            self._time_radial.append(
                radial_header["seconds"][0] + radial_header["microseconds"][0]
            )
            el_num = radial_header["elevation_number"][0] - 1
            if el_num not in data.keys():
                data[el_num] = defaultdict(list)
                aux[el_num] = defaultdict(list)
            aux[el_num]["azimuth"].append(radial_header["azimuth"][0])
            aux[el_num]["elevation"].append(radial_header["elevation"][0])
            for _ in range(radial_header["moment_number"][0]):
                moment_header = np.frombuffer(self.f.read(32), SDD_mom_header)
                dtype_code = moment_header["data_type"][0]
                dtype = self.dtype_corr.get(dtype_code, None)
                data_body = np.frombuffer(
                    self.f.read(moment_header["block_length"][0]),
                    "u{}".format(moment_header["bin_length"][0]),
                )
                if not dtype:
                    warnings.warn(
                        "Data type {} not understood, skipping".format(dtype_code),
                        RuntimeWarning,
                    )
                    continue
                if dtype not in aux[el_num].keys():
                    scale = moment_header["scale"][0]
                    offset = moment_header["offset"][0]
                    aux[el_num][dtype] = (scale, offset)
                # In `StandardData`, the `data` dictionary stores raw data instead of data
                # calibrated by scale and offset.
                # The calibration process is moved to `get_raw` part.
                data[el_num][dtype].append(data_body)
            radial_state = radial_header["radial_state"][0]
            if radial_state in [0, 3]:
                # Start of tilt or volume scan
                self._sweep_start_ray_index.append(radial_count)
            elif radial_state in [2, 4]:
                self._sweep_end_ray_index.append(radial_count)
            radial_count += 1
            if radial_state in [4, 6]:  # End scan
                break
        self.data = data
        self.aux = aux
        self.el = [i.elev for i in self.scan_config]

    def get_raw(self, tilt: int, drange: Number_T, dtype: str) -> Union[np.ndarray, tuple]:
        r"""
        Get radar raw data
        Parameters
        ----------
        tilt: int
            index of elevation angle
        drange: float
            radius of data
        dtype: str
            type of product (REF, VEL, etc.)
        Returns
        -------
        ret: ndarray or tuple of ndarray
        """
        # The scan number is set to zero in RHI mode.
        self.tilt = tilt if self.scan_type == "PPI" else 0
        self.drange = drange
        if self.scan_type == "RHI":
            max_range = self.scan_config[0].max_range1 / 1000
            if drange > max_range:
                drange = max_range
        self.elev = self.el[tilt]
        reso = self.scan_config[tilt].dop_reso / 1000
        try:
            raw = np.array(self.data[tilt][dtype])
        except KeyError:
            raise RadarDecodeError("Invalid product name")
        if raw.size == 0:
            warnings.warn("Empty data", RuntimeWarning)
            # Calculate size equivalent
            nrays = len(self.aux[tilt]["azimuth"])
            ngates = int(drange / reso)
            out = np.zeros((nrays, ngates)) * np.ma.masked
            return out
        # Data below 5 are used as reserved codes, which are used to indicate other
        # information instead of real data, so they should be masked.
        data = np.ma.masked_less(raw, 5)
        cut = data[:, : int(drange / reso)]
        shape_diff = np.round(drange / reso) - cut.shape[1]
        append = np.zeros((cut.shape[0], int(shape_diff))) * np.ma.masked
        if dtype in ["VEL", "SW"]:
            # The reserved code 1 indicates folded velocity.
            # These region will be shaded by color of `RF`.
            rf = np.ma.masked_not_equal(cut.data, 1)
            rf = np.ma.hstack([rf, append])
        cut = np.ma.hstack([cut, append])
        scale, offset = self.aux[tilt][dtype]
        r = (cut - offset) / scale
        if dtype in ["VEL", "SW"]:
            ret = (r, rf)
            # RF data is separately packed into the data.
        else:
            ret = r
        return ret

    def get_data(self, tilt: int, drange: Number_T, dtype: str) -> Union[Radial, Slice_]:
        r"""
        Get radar data
        Parameters
        ----------
        tilt: int
            index of elevation angle
        drange: float
            radius of data
        dtype: str
            type of product (REF, VEL, etc.)
        Returns
        -------
        r_obj: cinrad.datastruct.Radial
        """
        reso = self.scan_config[tilt].dop_reso / 1000
        ret = self.get_raw(tilt, drange, dtype)
        if self.scan_type == "PPI":
            shape = ret[0].shape[1] if isinstance(ret, tuple) else ret.shape[1]
            r_obj = Radial(
                ret,
                int(shape * reso),
                self.elev,
                reso,
                self.code,
                self.name,
                self.scantime,
                dtype,
                self.stationlon,
                self.stationlat,
                nyquist_velocity=self.scan_config[tilt].nyquist_spd,
                task=self.task_name,
            )
            x, y, z, d, a = self.projection(reso)
            r_obj.add_geoc(x, y, z)
            r_obj.add_polarc(d, a)
            return r_obj
        else:
            # Manual projection
            shape = ret[0].shape[1] if isinstance(ret, tuple) else ret.shape[1]
            dist = np.linspace(reso, self.drange, ret.shape[1])
            d, e = np.meshgrid(dist, self.aux[tilt]["elevation"])
            h = height(d, e, 0)
            rhi = Slice_(
                ret,
                d,
                h,
                self.scantime,
                self.code,
                self.name,
                dtype,
                azimuth=self.aux[tilt]["azimuth"][0],
            )
            return rhi

    def projection(self, reso: float) -> tuple:
        r = np.arange(reso, self.drange + reso, reso)
        theta = np.array(self.aux[self.tilt]["azimuth"]) * deg2rad
        lonx, latx = get_coordinate(
            r, theta, self.elev, self.stationlon, self.stationlat
        )
        hght = (
            height(r, self.elev, self.radarheight)
            * np.ones(theta.shape[0])[:, np.newaxis]
        )
        return lonx, latx, hght, r, theta

    def available_tilt(self, product: str) -> List[int]:
        r"""Get all available tilts for given product"""
        tilt = list()
        for i in list(self.data.keys()):
            if product in self.data[i].keys():
                tilt.append(i)
        return tilt

    def iter_tilt(self, drange: Number_T, dtype: str) -> Generator:
        for i in self.available_tilt(dtype):
            yield self.get_data(i, drange, dtype)

    def __repr__(self):
        return (
            'Radar station: {}/{};\nScan time: {};\n'
            'Longitude/Latitude: ({:.3f}, {:.3f});\nHeight: {}m;\n'
            'Task name: {}').format(
                self.name, self.code, self.scantime, self.stationlon, 
                self.stationlat, self.radarheight, self.task_name)
