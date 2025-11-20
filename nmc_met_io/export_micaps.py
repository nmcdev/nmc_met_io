import os
import numpy as np
from datetime import datetime, timedelta

# --- 辅助函数 ---

def _dk_trim(value):
    """
    辅助函数：将数字转换为字符串并去除首尾空格。
    与普通 strip 不同，它还会去除浮点数小数点后多余的 0 (对应 IDL: DK_TRIM)。
    例如: 12.500 -> '12.5', 12.0 -> '12'
    """
    # 处理列表或数组
    if isinstance(value, (list, tuple, np.ndarray)):
        return [_dk_trim(v) for v in value]
    
    # 处理单个数值
    if isinstance(value, (float, np.floating)):
        # 转为字符串，尝试去除末尾的 0 和 .
        # 使用 %.6f 避免科学计数法，然后 strip
        s = f"{value:.6f}".rstrip('0').rstrip('.')
        return s
    else:
        return str(value).strip()

# --- 核心功能函数 ---

def dk_io_micaps_headline(init_time=None, fhour=None, period=None, 
                          model_name=None, level=None, var_name=None):
    """
    构建 MICAPS 文件的文件头说明字符串 (对应 IDL: dk_io_micaps_headline)。
    """
    if init_time is None:
        init_time = datetime.now()
    
    # 1. 基础时间格式: YYMMDDHHIT
    headline = init_time.strftime('%y%m%d%H') + 'IT'
    
    # 2. 预报时效处理
    if fhour is not None:
        headline += f'_{fhour:03d}FH'
        valid_time = init_time + timedelta(hours=fhour)
        if period is not None:
            start_period_time = init_time + timedelta(hours=(fhour - period))
            str_start = start_period_time.strftime('%d%H')
            str_end = valid_time.strftime('%d%H')
            headline += f'_({str_start}-{str_end})'
        else:
            str_valid = valid_time.strftime('%d%H')
            headline += f'_({str_valid})'
            
    # 3. 模式名称
    if model_name is not None:
        headline += f'_{model_name.strip().upper()}'
        
    # 4. 层次
    if level is not None:
        if level != -1:
            lvl_str = f"{level:.2f}".rstrip('0').rstrip('.')
            headline += f'_{lvl_str}LEV'
            
    # 5. 变量名
    if var_name is not None:
        headline += f'_{var_name.strip()}'
        
    return headline

def dk_io_write_micaps_4(data, lon, lat, 
                         init_time=None, fhour=0, period=None, 
                         model_name=None, level=-1, var_name=None,
                         cn_interval=None, cn_start=None, cn_end=None,
                         out_dir=None, out_filename=None, 
                         data_format=' %10.4f', only_values=False):
    """
    输出数组为 MICAPS 第4类格点格式数据文件 (对应 IDL: dk_io_write_micaps_4)。
    """
    # 参数预处理
    if hasattr(data, 'values'): data = data.values
    if hasattr(lon, 'values'): lon = lon.values
    if hasattr(lat, 'values'): lat = lat.values

    if init_time is None: init_time = datetime.now()
    
    if cn_start is None: cn_start = np.nanmin(data)
    if cn_end is None: cn_end = np.nanmax(data)
    if cn_interval is None:
        diff = cn_end - cn_start
        cn_interval = 1.0 if diff == 0 else diff / 9.0

    show_method = -2 if only_values else -1

    if out_filename is None:
        date_str = init_time.strftime('%y%m%d%H')
        out_filename = f"{date_str}.{fhour:03d}"

    if out_dir is not None:
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating directory: {e}")
                return {'out_path_name': None, 'already_exist': False}
        out_path_name = os.path.join(out_dir, out_filename)
    else:
        out_path_name = out_filename

    if os.path.exists(out_path_name):
        return {'out_path_name': out_path_name, 'already_exist': True}

    # 构建头信息
    desc_str = dk_io_micaps_headline(
        init_time=init_time, fhour=fhour, period=period,
        model_name=model_name, level=level, var_name=var_name
    )
    head_line_1 = f"diamond 4 {desc_str}"

    nlon = len(lon)
    nlat = len(lat)
    dlon = lon[1] - lon[0] if nlon > 1 else 0.0
    dlat = lat[1] - lat[0] if nlat > 1 else 0.0
    
    time_parts = init_time.strftime('%y %m %d %H')
    
    head_line_2 = (
        f"{time_parts} "
        f"{fhour} "
        f"{level:.2f}".rstrip('0').rstrip('.') + " "
        f"{dlon:.3f} "
        f"{dlat:.3f} "
        f"{lon[0]:.3f} "
        f"{lon[-1]:.3f} "
        f"{lat[0]:.3f} "
        f"{lat[-1]:.3f} "
        f"{nlon} "
        f"{nlat} "
        f"{cn_interval:.2f} "
        f"{cn_start:.2f} "
        f"{cn_end:.2f} "
        f"1 "
        f"{show_method}"
    )

    # 写入数据
    try:
        with open(out_path_name, 'w', encoding='utf-8') as f:
            f.write(head_line_1 + '\n')
            f.write(head_line_2 + '\n')

            flat_data = data.flatten()
            line_buffer = []
            cols_per_line = 6
            
            fmt = "{" + ":" + data_format.strip().replace('%', '') + "}"
            
            for i, val in enumerate(flat_data):
                if np.isnan(val):
                    val_str = " 9999.0000" 
                else:
                    val_str = fmt.format(val)
                
                line_buffer.append(val_str)
                if (i + 1) % cols_per_line == 0:
                    f.write("".join(line_buffer) + '\n')
                    line_buffer = []
            
            if line_buffer:
                f.write("".join(line_buffer) + '\n')

    except Exception as e:
        if os.path.exists(out_path_name):
            os.remove(out_path_name)
        raise e

    return {'out_path_name': out_path_name, 'already_exist': False}

def dk_io_write_micaps_3(ids, lons, lats, alts, values, obs_time,
                         level=-1, var_name="观测数据",
                         cn_levels=None, smooth_ceof=1, bold_ceof=0,
                         bpoints=None, out_dir=None, out_filename=None,
                         data_format='{:.4f}'):
    """
    输出填充图数据为 micaps 第3类格式数据文件 (对应 IDL: dk_io_write_micaps_3)。
    """

    # --- 1. 数据预处理 ---
    # 确保输入是 numpy 数组以方便索引
    ids = np.array(ids)
    lons = np.array(lons)
    lats = np.array(lats)
    alts = np.array(alts)
    values = np.array(values)

    # 维度计算
    n_stations = len(ids)
    
    if values.ndim == 1:
        n_values = 1
        # 重塑以便统一处理： (1, n_stations)
        values = values.reshape(1, -1)
    else:
        # 假设 shape 是 (n_features, n_stations)
        if values.shape[1] != n_stations and values.shape[0] == n_stations:
             # 兼容性修正：如果用户传入了 (Stations, Features)
             values = values.T
        n_values = values.shape[0]

    # 时间处理
    if isinstance(obs_time, (float, int)):
        pass # 这里假设外部已处理好，或作为 timestamp
    
    if not isinstance(obs_time, datetime):
        try:
            obs_time = datetime.fromtimestamp(obs_time)
        except:
            if not isinstance(obs_time, datetime):
                obs_time = datetime.now()

    # 文件名处理
    if out_filename is None:
        date_str = obs_time.strftime('%y%m%d%H')
        out_filename = f"{date_str}.000"

    # 路径处理
    if out_dir is not None:
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        out_path_name = os.path.join(out_dir, out_filename)
    else:
        out_path_name = out_filename

    if os.path.exists(out_path_name):
        return {'out_path_name': out_path_name, 'already_exist': True}

    # --- 2. 构建头部信息 ---
    time_str_long = obs_time.strftime('%Y年%m月%d日%H时')
    head_line = f"diamond 3 {time_str_long}{var_name.strip()}"

    time_str_short = obs_time.strftime('%y %m %d %H')
    
    head_info_parts = [time_str_short, _dk_trim(level)]

    if cn_levels is None or len(cn_levels) == 0:
        head_info_parts.append("0")
    else:
        head_info_parts.append(str(len(cn_levels)))
        head_info_parts.extend(_dk_trim(cn_levels))
    
    head_info_parts.append(_dk_trim(smooth_ceof))
    head_info_parts.append(_dk_trim(bold_ceof))

    if bpoints is None or len(bpoints) == 0:
        head_info_parts.append("0")
    else:
        n_pts = len(bpoints) // 2
        head_info_parts.append(str(n_pts))
        head_info_parts.extend(_dk_trim(bpoints))
    
    head_info = "    ".join(head_info_parts)

    size_info = f"    {_dk_trim(n_values)}    {_dk_trim(n_stations)}"

    # --- 3. 写入数据 ---
    if '%' in data_format:
        fmt_str = "{:" + data_format.strip().replace('%', '') + "}"
    else:
        fmt_str = data_format
        if '{' not in fmt_str:
             fmt_str = "{:12.4f}"

    try:
        with open(out_path_name, 'w', encoding='utf-8') as f:
            f.write(head_line + '\n')
            f.write(head_info + '\n')
            f.write(size_info + '\n')

            for i in range(n_stations):
                sid = str(ids[i]).strip()
                slon = f"{lons[i]:8.2f}"
                slat = f"{lats[i]:8.2f}"
                salt = f"{alts[i]:12.2f}"
                
                st_vals = values[:, i]
                val_strs = [fmt_str.format(v) for v in st_vals]
                s_vals = " ".join(val_strs)

                line = f"{sid}  {slon} {slat} {salt} {s_vals}"
                f.write(line + '\n')

    except Exception as e:
        if os.path.exists(out_path_name):
            os.remove(out_path_name)
        raise e

    return {'out_path_name': out_path_name, 'already_exist': False}

def dk_io_write_micaps_11(u, v, lon, lat,
                          init_time=None, fhour=0, period=None,
                          model_name=None, level=-1, var_name=None,
                          out_dir=None, out_filename=None,
                          data_format=' %10.4f'):
    """
    输出数组为 MICAPS 第11类矢量格式数据文件 (对应 IDL: dk_io_write_micaps_11)。
    
    Parameters
    ----------
    u : numpy.ndarray or xarray.DataArray
        U 分量场 (2D)。假设形状为 (nlat, nlon)。
    v : numpy.ndarray or xarray.DataArray
        V 分量场 (2D)。假设形状为 (nlat, nlon)。
    lon : numpy.ndarray
        经度数组 (1D)。
    lat : numpy.ndarray
        纬度数组 (1D)。
    init_time, fhour, period, model_name, level, var_name:
        同 micaps_4。
    out_dir, out_filename:
        同 micaps_4。
    data_format : str, optional
        数据格式。
    """
    
    # 参数处理
    if hasattr(u, 'values'): u = u.values
    if hasattr(v, 'values'): v = v.values
    if hasattr(lon, 'values'): lon = lon.values
    if hasattr(lat, 'values'): lat = lat.values

    if init_time is None: init_time = datetime.now()
    
    # 文件名处理
    if out_filename is None:
        date_str = init_time.strftime('%y%m%d%H')
        out_filename = f"{date_str}.{fhour:03d}"

    if out_dir is not None:
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating directory: {e}")
                return {'out_path_name': None, 'already_exist': False}
        out_path_name = os.path.join(out_dir, out_filename)
    else:
        out_path_name = out_filename

    if os.path.exists(out_path_name):
        return {'out_path_name': out_path_name, 'already_exist': True}

    # 构造头信息
    desc_str = dk_io_micaps_headline(
        init_time=init_time, fhour=fhour, period=period,
        model_name=model_name, level=level, var_name=var_name
    )
    head_line_1 = f"diamond 11 {desc_str}"

    nlon = len(lon)
    nlat = len(lat)
    # 网格增量计算
    dlon = lon[1] - lon[0] if nlon > 1 else 0.0
    dlat = lat[1] - lat[0] if nlat > 1 else 0.0
    
    # 计算有效时间 (init + fhour)
    valid_time = init_time + timedelta(hours=fhour)
    time_parts = valid_time.strftime('%y %m %d %H')

    # Head Info: YY MM DD HH FHour Level dlon dlat slon elon slat elat nlon nlat
    head_line_2 = (
        f"{time_parts} "
        f"{fhour} "
        f"{level:.2f}".rstrip('0').rstrip('.') + " "
        f"{dlon:.3f} "
        f"{dlat:.3f} "
        f"{lon[0]:.3f} "
        f"{lon[-1]:.3f} "
        f"{lat[0]:.3f} "
        f"{lat[-1]:.3f} "
        f"{nlon} "
        f"{nlat}"
    )

    # 写入数据
    try:
        with open(out_path_name, 'w', encoding='utf-8') as f:
            f.write(head_line_1 + '\n')
            f.write(head_line_2 + '\n')

            # 准备格式化字符串
            fmt = "{" + ":" + data_format.strip().replace('%', '') + "}"
            cols_per_line = 6

            # 定义写入函数以避免 U 和 V 重复代码
            def write_component(component_data):
                flat_data = component_data.flatten()
                line_buffer = []
                for i, val in enumerate(flat_data):
                    if np.isnan(val):
                        val_str = " 9999.0000"
                    else:
                        val_str = fmt.format(val)
                    
                    line_buffer.append(val_str)
                    if (i + 1) % cols_per_line == 0:
                        f.write("".join(line_buffer) + '\n')
                        line_buffer = []
                if line_buffer:
                    f.write("".join(line_buffer) + '\n')

            # 先写 U，后写 V
            write_component(u)
            write_component(v)

    except Exception as e:
        if os.path.exists(out_path_name):
            os.remove(out_path_name)
        raise e

    return {'out_path_name': out_path_name, 'already_exist': False}

def dk_io_write_micaps_grid_3(data, lon, lat,
                              init_time=None, fhour=0, period=None,
                              model_name=None, level=-1, var_name=None,
                              out_dir=None, out_filename=None,
                              data_format='{:.4f}', min_data=None):
    """
    输出格点预报数据为 micaps 第3类格式数据文件 (格点转站点方式) 
    (对应 IDL: dk_io_write_micaps_grid_3)。
    """
    # 参数处理
    if hasattr(data, 'values'): data = data.values
    if hasattr(lon, 'values'): lon = lon.values
    if hasattr(lat, 'values'): lat = lat.values

    if init_time is None: init_time = datetime.now()
    if level is None: level = -1

    # 文件名处理
    if out_filename is None:
        date_str = init_time.strftime('%y%m%d%H')
        out_filename = f"{date_str}.{fhour:03d}"

    if out_dir is not None:
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating directory: {e}")
                return {'out_path_name': None, 'already_exist': False}
        out_path_name = os.path.join(out_dir, out_filename)
    else:
        out_path_name = out_filename

    if os.path.exists(out_path_name):
        return {'out_path_name': out_path_name, 'already_exist': True}

    # 1. 计算符合条件的数据点数 (nData)
    # Python 的 flatten 顺序通常是 Row-major (Lat-Lon), IDL 循环结构是 Lat(外) Lon(内)。
    # 为了匹配 IDL 的 ID 生成顺序，我们需要确保遍历顺序一致。
    # 如果 data 是 (nLat, nLon)，直接 flatten 即为 Lat0_Lon0, Lat0_Lon1... 符合 IDL 逻辑。
    
    flat_data = data.flatten()
    n_total = len(flat_data)
    
    # 生成坐标网格用于输出
    # meshgrid indexing='xy' (默认) -> lon_grid (nlat, nlon), lat_grid (nlat, nlon)
    # 注意 numpy meshgrid 默认 behavior 可能与 data shape 对应有坑。
    # 如果 data 是 (Lat, Lon), 建议用 indexing='ij' 传入 (lat, lon)，
    # 但 IDL 传入的是 lon(1D), lat(1D).
    # 最稳妥是手动生成 flat 坐标列表以匹配 loop 顺序.
    
    # 模拟 IDL Loop: FOR j=0, nlat-1 (Lat) DO FOR i=0, nlon-1 (Lon)
    # 所以 Lat 是外循环，Lon 是内循环
    # data[i,j] in IDL (Lon, Lat) -> data[j,i] in Python (Lat, Lon)
    
    # 构建 mask
    if min_data is not None:
        mask = flat_data >= min_data
        valid_indices = np.where(mask)[0]
        n_data = len(valid_indices)
    else:
        n_data = n_total
        valid_indices = np.arange(n_total)

    # 2. 构建头信息
    desc_str = dk_io_micaps_headline(
        init_time=init_time, fhour=fhour, period=period,
        model_name=model_name, level=level, var_name=var_name
    )
    head_line_1 = f"diamond 3 {desc_str}"

    # Head Info: ValidTime Level 0 0 0 0
    valid_time = init_time + timedelta(hours=fhour)
    time_parts = valid_time.strftime('%y %m %d %H')
    level_int = int(level)
    head_line_2 = f" {time_parts}    {level_int}     0     0     0     0"

    # Size Info: 1 nData
    head_line_3 = f"    1    {n_data}"

    # 3. 写入数据
    # 格式化
    if '%' in data_format:
        fmt_str = "{:" + data_format.strip().replace('%', '') + "}"
    else:
        fmt_str = data_format
        if '{' not in fmt_str:
             fmt_str = "{:12.4f}"
             
    # 预生成 ID 列表
    # grid_id 对应的是全场顺序的 index，即使有 minData 过滤，IDL 里的 id 也是
    # "id += 1" 在循环末尾，意味着 id 实际上是格点的序号 (0 to N-1)。
    # IDL 代码逻辑：
    # id = 0
    # FOR j... FOR i...
    #   IF data < minData CONTINUE
    #   PRINT id, lon, lat...
    #   id += 1
    # 注意：IDL 中 id += 1 是在 print 之后。
    # 如果 continue 了，id 还会加吗？
    # IDL 语法：IF ... THEN CONTINUE. 如果执行了 CONTINUE，循环体剩下部分跳过，包括 id+=1。
    # 这意味着 id 只有在符合 minData 条件并输出时才增加。即 id 是 "输出序号"，而不是 "格点索引"。
    
    try:
        with open(out_path_name, 'w', encoding='utf-8') as f:
            f.write(head_line_1 + '\n')
            f.write(head_line_2 + '\n')
            f.write(head_line_3 + '\n')
            
            # 生成坐标网格 (Lat, Lon) 形状
            lon_grid, lat_grid = np.meshgrid(lon, lat) # result shape depends, standard (Lat, Lon) if lat is y-axis
            flat_lons = lon_grid.flatten()
            flat_lats = lat_grid.flatten()

            # 计数器
            current_id = 0
            
            for idx in valid_indices:
                # 提取数据
                val = flat_data[idx]
                this_lon = flat_lons[idx]
                this_lat = flat_lats[idx]
                
                # 格式化
                sid = f"{current_id:10d}" # IDL: I10
                slon = f"{this_lon:8.3f}" # IDL: F8.3
                slat = f"{this_lat:8.3f}" # IDL: F8.3
                salt = " 666 "            # IDL: constant
                
                if np.isnan(val):
                     # Diamond 3 对 nan 的处理一般依赖用户习惯，这里保留数值
                     sval = " 9999.0"
                else:
                     sval = fmt_str.format(val)
                
                # 写入
                # String format: ID Lon Lat 666 Value
                line = f"{sid}  {slon}  {slat} {salt} {sval}"
                f.write(line + '\n')
                
                current_id += 1

    except Exception as e:
        print(f"Error writing micaps grid_3 file {out_path_name}: {e}")
        if os.path.exists(out_path_name):
            os.remove(out_path_name)
        raise e

    return {'out_path_name': out_path_name, 'already_exist': False}