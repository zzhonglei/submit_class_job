from tqdm import tqdm
import geopandas as gpd
from pyproj import Transformer
from 道路匹配.transfor import gcj02_to_wgs84
from multiprocessing import Pool, cpu_count
import numpy as np
import pandas as pd


def process_group(group):
    # 初始化坐标转换器
    transformer = Transformer.from_crs("epsg:4326", "epsg:32648", always_xy=True)
    group = group.reset_index(drop=True)

    # 转换坐标并投影（矢量化处理）
    lons = group['lon'].values
    lats = group['lat'].values

    # 批量处理 gcj02_to_wgs84 转换
    wgs84_coords = np.array([gcj02_to_wgs84(lon, lat) for lon, lat in zip(lons, lats)])
    wgs84_lons = wgs84_coords[:, 0]
    wgs84_lats = wgs84_coords[:, 1]

    # 批量坐标投影
    xs, ys = transformer.transform(wgs84_lons, wgs84_lats)
    group['wgs84_lon'] = wgs84_lons
    group['wgs84_lat'] = wgs84_lats
    group['x'] = xs
    group['y'] = ys

    # 将 UNIX 时间戳转换为 datetime 对象
    if not pd.api.types.is_datetime64_any_dtype(group['time']):
        group['time'] = pd.to_datetime(group['time'], unit='s')  # 指定单位为秒

    stay_points_local = []
    x = group['x'].values
    y = group['y'].values
    t = group['time'].values  # 现在 t 是 datetime64[ns] 类型
    n = len(group)
    distance_threshold = 500  # 米
    time_threshold = np.timedelta64(2, 'm')  # 2分钟
    i = 0
    while i < n:
        t_i = t[i]
        # 找到超过时间阈值的位置索引
        t_i_end = t_i + time_threshold
        j_end = np.searchsorted(t, t_i_end, side='right')
        # 批量计算点 i 到 j_end 的距离
        dx = x[i + 1:j_end] - x[i]
        dy = y[i + 1:j_end] - y[i]
        distances = np.sqrt(dx ** 2 + dy ** 2)
        # 找到距离阈值内的索引
        within_threshold = np.where(distances <= distance_threshold)[0]
        if len(within_threshold) > 0:
            # 获取距离阈值内的最后一个点
            j = i + 1 + within_threshold[-1]
            delta_t = t[j] - t_i
            if delta_t >= time_threshold:
                # 选择代表点（如第一个点）
                stay_lon = group.loc[i, 'wgs84_lon']
                stay_lat = group.loc[i, 'wgs84_lat']
                stay_points_local.append({'lon': stay_lon, 'lat': stay_lat})
                i = j  # 跳过已处理的点
                continue
        i += 1
    return stay_points_local


def main():
    print("开始逐块读取和处理数据...")

    # 设置进程数量，默认为 CPU 核心数
    num_processes = cpu_count()

    # 设置用于存储所有停驻点的列表
    all_stay_points = []

    # 初始化一个空的 DataFrame 作为缓冲区
    buffer_data = pd.DataFrame(columns=['car_id', 'order_id', 'time', 'lon', 'lat'])

    # 读取数据，使用 chunksize 读取数据块
    chunk_iter = pd.read_csv(
        '/Users/zhonglei/实习任务/大数据实习任务2/实习2/data/成都滴滴数据20161112/gps_20161112.txt',
        sep=',',  # 修改分隔符为逗号
        engine='python',
        header=None,
        names=['car_id', 'order_id', 'time', 'lon', 'lat'],
        dtype={
            'car_id': 'str',  # 设置为字符串类型
            'order_id': 'str',  # 设置为字符串类型
            'time': 'Int32',  # 设置为可空整数类型
            'lon': 'float32',
            'lat': 'float32'
        },
        chunksize=500000,  # 根据内存情况调整
        on_bad_lines='skip'  # 跳过格式错误的行（pandas >=1.3.0）
    )

    for chunk in tqdm(chunk_iter, desc='读取数据块'):
        # 将当前块添加到缓冲区
        if buffer_data.empty:
            buffer_data = chunk
        else:
            buffer_data = pd.concat([buffer_data, chunk], ignore_index=True)

        # 移除 'time' 列中缺失的行
        buffer_data = buffer_data.dropna(subset=['time'])

        # 按 'car_id'、'order_id' 和 'time' 排序数据
        buffer_data = buffer_data.sort_values(by=['car_id', 'order_id', 'time'])

        # 按 'car_id' 和 'order_id' 对数据进行分组
        grouped = buffer_data.groupby(['car_id', 'order_id'])

        # 获取所有的组键
        group_keys = list(grouped.groups.keys())

        # 如果没有组键，则继续下一个块
        if not group_keys:
            buffer_data = pd.DataFrame(columns=['car_id', 'order_id', 'time', 'lon', 'lat'])
            continue

        # 如果只有一个组，可能尚未完整，跳过处理
        if len(group_keys) == 1:
            continue

        # 保留最后一个组的键，可能未完整
        last_group_key = group_keys[-1]

        # 准备要处理的组，排除最后一个可能不完整的组
        groups_to_process = [grouped.get_group(key) for key in group_keys[:-1]]

        # 使用多进程池处理组
        with Pool(processes=num_processes) as pool:
            results = list(
                tqdm(pool.imap(process_group, groups_to_process), total=len(groups_to_process), desc='处理订单'))

        # 收集结果
        for stay_points_group in results:
            all_stay_points.extend(stay_points_group)

        # 从缓冲区中删除已处理的组，只保留最后一个组的数据
        buffer_data = grouped.get_group(last_group_key).copy()

    # 处理缓冲区中剩余的数据
    if not buffer_data.empty:
        # 移除 'time' 列中缺失的行
        buffer_data = buffer_data.dropna(subset=['time'])

        # 按 'car_id' 和 'order_id' 对数据进行分组
        grouped = buffer_data.groupby(['car_id', 'order_id'])

        groups_to_process = [grouped.get_group(key) for key in grouped.groups.keys()]

        with Pool(processes=num_processes) as pool:
            results = list(
                tqdm(pool.imap(process_group, groups_to_process), total=len(groups_to_process), desc='处理剩余订单'))

        for stay_points_group in results:
            all_stay_points.extend(stay_points_group)

    # 保存停驻点到 shapefile
    print("将停驻点保存到 'stay_points.shp'...")
    stay_points_gdf = gpd.GeoDataFrame(
        all_stay_points,
        geometry=gpd.points_from_xy(
            [p['lon'] for p in all_stay_points],
            [p['lat'] for p in all_stay_points]
        ),
        crs="epsg:4326"
    )
    stay_points_gdf.to_file('stay_points.shp')
    print("处理成功完成。")


if __name__ == '__main__':
    main()