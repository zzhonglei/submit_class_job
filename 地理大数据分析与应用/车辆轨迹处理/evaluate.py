from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from sklearn.preprocessing import MinMaxScaler
from 道路匹配.transfor import gcj02_to_wgs84
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import numpy as np


def process_car_data(df_car):
    # 将 'time' 列转换为 datetime 类型（包含日期和时间）
    df_car['time'] = pd.to_datetime(df_car['time'], format='%Y-%m-%d %H:%M:%S')

    # 按 'order_id' 和 'time' 进行排序
    df_car = df_car.sort_values(['order_id', 'time'])

    # 按 'order_id' 分组
    order_groups = df_car.groupby('order_id')

    order_results = []

    for order_id, df_order in order_groups:
        # 将坐标从 gcj02 转换为 wgs84
        df_order[['lng_wgs84', 'lat_wgs84']] = df_order.apply(
            lambda x: gcj02_to_wgs84(x['lng'], x['lat']), axis=1, result_type='expand'
        )

        # 创建 GeoDataFrame 并设置坐标参考系为 WGS84
        gdf_order_wgs84 = gpd.GeoDataFrame(
            df_order,
            geometry=gpd.points_from_xy(df_order['lng_wgs84'], df_order['lat_wgs84']),
            crs='EPSG:4326'
        )

        # 转换为投影坐标参考系以进行精确的距离计算
        gdf_order_proj = gdf_order_wgs84.to_crs(epsg=32648)

        # 检查点的数量是否足够创建 LineString
        if len(gdf_order_proj.geometry) < 2:
            # 点数不足，设置默认值
            trajectory_length = 0.0
            trajectory_line = None  # 或者您可以选择其他适当的默认值
        else:
            try:
                # 在投影坐标参考系中创建 LineString
                trajectory_line_proj = LineString(gdf_order_proj.geometry.tolist())

                # 计算轨迹长度
                trajectory_length = trajectory_line_proj.length

                # 在 WGS84 坐标参考系中创建 LineString 几何对象
                trajectory_line = LineString(gdf_order_wgs84.geometry.tolist())
            except Exception:
                # 处理创建 LineString 时可能发生的异常
                trajectory_length = 0.0
                trajectory_line = None  # 或者您可以选择其他适当的默认值

        # 计算持续时间（以秒为单位）
        if not df_order['time'].empty:
            duration = (df_order['time'].iloc[-1] - df_order['time'].iloc[0]).total_seconds()
        else:
            duration = 0.0

        # 保存订单结果
        order_result = {
            'agent_id': df_order['agent_id'].iloc[0],
            'car_id': df_order['car_id'].iloc[0],
            'order_id': order_id,
            'trajectory_length': trajectory_length,
            'duration': duration,
            'geometry': trajectory_line,
            'order_end_time': df_order['time'].iloc[-1] if not df_order['time'].empty else pd.NaT
        }
        order_results.append(order_result)

    # 按 'order_end_time' 对订单结果进行排序
    order_results.sort(key=lambda x: x['order_end_time'] if pd.notnull(x['order_end_time']) else pd.Timestamp.min)

    # 计算与下一个订单的时间间隔
    for i in range(len(order_results)):
        if i < len(order_results) - 1:
            time_to_next_order = (
                    order_results[i + 1]['order_end_time'] - order_results[i]['order_end_time']
            ).total_seconds()
            order_results[i]['time_to_next_order'] = time_to_next_order
        else:
            order_results[i]['time_to_next_order'] = np.inf

        # 移除 'order_end_time' 字段，因为不再需要
        del order_results[i]['order_end_time']

    return order_results

def main():
    chunksize = 100000  # 每个块的大小
    pool = Pool(cpu_count() - 1)  # 创建一个工作进程池，保留一个CPU核心
    results = []
    df_buffer = pd.DataFrame()

    # 分块读取CSV文件
    with pd.read_csv('/Volumes/win/大数据实习任务2/实习2/data/成都滴滴数据20161112/gps_20161112_order.csv',
                     chunksize=chunksize) as reader:
        for chunk in tqdm(reader):
            # 将缓冲区和当前块合并
            df_combined = pd.concat([df_buffer, chunk], ignore_index=True)

            # 获取唯一的 car_id
            car_ids = df_combined['car_id'].unique()

            if len(car_ids) == 1:
                # 只有一个 car_id，将数据保留在缓冲区
                df_buffer = df_combined
            else:
                # 处理除最后一个 car_id 以外的所有 car_id
                car_ids_to_process = car_ids[:-1]
                for car_id in car_ids_to_process:
                    df_car = df_combined[df_combined['car_id'] == car_id]
                    res = pool.apply_async(process_car_data, args=(df_car,))
                    results.append(res)

                # 保留最后一个 car_id 的数据在缓冲区
                df_buffer = df_combined[df_combined['car_id'] == car_ids[-1]]

    # 处理缓冲区中剩余的数据
    if not df_buffer.empty:
        df_car = df_buffer
        res = pool.apply_async(process_car_data, args=(df_car,))
        results.append(res)

    pool.close()
    pool.join()

    # 收集所有处理结果
    processed_orders = []
    for res in results:
        processed_orders.extend(res.get())

    df_processed = pd.DataFrame(processed_orders)

    # 处理 'time_to_next_order' 列中的无限值
    max_time_to_next_order = df_processed['time_to_next_order'].replace(np.inf, np.nan).max()
    df_processed['time_to_next_order'] = df_processed['time_to_next_order'].replace(np.inf, max_time_to_next_order)

    # 初始化 MinMaxScaler
    scaler = MinMaxScaler()

    # 归一化轨迹长度
    df_processed['trajectory_length_norm'] = scaler.fit_transform(
        df_processed[['trajectory_length']]
    )

    # 计算加速度（轨迹长度 / 时间）
    # 处理 'duration' 列中的零值和负值，避免除零错误
    df_processed['duration'] = df_processed['duration'].apply(lambda x: x if x > 0 else 1e-6)
    df_processed['acceleration'] = df_processed['trajectory_length'] / df_processed['duration']

    # 归一化加速度
    df_processed['acceleration_norm'] = scaler.fit_transform(
        df_processed[['acceleration']]
    )

    # 处理 'time_to_next_order' 列中的零值和负值
    df_processed['time_to_next_order'] = df_processed['time_to_next_order'].apply(lambda x: x if x > 0 else 1e-6)
    # 归一化 'time_to_next_order'
    df_processed['time_to_next_order_norm'] = scaler.fit_transform(
        df_processed[['time_to_next_order']]
    )

    # 计算综合评分
    df_processed['score'] = (
        df_processed['trajectory_length_norm']
        + df_processed['acceleration_norm']
        + df_processed['time_to_next_order_norm']
    ) / 3

    # 根据评分对订单进行分类，分为3类：3（高）、2（中）、1（低）
    df_processed['category'] = pd.qcut(df_processed['score'], 3, labels=[3, 2, 1])

    # 创建 GeoDataFrame，设置几何信息和坐标参考系
    gdf = gpd.GeoDataFrame(
        df_processed, geometry='geometry', crs='EPSG:4326'
    )

    # 保存结果为 Shapefile
    gdf[['agent_id', 'car_id', 'category', 'geometry']].to_file('orders.shp')

if __name__ == '__main__':
    main()