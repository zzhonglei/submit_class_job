import geopandas as gpd
import pandas as pd
import numpy as np
from 道路匹配.transfor import gcj02_to_wgs84
from scipy.spatial import cKDTree
from collections import Counter
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from shapely.geometry import Point
import os

# 全局变量，用于在子进程中共享 POI 树和 BASETYPE 值
poi_tree = None
poi_basetype = None

def initialize_worker(tree, basetype_values):
    """
    初始化子进程时调用，设置全局变量。
    """
    global poi_tree
    global poi_basetype
    poi_tree = tree
    poi_basetype = basetype_values

def process_point(data):
    """
    处理单个点：查找最近的10个POI，统计BASETYPE出现最多的值。
    """
    global poi_tree
    global poi_basetype
    index, x, y = data
    distances, indices = poi_tree.query([x, y], k=10)
    basetype_values = poi_basetype[indices]
    most_common_basetype = Counter(basetype_values).most_common(1)[0][0]
    return index, most_common_basetype

if __name__ == '__main__':
    # 读取 POI.shp 数据
    poi = gpd.read_file('/Volumes/win/大数据实习任务2/实习2/data/成都2016年poi/成都2016年poi/POI.shp')

    # 提取 GCJ-02 坐标系下的经纬度
    poi['x_gcj'] = poi.geometry.x
    poi['y_gcj'] = poi.geometry.y

    # 将 GCJ-02 坐标转换为 WGS84 坐标
    poi[['x_wgs84', 'y_wgs84']] = poi.apply(
        lambda row: pd.Series(gcj02_to_wgs84(row['x_gcj'], row['y_gcj'])), axis=1
    )

    # 创建新的几何列
    poi['geometry'] = poi.apply(
        lambda row: Point(row['x_wgs84'], row['y_wgs84']), axis=1
    )

    # 设置坐标参考系为 WGS84
    poi = poi.set_crs(epsg=4326)

    # 转换坐标系为 EPSG:32648
    poi = poi.to_crs(epsg=32648)

    # 提取转换后的坐标
    poi['x'] = poi.geometry.x
    poi['y'] = poi.geometry.y

    # 构建 POI 数据的 KDTree
    poi_coords = np.vstack((poi['x'], poi['y'])).T
    tree = cKDTree(poi_coords)

    # 获取 POI 数据的 BASETYPE 字段
    basetype_values = poi['BASETYPE'].values

    # 读取 d_point.shp 数据
    points = gpd.read_file('/Volumes/win/大数据实习任务2/实习2/data/成都滴滴数据20161112/OD_SHP/d_point.shp')  # 确保文件名正确

    # 设置坐标参考系为 WGS84
    points = points.set_crs(epsg=4326)

    # 转换坐标系为 EPSG:32648
    points = points.to_crs(epsg=32648)

    # 提取转换后的坐标
    points['x'] = points.geometry.x
    points['y'] = points.geometry.y

    # 获取总点数
    total_points = len(points)

    # 获取 CPU 核心数
    num_processes = cpu_count() - 1

    # 构建 multiprocessing Pool，并初始化子进程的全局变量
    pool = Pool(processes=num_processes, initializer=initialize_worker, initargs=(tree, basetype_values))

    # 准备索引和坐标数据
    point_data = list(zip(points.index, points['x'], points['y']))

    try:
        # 使用 imap_unordered 结合 tqdm 显示进度条
        results = []
        for result in tqdm(pool.imap_unordered(process_point, point_data), total=total_points,
                           desc="Processing Points"):
            results.append(result)
    finally:
        pool.close()
        pool.join()

    # 将结果转为 DataFrame 并设置索引
    results_df = pd.DataFrame(results, columns=['index', 'most_common_basetype'])
    results_df = results_df.set_index('index')

    # 将最常见的 BASETYPE 值赋予 points 数据
    points['most_common_basetype'] = results_df['most_common_basetype']

    # 删除几何列并保存为 CSV 文件
    points_df = points.drop(columns='geometry')
    points_df.to_csv('output.csv', index=False)

    print("处理完成，结果已保存到 output.csv")