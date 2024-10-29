import pandas as pd
import numpy as np
from multiprocessing import Pool, Manager
from tqdm import tqdm
from pyproj import Transformer

def nearest_neighbor_tsp(coordinates):
    num_points = len(coordinates)  # 获取点的总数
    if num_points == 0:
        return []  # 如果没有点，返回空列表
    transformer = Transformer.from_crs("epsg:4326", "epsg:32650", always_xy=True)
    projected_coords = []
    for lon, lat in coordinates:
        x, y = transformer.transform(lon, lat)
        projected_coords.append((x, y))
    visited = [False] * num_points  # 初始化访问标记列表，所有点均未访问
    order = []  # 存储访问顺序的列表
    current_index = 0  # 从第一个点（索引为0）开始
    order.append(current_index)  # 将起始点添加到访问顺序中
    visited[current_index] = True  # 标记起始点为已访问
    # 遍历所有剩余的点
    for _ in range(num_points - 1):
        last_index = order[-1]  # 获取当前路径的最后一个点的索引
        last_point = projected_coords[last_index]  # 获取最后一个点的投影坐标
        min_dist = float('inf')  # 初始化最小距离为无穷大
        min_index = -1  # 初始化最近邻的索引
        # 遍历所有点，寻找未访问的最近邻
        for i in range(num_points):
            if not visited[i]:
                # 计算欧几里得距离（在投影后的平面坐标系中）
                dist = np.hypot(last_point[0] - projected_coords[i][0],
                                last_point[1] - projected_coords[i][1])
                if dist < min_dist:
                    min_dist = dist  # 更新最小距离
                    min_index = i  # 更新最近邻的索引
        # 将找到的最近邻添加到访问顺序中并标记为已访问
        order.append(min_index)
        visited[min_index] = True
    return order  # 返回最终的访问顺序


class TSPProcessor:
    def __init__(self, lock):
        self.lock = lock

    def process(self, args):
        face_id, group = args
        coordinates = list(zip(group['经度'], group['纬度']))
        order = nearest_neighbor_tsp(coordinates)
        group = group.reset_index(drop=True)
        group['order'] = 0
        for idx, order_idx in enumerate(order):
            group.at[order_idx, 'order'] = idx + 1  # 排序从1开始
        with self.lock:
            with open('武汉市POI排序.csv', 'a', encoding='utf-8', newline='') as f:
                group.to_csv(f, header=False, index=False)

if __name__ == '__main__':
    file_path = "武汉市链接POI.csv"
    df = pd.read_csv(file_path)
    # 根据 'face_id' 排序
    df_sorted = df.sort_values(by='face_id')
    # 按照 'face_id' 分组
    groups = list(df_sorted.groupby('face_id'))
    num_groups = len(groups)

    manager = Manager()
    lock = manager.Lock()
    processor = TSPProcessor(lock)

    columns = df.columns.tolist() + ['order']
    with open('武汉市POI排序.csv', 'w', encoding='utf-8', newline='') as f:
        pd.DataFrame(columns=columns).to_csv(f, index=False)

    pool = Pool()
    with tqdm(total=num_groups) as pbar:
        for _ in pool.imap_unordered(processor.process, groups):
            pbar.update()
    pool.close()
    pool.join()