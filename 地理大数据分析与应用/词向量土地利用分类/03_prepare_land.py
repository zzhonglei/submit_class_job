from pyproj import Transformer
import numpy as np
import pandas as pd

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

if __name__ == '__main__':
    # 读取两个CSV文件
    land_path = '武汉市地块POI.csv'  # 替换为第一个文件的路径
    poi_path = '武汉市POI排序.csv'  # 替换为第二个文件的路径
    land = pd.read_csv(land_path)
    poi = pd.read_csv(poi_path)
    # 筛选第一文件的 id 列中存在于第二文件 face_id 列中的行
    land_have_poi = land[land['Id'].isin(poi['face_id'])]
    coordinates = list(zip(land_have_poi['center_x'], land_have_poi['center_y']))

    order = nearest_neighbor_tsp(coordinates)
    land_have_poi = land_have_poi.reset_index(drop=True)
    land_have_poi['order'] = 0
    for idx, order_idx in enumerate(order):
        land_have_poi.at[order_idx, 'order'] = idx + 1  # 排序从1开始
    land_order = land_have_poi['order'].values
    # 创建一个从land_have_poi的Id到order的映射
    face_order_mapping = dict(zip(land_have_poi['Id'], land_have_poi['order']))
    # 将POI数据中的face_id映射为排序顺序
    poi['face_order'] = poi['face_id'].map(face_order_mapping)
    # 处理POI数据中没有匹配的face_id，将其face_order设置为一个较大数值以放置在最后
    poi['face_order'] = poi['face_order'].fillna(float('inf'))
    # 根据face_order进行排序，并在相同face_id下根据order列排序
    poi_sorted = poi.sort_values(by=['face_order', 'order'], ascending=[True, True])
    # 删除辅助的face_order列
    poi_sorted = poi_sorted.drop(columns=['face_order'])
    # 输出排序后的POI数据
    output_path = 'sorted_poi.csv'
    poi_sorted.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"排序后的POI数据已保存到'{output_path}'。")