import geopandas as gpd
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from fastdtw import fastdtw
from sklearn.cluster import AgglomerativeClustering
import os
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


def extract_coordinates(line):
    """
    提取轨迹线的(x, y)坐标列表。

    :param line: Shapely LineString 对象
    :return: 坐标列表
    """
    return list(line.coords)


def compute_fastdtw_distance(args):
    """计算两个轨迹之间的FastDTW距离。"""
    i, j, traj_i, traj_j = args
    distance, _ = fastdtw(traj_i, traj_j)
    return (i, j, distance)


def traj_pairs_generator(trajectories, num_trajectories):
    """生成轨迹对的生成器，避免一次性存储所有轨迹对。"""
    for i in range(num_trajectories):
        traj_i = trajectories[i]
        for j in range(i + 1, num_trajectories):
            traj_j = trajectories[j]
            yield (i, j, traj_i, traj_j)


def main():
    # 输入文件路径
    input_file = '/Volumes/win/大数据实习任务2/实习2/data/成都滴滴数据20161109/轨迹线/line.shp'
    output_filename = 'clustered_trajectories.shp'

    # 读取shp文件，仅读取几何列以节省内存
    df = gpd.read_file(input_file, geometry='geometry')
    print("shp文件读取完毕")

    # 提取轨迹数据为坐标列表
    trajectories = df['geometry'].apply(extract_coordinates).tolist()
    num_trajectories = len(trajectories)
    print(f"共提取 {num_trajectories} 条轨迹")

    # 随机抽取1000条轨迹
    sample_size = 1000
    if num_trajectories < sample_size:
        print(f"轨迹数量少于 {sample_size}，将使用所有 {num_trajectories} 条轨迹")
        sample_indices = list(range(num_trajectories))
    else:
        random_seed = 42  # 设置随机种子以确保可重复性
        np.random.seed(random_seed)
        sample_indices = np.random.choice(num_trajectories, size=sample_size, replace=False)
    print(f"随机抽取 {len(sample_indices)} 条轨迹用于聚类")

    # 提取样本轨迹和对应的GeoDataFrame子集
    sampled_trajectories = [trajectories[i] for i in sample_indices]
    sampled_df = df.iloc[sample_indices].reset_index(drop=True)
    sampled_num = len(sampled_trajectories)

    # 生成轨迹对的生成器
    traj_gen = traj_pairs_generator(sampled_trajectories, sampled_num)
    print("轨迹对生成器创建完毕")

    # 使用多进程计算FastDTW距离，并动态填充距离矩阵
    distance_matrix = np.zeros((sampled_num, sampled_num), dtype=np.float32)

    num_processes = cpu_count()
    with Pool(processes=num_processes) as pool:
        for result in tqdm(pool.imap(compute_fastdtw_distance, traj_gen),
                           total=sampled_num * (sampled_num - 1) // 2,
                           desc="计算FastDTW距离"):
            i, j, distance = result
            distance_matrix[i, j] = distance
            distance_matrix[j, i] = distance  # 对称矩阵

    print("距离矩阵计算完毕")

    # 使用层次聚类对距离矩阵进行聚类
    clustering = AgglomerativeClustering(
        n_clusters=5,  # 可根据需求调整
        metric='precomputed',  # 将 'affinity' 改为 'metric'
        linkage='average'
    )
    clustering.fit(distance_matrix)

    # 将聚类结果添加到GeoDataFrame
    sampled_df['cluster'] = clustering.labels_

    # 获取输入文件的目录路径
    output_dir = os.path.dirname(input_file)
    output_path = os.path.join(output_dir, output_filename)

    # 保存结果到指定路径
    sampled_df.to_file(output_path)
    print(f"聚类完成，结果已保存到 '{output_path}'。")


if __name__ == '__main__':
    main()