import pandas as pd
import os
from gotrackit.gps.Trajectory import TrajectoryPoints
import concurrent.futures
import glob
from tqdm import tqdm

def process_agent_ids(args):
    agent_id_list, input_file, output_folder, html_output_folder, process_number = args
    # 分块读取数据并根据 agent_ids 进行过滤
    data_iter = pd.read_csv(input_file, chunksize=100000)
    data_list = []
    for chunk in data_iter:
        data_filtered = chunk[chunk['agent_id'].isin(agent_id_list)]
        data_list.append(data_filtered)
    data = pd.concat(data_list)
    # 删除重复项
    data.drop_duplicates(subset=['agent_id', 'time'], keep='first', inplace=True)
    data.reset_index(inplace=True, drop=True)
    # 构建 TrajectoryPoints 类
    tp = TrajectoryPoints(gps_points_df=data, time_unit='s', plain_crs='EPSG:32648')

    tp.del_dwell_points()
    # print(f"Process {process_number}: 删除停驻点")

    # tp.lower_frequency(lower_n=3)
    # print(f"Process {process_number}: 完成采样")

    tp.kf_smooth()
    # print(f"Process {process_number}: 完成平滑")

    process_df = tp.trajectory_data(_type='df')

    # 将结果写入特定进程的输出文件
    output_file_process = os.path.join(output_folder, f'after_reprocess_gps_{process_number}.csv')
    process_df.to_csv(output_file_process, encoding='utf_8_sig', index=False)

    # # 导出 HTML 文件
    # out_fldr = html_output_folder
    # if not os.path.exists(out_fldr):
    #     os.makedirs(out_fldr)
    # html_file_name = f'sample_{process_number}'
    # tp.export_html(out_fldr=out_fldr, file_name=html_file_name, radius=9.0)

if __name__ == '__main__':
    import multiprocessing

    input_file = r'/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112_order.csv'
    output_folder = r'./gps'
    html_output_folder = os.path.join(output_folder, 'html')

    # 如果输出目录不存在，则创建
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    if not os.path.exists(html_output_folder):
        os.makedirs(html_output_folder)

    # 获取唯一的 agent_id 列表
    print("读取唯一的 agent IDs...")
    agent_ids_iter = pd.read_csv(input_file, usecols=['agent_id'], chunksize=100000)
    agent_ids_set = set()
    for chunk in agent_ids_iter:
        agent_ids_set.update(chunk['agent_id'].unique())
    agent_ids = list(agent_ids_set)
    print(f"总共唯一的 agent IDs 数量: {len(agent_ids)}")

    # 将 agent_ids 拆分成块
    chunk_size = 100  # 根据内存情况调整此数字
    agent_id_chunks = [agent_ids[i:i + chunk_size] for i in range(0, len(agent_ids), chunk_size)]

    # 为每个进程准备参数
    args_list = []
    for i, agent_id_chunk in enumerate(agent_id_chunks):
        args_list.append((agent_id_chunk, input_file, output_folder, html_output_folder, i))

    # 使用带有 tqdm 进度条的 ProcessPoolExecutor
    max_workers = multiprocessing.cpu_count()  # 使用所有可用的 CPU 核心
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(executor.map(process_agent_ids, args_list), total=len(args_list)))

    print("所有进程已完成")

    # 合并所有输出的 CSV 文件
    print("合并输出的 CSV 文件...")
    all_files = glob.glob(os.path.join(output_folder, "after_reprocess_gps_*.csv"))
    df_list = []
    for filename in all_files:
        df = pd.read_csv(filename)
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)
    merged_output_file = os.path.join(output_folder, 'after_reprocess_gps.csv')
    merged_df.to_csv(merged_output_file, index=False, encoding='utf_8_sig')

    print(f"已将所有输出文件合并为 {merged_output_file}")