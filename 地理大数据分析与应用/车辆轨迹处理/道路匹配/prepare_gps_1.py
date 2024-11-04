import csv
from datetime import datetime
from tqdm import tqdm  # 导入 tqdm 模块用于显示进度条

# 输入和输出文件路径
input_file = '/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112_order.txt'  # 替换为你的输入文件路径
output_file = '/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112_order.csv'  # 输出文件将保存为 result.csv

agent_id = 1  # 初始化 agent_id，从 1 开始
current_order_id = None  # 当前处理的订单ID


# 计算输入文件的总行数以设置 tqdm 的总进度
def count_lines(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, _ in enumerate(f, 1):
            pass
    return i


try:
    total_lines = count_lines(input_file)
except Exception as e:
    print(f"无法计算文件行数: {e}")
    total_lines = None  # 如果无法计算总行数，则不设置总数

# 打开输入和输出文件
with open(input_file, 'r', encoding='utf-8') as infile, \
        open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.writer(outfile)
    # 将表头写入输出的 CSV 文件
    writer.writerow(['agent_id', 'lng', 'lat', 'time', 'car_id', 'order_id'])

    # 使用 tqdm 显示进度条
    with tqdm(total=total_lines, desc="处理进度", unit="行") as pbar:
        for line in infile:
            line = line.strip()
            if not line:
                pbar.update(1)
                continue  # 跳过空行

            # 将每行分割成各个部分
            parts = line.split(',')
            if len(parts) != 5:
                pbar.update(1)
                continue  # 跳过不是正好有 5 个字段的行

            car_id, order_id, timestamp_str, lng, lat = parts

            # 检查是否是新的订单ID
            if order_id != current_order_id:
                current_order_id = order_id
                assigned_agent_id = agent_id
                agent_id += 1  # 为下一个不同的订单ID准备
            else:
                assigned_agent_id = agent_id - 1  # 使用相同的 agent_id

            try:
                # 将时间戳转换为所需的日期时间格式
                timestamp = int(timestamp_str)
                dt = datetime.fromtimestamp(timestamp)
                time_str = dt.strftime('%Y-%m-%d %H:%M:%S')  # 格式: YYYY-MM-DD HH:MM:SS
            except ValueError:
                # 如果时间戳转换失败，跳过该行
                pbar.update(1)
                continue

            # 将处理后的数据写入输出文件
            writer.writerow([assigned_agent_id, lng, lat, time_str, car_id, order_id])

            pbar.update(1)  # 更新进度条

print("处理完成，结果已保存到", output_file)