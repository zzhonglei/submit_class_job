import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# 示例2：使用系统中已存在的中文字体（macOS）

plt.rcParams['font.family'] = 'STHeiti'
plt.rcParams['font.sans-serif'] = ['STHeiti']
# 解决负号显示问题
plt.rcParams['axes.unicode_minus'] = False
# 1. 读取CSV数据
# 请将'path_to_your_file.csv'替换为你的CSV文件的实际路径
csv_file = '/Volumes/win/大数据实习任务2/实习2/data/成都滴滴数据20161112/POI.csv'
df = pd.read_csv(csv_file)

# 2. 数据预处理
# 假设'end_time'是Unix时间戳（秒级或毫秒级），需要根据实际情况调整
# 如果'end_time'是字符串格式的日期时间，请使用pd.to_datetime直接转换

# 尝试将'end_time'转换为日期时间格式
# 如果'end_time'是Unix时间戳（秒），使用 unit='s'
# 如果是毫秒，使用 unit='ms'
try:
    df['end_time'] = pd.to_datetime(df['end_time'], unit='s')
except:
    df['end_time'] = pd.to_datetime(df['end_time'], unit='ms')

# 提取小时信息
df['hour'] = df['end_time'].dt.hour

# 3. 统计百分比
# 按小时和观测值分组计数
grouped = df.groupby(['hour', 'most_common_basetype']).size().reset_index(name='count')

# 计算每小时的总计数
total_per_hour = grouped.groupby('hour')['count'].transform('sum')

# 计算百分比
grouped['percentage'] = (grouped['count'] / total_per_hour) * 100

# 4. 数据可视化
# 设置绘图风格
sns.set(style="whitegrid")

# 创建一个透视表，以便绘图
pivot_table = grouped.pivot(index='hour', columns='most_common_basetype', values='percentage').fillna(0)

# 绘制堆积柱状图
pivot_table.plot(kind='bar', stacked=True, figsize=(15, 8), colormap='tab20')

plt.xlabel('Hour of Day')
plt.ylabel('Percentage (%)')
plt.title('Percentage of Each most_common_basetype per Hour')
plt.legend(title='most_common_basetype', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# 显示图形
plt.show()

# 5. 输出结果到CSV
# 将结果保存为新的CSV文件
output_csv = 'hourly_basetype_percentage.csv'
grouped.to_csv(output_csv, index=False)
print(f"统计结果已保存到 {output_csv}")