import pandas as pd

# 读取表格文件
input_path = 'sorted_poi.csv'
df = pd.read_csv(input_path)

# 拼接“大类”和“中类”列为字符串，创建新列
df['combined'] = df['大类'].astype(str) + df['中类'].astype(str)

# 输出结果到txt文件
output_path = 'output.txt'
with open(output_path, 'w', encoding='utf-8') as f:
    current_order = None  # 当前的 order 初始化为空
    line_content = ""  # 当前行的内容初始化为空
    for _, row in df.iterrows():
        # 检查 order 是否变化
        if row['face_id'] != current_order:
            # 如果不是第一次行且内容非空，写入换行
            if line_content:
                f.write(line_content + '\n')
            # 更新 current_order 并重置 line_content
            current_order = row['face_id']
            line_content = row['combined']
        else:
            # 如果 order 没有变化，继续拼接内容
            line_content += ',' + row['combined']

    # 写入最后一行内容
    if line_content:
        f.write(line_content + '\n')

print(f"结果已输出到'{output_path}'文件中。")