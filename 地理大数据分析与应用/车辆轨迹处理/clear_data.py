import pandas as pd

def sort_and_deduplicate(input_file, output_file):
    # 读取数据，每行没有表头，分隔符为逗号
    df = pd.read_csv(input_file, header=None, sep=',', encoding='utf-8')

    # 删除完全相同的行
    df = df.drop_duplicates()

    # 按第一列和第二列排序
    df = df.sort_values(by=[0, 1])

    # 将结果写入输出文件，不包含索引和表头
    df.to_csv(output_file, header=False, index=False, encoding='utf-8')

if __name__ == "__main__":
    input_file = '/Users/zhonglei/Library/Mobile Documents/com~apple~CloudDocs/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161109/order_20161109.txt'   # 输入文件名
    output_file = '/Users/zhonglei/Library/Mobile Documents/com~apple~CloudDocs/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161109/order_20161109_order.txt' # 输出文件名
    sort_and_deduplicate(input_file, output_file)