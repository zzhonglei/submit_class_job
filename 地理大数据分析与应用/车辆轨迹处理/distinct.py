import os
import sys
import csv
import tempfile
import heapq
from functools import total_ordering
from itertools import count  # 新增导入


@total_ordering
class Record:
    """
    用于在堆中比较记录的类。
    """

    def __init__(self, line, sort_keys):
        self.line = line
        self.sort_keys = sort_keys  # 用于排序的关键字

    def __lt__(self, other):
        return self.sort_keys < other.sort_keys

    def __eq__(self, other):
        return self.sort_keys == other.sort_keys


def split_and_sort(input_file, temp_dir, chunk_size=1000000):
    """
    将大文件分割成多个已排序的小块，并写入临时文件。
    """
    temp_files = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        chunk = []
        for i, row in enumerate(reader, 1):
            if len(row) < 5:
                continue  # 跳过不完整的行
            chunk.append(row)
            if i % chunk_size == 0:
                temp_file = sort_and_save_chunk(chunk, temp_dir)
                temp_files.append(temp_file)
                chunk = []
                print(f"已处理 {i} 行，生成临时文件：{temp_file.name}")
        if chunk:
            temp_file = sort_and_save_chunk(chunk, temp_dir)
            temp_files.append(temp_file)
            print(f"已处理 {i} 行，生成临时文件：{temp_file.name}")
    return temp_files


def sort_and_save_chunk(chunk, temp_dir):
    """
    对一个块进行排序并保存到临时文件。
    """
    # 按照第1、2、4、5列升序，第3列降序排序
    # 假设第3列为数值类型，如果不是，请根据实际情况调整
    chunk.sort(key=lambda x: (x[0], x[1], x[3], x[4], -float(x[2])))
    temp_file = tempfile.NamedTemporaryFile(mode='w+', dir=temp_dir, delete=False, newline='', encoding='utf-8')
    writer = csv.writer(temp_file)
    writer.writerows(chunk)
    temp_file.flush()
    temp_file.seek(0)
    return temp_file


def merge_and_deduplicate(sorted_files, output_file):
    """
    合并多个已排序的临时文件，并去除重复记录，只保留每组中第3个元素最大的记录。
    """
    unique_counter = count()  # 初始化计数器
    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        heap = []
        for f in sorted_files:
            reader = csv.reader(f)
            try:
                row = next(reader)
                if len(row) < 5:
                    continue
                key = (row[0], row[1], row[3], row[4])
                record = Record(row, key)
                heapq.heappush(heap, (record, next(unique_counter), reader))  # 修改此行
            except StopIteration:
                f.close()
        prev_key = None
        while heap:
            record, _, reader = heapq.heappop(heap)
            current_key = record.sort_keys
            if current_key != prev_key:
                writer.writerow(record.line)
                prev_key = current_key
            try:
                row = next(reader)
                if len(row) < 5:
                    continue
                key = (row[0], row[1], row[3], row[4])
                new_record = Record(row, key)
                heapq.heappush(heap, (new_record, next(unique_counter), reader))  # 修改此行
            except StopIteration:
                pass  # 该文件已读完
        # 关闭所有文件
        for f in sorted_files:
            f.close()


def final_sort(dedup_file, final_output, temp_dir, chunk_size=1000000):
    """
    对去重后的文件进行最终排序，按照第1、2、3列升序。
    """
    temp_files = []
    with open(dedup_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        chunk = []
        for i, row in enumerate(reader, 1):
            if len(row) < 5:
                continue
            chunk.append(row)
            if i % chunk_size == 0:
                temp_file = sort_final_chunk(chunk, temp_dir)
                temp_files.append(temp_file)
                chunk = []
                print(f"已处理去重文件 {i} 行，生成最终临时文件：{temp_file.name}")
        if chunk:
            temp_file = sort_final_chunk(chunk, temp_dir)
            temp_files.append(temp_file)
            print(f"已处理去重文件 {i} 行，生成最终临时文件：{temp_file.name}")
    # 合并所有最终临时文件
    unique_counter_final = count()
    with open(final_output, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        heap = []
        for f in temp_files:
            reader = csv.reader(f)
            try:
                row = next(reader)
                if len(row) < 5:
                    continue
                key = (row[0], row[1], float(row[2]))
                record = Record(row, key)
                heapq.heappush(heap, (record, next(unique_counter_final), reader))
            except StopIteration:
                f.close()
        while heap:
            record, _, reader = heapq.heappop(heap)
            writer.writerow(record.line)
            try:
                row = next(reader)
                if len(row) < 5:
                    continue
                key = (row[0], row[1], float(row[2]))
                new_record = Record(row, key)
                heapq.heappush(heap, (new_record, next(unique_counter_final), reader))
            except StopIteration:
                pass  # 文件已读完
        # 关闭所有文件
        for f in temp_files:
            f.close()


def sort_final_chunk(chunk, temp_dir):
    """
    对最终排序的一个块进行排序并保存到临时文件。
    """
    # 按照第1、2、3列升序排序
    chunk.sort(key=lambda x: (x[0], x[1], float(x[2])))
    temp_file = tempfile.NamedTemporaryFile(mode='w+', dir=temp_dir, delete=False, newline='', encoding='utf-8')
    writer = csv.writer(temp_file)
    writer.writerows(chunk)
    temp_file.flush()
    temp_file.seek(0)
    return temp_file


def cleanup_temp_files(temp_files):
    """
    删除所有临时文件。
    """
    for f in temp_files:
        try:
            os.unlink(f.name)
        except Exception as e:
            print(f"无法删除临时文件 {f.name}：{e}")


def process_large_file(input_file, final_output, temp_dir=None, chunk_size=1000000):
    """
    主函数，处理大文件并生成最终输出。
    """
    if temp_dir is None:
        temp_dir = tempfile.gettempdir()
    print("开始分割并排序...")
    sorted_temp_files = split_and_sort(input_file, temp_dir, chunk_size)
    print(f"已生成 {len(sorted_temp_files)} 个排序的临时文件。")

    dedup_file = os.path.join(temp_dir, 'deduped.csv')
    print("开始合并并去重...")
    merge_and_deduplicate(sorted_temp_files, dedup_file)
    print(f"去重完成，生成文件：{dedup_file}")

    # 清理初始排序的临时文件
    cleanup_temp_files(sorted_temp_files)
    print("已删除初始排序的临时文件。")

    print("开始最终排序...")
    final_sort(dedup_file, final_output, temp_dir, chunk_size)
    print(f"最终排序完成，生成文件：{final_output}")

    # 删除去重文件
    try:
        os.unlink(dedup_file)
        print(f"已删除去重临时文件：{dedup_file}")
    except Exception as e:
        print(f"无法删除去重临时文件 {dedup_file}：{e}")


if __name__ == "__main__":
    # 直接在脚本内部定义输入和输出文件路径
    input_file = "/Users/zhonglei/Library/Mobile Documents/com~apple~CloudDocs/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112.txt"
    final_output = "/Users/zhonglei/Library/Mobile Documents/com~apple~CloudDocs/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112_order.txt"
    process_large_file(input_file, final_output)