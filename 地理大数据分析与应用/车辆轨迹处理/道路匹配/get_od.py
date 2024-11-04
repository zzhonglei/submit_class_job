import csv

input_file = '/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112.txt'  # 替换为您的实际输入文件名
output_file = '/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都滴滴数据20161112/gps_20161112_od.csv'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:

    csv_writer = csv.writer(outfile)
    # 写入表头，包括od_id作为整型，以及车辆ID、订单ID、起始时间和结束时间
    csv_writer.writerow(['od_id', 'o_x', 'o_y', 'd_x', 'd_y', 'way_points',
                         'vehicle_id', 'order_id', 'start_time', 'end_time'])

    current_vehicle_id = None
    current_order_id = None
    current_order_points = []
    current_order_times = []
    od_id_counter = 1  # 初始化od_id计数器

    for line in infile:
        line = line.strip()
        if not line:
            continue  # 跳过空行

        # 将行分割成组件
        parts = line.split(',')
        if len(parts) != 5:
            continue  # 跳过格式不正确的行

        vehicle_id, order_id, time_str, longitude, latitude = parts

        # 检查是否仍在处理同一个订单
        if current_order_id == order_id and current_vehicle_id == vehicle_id:
            current_order_points.append((float(longitude), float(latitude)))
            current_order_times.append(time_str)
        else:
            # 处理前一个订单的数据
            if current_order_points:
                o_x, o_y = current_order_points[0]
                d_x, d_y = current_order_points[-1]
                start_time = current_order_times[0]
                end_time = current_order_times[-1]
                num_points = len(current_order_points)
                middle_index = num_points // 2
                way_point_x, way_point_y = current_order_points[middle_index]
                way_point_str = f"{way_point_x},{way_point_y}"

                # 将处理后的数据写入CSV文件，包括od_id, 车辆ID和订单ID
                csv_writer.writerow([
                    od_id_counter,
                    o_x, o_y,
                    d_x, d_y,
                    way_point_str,
                    current_vehicle_id,
                    current_order_id,
                    start_time,
                    end_time
                ])

                od_id_counter += 1  # 增加od_id计数器

            # 为新订单重置变量
            current_vehicle_id = vehicle_id
            current_order_id = order_id
            current_order_points = [(float(longitude), float(latitude))]
            current_order_times = [time_str]

    # 在循环结束后处理最后一个订单
    if current_order_points:
        o_x, o_y = current_order_points[0]
        d_x, d_y = current_order_points[-1]
        start_time = current_order_times[0]
        end_time = current_order_times[-1]
        num_points = len(current_order_points)
        middle_index = num_points // 2
        way_point_x, way_point_y = current_order_points[middle_index]
        way_point_str = f"{way_point_x},{way_point_y}"

        # 将最后一个订单的数据写入CSV文件，包括od_id, 车辆ID和订单ID
        csv_writer.writerow([
            od_id_counter,
            o_x, o_y,
            d_x, d_y,
            way_point_str,
            current_vehicle_id,
            current_order_id,
            start_time,
            end_time
        ])