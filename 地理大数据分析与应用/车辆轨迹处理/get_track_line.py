from 道路匹配.transfor import gcj02_to_wgs84
from shapely.geometry import LineString, mapping
import fiona
from fiona.crs import from_epsg

# 输入和输出文件名
input_file = '/Users/zhonglei/实习任务/大数据实习任务2/实习2/data/成都滴滴数据20161109/gps_20161109_order.txt'  # 请替换为您的实际输入文件名
output_shapefile = 'line.shp'

# 定义shapefile的schema
schema = {
    'geometry': 'LineString',
    'properties': {
        'car_id': 'str',
        'order_id': 'str',
    },
}

# 打开输出的shapefile文件
with fiona.open(output_shapefile, mode='w', driver='ESRI Shapefile',
                schema=schema, crs=from_epsg(4326)) as output:

    # 逐行读取输入文件，避免一次性读入大量数据
    with open(input_file, 'r') as f:
        current_order_id = None
        current_car_id = None
        points = []
        line_number = 0  # 用于调试，记录行号
        for line in f:
            line_number += 1
            # 解析每一行的数据
            elements = line.strip().split(',')
            if len(elements) != 5:
                print(f"跳过格式不正确的行 {line_number}: {line.strip()}")
                continue  # 跳过格式不正确的行
            car_id, order_id, time_str, lon_str, lat_str = elements
            try:
                lon, lat = float(lon_str), float(lat_str)
            except ValueError:
                print(f"跳过无法转换坐标的行 {line_number}: {line.strip()}")
                continue  # 跳过无法转换坐标的行
            # 坐标转换
            try:
                lng_wgs84, lat_wgs84 = gcj02_to_wgs84(lon, lat)
            except Exception as e:
                print(f"坐标转换失败的行 {line_number}: {line.strip()}，错误: {e}")
                continue  # 跳过转换失败的行
            # 如果order_id相同，继续添加点
            if order_id == current_order_id:
                points.append((lng_wgs84, lat_wgs84))
            else:
                # 如果不是第一个订单，保存之前的LineString
                if current_order_id is not None and len(points) >= 2:
                    line_geom = LineString(points)
                    output.write({
                        'geometry': mapping(line_geom),
                        'properties': {
                            'car_id': current_car_id,
                            'order_id': current_order_id,
                        },
                    })
                elif current_order_id is not None:
                    print(f"订单 {current_order_id} 只有 {len(points)} 个点，跳过。")
                # 重置变量，开始处理新的订单
                current_order_id = order_id
                current_car_id = car_id
                points = [(lng_wgs84, lat_wgs84)]

        # 循环结束后，保存最后一个订单的LineString
        if current_order_id is not None and len(points) >= 2:
            line_geom = LineString(points)
            output.write({
                'geometry': mapping(line_geom),
                'properties': {
                    'car_id': current_car_id,
                    'order_id': current_order_id,
                },
            })
        elif current_order_id is not None:
            print(f"订单 {current_order_id} 只有 {len(points)} 个点，跳过。")