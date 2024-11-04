# 1. 从gotrackit导入相关模块
import pandas as pd
from gotrackit.visualization import KeplerVis

if __name__ == '__main__':

    # 读取几何文件
    trip_df = pd.read_csv(r'match_res.csv')

    # 新建KeplerVis类
    kv = KeplerVis()

    # 添加点层
    kv.add_trip_layer(trip_df, lng_field='prj_lng', lat_field='prj_lat',color=[244,67,54],trail_length=50)

    # 添加点层
    kv.add_trip_layer(trip_df, lng_field='lng', lat_field='lat',trail_length=50)

    # 输出HTML到本地
    # 此函数也会返回一个map对象，可在jupyter环境下进行交互式操作
    map = kv.export_html(height=600, out_fldr=r'./', file_name='trip')