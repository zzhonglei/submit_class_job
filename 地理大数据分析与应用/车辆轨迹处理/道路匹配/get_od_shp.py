import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from transfor import gcj02_to_wgs84

# 读取CSV文件
csv_file = '/Users/zhonglei/实习任务/大数据实习任务2/实习2/data/成都滴滴数据20161112/gps_20161112_od.csv'  # 替换为你的CSV文件路径
df = pd.read_csv(csv_file)

# 检查必要的列是否存在
required_columns = ['od_id', 'o_x', 'o_y', 'd_x', 'd_y', 'start_time', 'end_time']
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"缺少必要的列: {col}")

# 创建起点 GeoDataFrame
df_o = df[['od_id', 'o_x', 'o_y', 'start_time']].copy()
df_o['geometry'] = df_o.apply(lambda row: Point(*gcj02_to_wgs84(row['o_x'], row['o_y'])), axis=1)
gdf_o = gpd.GeoDataFrame(df_o, geometry='geometry')

# 创建终点 GeoDataFrame
df_d = df[['od_id', 'd_x', 'd_y', 'end_time']].copy()
df_d['geometry'] = df_d.apply(lambda row: Point(*gcj02_to_wgs84(row['d_x'], row['d_y'])), axis=1)
gdf_d = gpd.GeoDataFrame(df_d, geometry='geometry')

# 设置坐标参考系统 (CRS)，假设使用 WGS84
gdf_o.set_crs(epsg=4326, inplace=True)
gdf_d.set_crs(epsg=4326, inplace=True)

# 选择需要导出的字段
gdf_o = gdf_o[['od_id', 'start_time', 'geometry']]
gdf_d = gdf_d[['od_id', 'end_time', 'geometry']]

# 导出为 SHP 文件
o_shp = 'o_point.shp'
d_shp = 'd_point.shp'

gdf_o.to_file(o_shp, driver='ESRI Shapefile')
gdf_d.to_file(d_shp, driver='ESRI Shapefile')

print(f"成功创建起点 SHP 文件: {o_shp}")
print(f"成功创建终点 SHP 文件: {d_shp}")