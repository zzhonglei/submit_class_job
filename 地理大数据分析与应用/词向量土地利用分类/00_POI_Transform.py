import pandas as pd
from transfor import gcj02_to_wgs84

if __name__ == '__main__':
    file_path = "F:\\大数据实习\\实习1\\data\\武汉市POI数据\\武汉市POI数据.csv"

    # 读取数据
    df = pd.read_csv(file_path)


    # 使用 bd09_to_wgs84 转换经纬度
    def convert_coordinates(row):
        lon, lat = gcj02_to_wgs84(row['经度'], row['纬度'])
        return pd.Series([lon, lat])


    # 应用转换函数
    df[['经度', '纬度']] = df.apply(convert_coordinates, axis=1)

    # 保存修改后的文件
    output_path = "F:\\大数据实习\\实习1\\data\\武汉市POI数据\\武汉市POI数据_modified.csv"
    df.to_csv(output_path, index=False)
    print(f"文件已成功保存到 {output_path}")
