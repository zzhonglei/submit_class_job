"""
  * 经纬度偏移转换
"""
import math

PI = 3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280
x_PI = float(PI * float(3000.0) / float(180.0))
aa = float(6378245.0)
ee = 0.00669342162296594323


def out_of_china(lng, lat):
    # 纬度3.86~53.55,经度73.66~135.05
    if 73.66 < lng < 135.05 and 3.86 < lat < 53.55:
        return False


def transform_lat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * PI) + 20.0 * math.sin(2.0 * lng * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * PI) + 40.0 * math.sin(lat / 3.0 * PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * PI) + 320 * math.sin(lat * PI / 30.0)) * 2.0 / 3.0
    return ret


def transform_lng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * PI) + 20.0 * math.sin(2.0 * lng * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * PI) + 40.0 * math.sin(lng / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * PI) + 300.0 * math.sin(lng / 30.0 * PI)) * 2.0 / 3.0
    return ret


def bd09_to_gcj02(bd_lon, bd_lat):
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_PI)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_PI)
    gg_lng = z * math.cos(theta)
    gg_lat = z * math.sin(theta)
    return [gg_lng, gg_lat]


def gcj02_to_wgs84(lng, lat):
    # 判断是否为国外坐标
    if out_of_china(lng, lat):
        return [lng, lat]
    else:
        dlat = transform_lat(lng - 105.0, lat - 35.0)
        dlng = transform_lng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * PI
        magic = math.sin(radlat)
        magic = 1 - ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((aa * (1 - ee)) / (magic * sqrtmagic) * PI)
        dlng = (dlng * 180.0) / (aa / sqrtmagic * math.cos(radlat) * PI)
        mglat = lat + dlat
        mglng = lng + dlng
        return [lng * 2 - mglng, lat * 2 - mglat]


def bd09_to_wgs84(lng, lat):
    point = bd09_to_gcj02(lng, lat)
    wgs84point = gcj02_to_wgs84(point[0], point[1])
    return [wgs84point[0], wgs84point[1]]
