import requests
import convers as llc
from tqdm import tqdm
import re
import pandas as pd



def read_data(file_path):
    unique_lines = set()
    result = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            clean_line = line.strip()
            if clean_line and clean_line not in unique_lines:
                unique_lines.add(clean_line)
                split_elements = clean_line.split('|')
                if len(split_elements) == 5:
                    result.append(split_elements)
    return result



def get_poi(url,rep_params):
    response = requests.get(url, params=rep_params)
    data = response.json()  # 返回字典数据dict
    status = data["status"]
    if status != "0":
        address = data["geocodes"][0]["formatted_address"]
        lng = float(data["geocodes"][0]["location"].split(",")[0])
        lat = float(data["geocodes"][0]["location"].split(",")[1])
        wgs84_lng = float(llc.gcj02_to_wgs84(lng, lat)[0])
        wgs84_lat = float(llc.gcj02_to_wgs84(lng, lat)[1])
        tqdm.write(f"{address}", end='')
        return address, wgs84_lng, wgs84_lat
    else:
        # 构建要输出的字符串
        message = "查询地址：" + rep_params["address"] + " 失效"
        # 使用 tqdm.write 输出信息
        print("\033[F\r", end='')  # \033[F 上移一行, \r 移动到行首
        tqdm.write('\n',end='')
        tqdm.write(message, end='\n')
        return None,None,None



def extract_price(value):
    match = re.search(r'\d+', value)
    if match:
        return int(match.group(0))
    else:
        return None


if __name__ == '__main__':
    url = "https://restapi.amap.com/v3/geocode/geo?"
    amp_api_key = '3eea59cac82a974a6eacd191ab706de6'
    data = read_data("./proceess1.txt")
    data_len = len(data)
    addresses = []
    web_addresses = []
    wgs84_lngs = []
    wgs84_lats = []
    prices = []
    brief = []
    for i in tqdm(range(data_len),desc="获取POI"):
        rep_params = {
            "key": amp_api_key,
            "address": str(data[i][3]+" "+data[i][4]),
            "city": "武汉市"
        }
        address, wgs84_lng, wgs84_lat = get_poi(url, rep_params)
        if address==None:
            continue
        addresses.append(address)
        wgs84_lngs.append(wgs84_lng)
        wgs84_lats.append(wgs84_lat)
        prices.append(extract_price(data[i][1]))
        brief.append(data[i][0])
        web_addresses.append(str(data[i][2]+" "+data[i][3]+" "+data[i][4]))
    df = pd.DataFrame({
        '地址': addresses,
        '网站介绍地址':web_addresses,
        'lng': wgs84_lngs,
        'lat': wgs84_lats,
        '价格': prices,
        '简介': brief
    })
    df.to_csv('output.csv', index=False, encoding='utf-8')


    # url = "https://restapi.amap.com/v3/geocode/geo?"
    # amp_api_key = '3eea59cac82a974a6eacd191ab706de6'
    # rep_params = {
    #     "key": amp_api_key,
    #     "address": str("洪山"+" "+"瑜东路"+" "+"喻家湖东路"),
    #     "city": "武汉市"
    # }
    # address, wgs84_lng, wgs84_lat = get_poi(url, rep_params)
    # print(address)