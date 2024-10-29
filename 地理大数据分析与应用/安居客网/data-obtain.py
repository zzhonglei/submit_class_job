import requests
import parsel
import random
from fake_useragent import UserAgent
from requests.exceptions import Timeout, ProxyError, ConnectionError, RequestException
import multiprocessing as mp
import time
import re


empty_count = 0


def get_headers():
    ua = UserAgent()
    headers = {
        "user-agent": ua.random,
    }
    return headers


def test_proxy(proxies, retries=3, delay=2):
    test_url = 'https://httpbin.org/ip'
    for attempt in range(retries):
        try:
            response = requests.get(test_url, proxies=proxies, timeout=5)
            if response.status_code == 200:
                print("代理可用，返回的IP是:", response.json()['origin'])
                return True
            else:
                print("代理无效，状态码:", response.status_code)
                return False
        except Timeout:
            print(f"请求超时，正在重试...（第 {attempt + 1}/{retries} 次）")
        except ProxyError:
            print(f"代理连接失败，正在重试...（第 {attempt + 1}/{retries} 次）")
        except ConnectionError:
            print(f"连接错误，可能是网络问题，正在重试...（第 {attempt + 1}/{retries} 次）")
        except RequestException as e:
            print(f"请求失败: {e}")
            return False
        if attempt < retries - 1:
            print(f"等待 {delay} 秒后重试...")
            time.sleep(delay)
    return False


json_Data = None
def get_proxy(proxy_url):
    global json_Data
    if json_Data is None:
        json_Data = requests.get(proxy_url).json()
    index = random.randrange(len(json_Data['data']))
    ip = json_Data['data'][index]['ip']
    port = json_Data['data'][index]['port']
    proxies = {
        'http': 'http://' + ip + ':' + str(port),
        'https': 'http://' + ip + ':' + str(port)
    }
    print("当前代理：",proxies['http'])
    return proxies


def analysis_data(html_data):
    selector = parsel.Selector(html_data)
    divs = selector.xpath('//div[@class="property"]')
    next_page =selector.xpath('.//div[@class="pagination"]//a[@class="next next-active"]//text()').get()
    print("下一页:",next_page!=None)
    if next_page!=None:
        global empty_count
        empty_count = 0
    if next_page=="":
        print("处理到最后一页")
        next_page = None
    result = ""
    for div in divs:
        title = div.xpath('.//div[@class="property-content"]//h3/text()').get().strip()
        price = div.xpath('.//p[@class="property-price-average"]//text()').get().strip()
        address_1 = div.xpath('.//p[@class="property-content-info-comm-address"]//span[1]//text()').get().strip()
        address_2 = div.xpath('.//p[@class="property-content-info-comm-address"]//span[2]//text()').get().strip()
        address_3 = div.xpath('.//p[@class="property-content-info-comm-address"]//span[3]//text()').get().strip()
        result+=title+'|'+price+'|'+address_1+'|'+address_2+'|'+address_3+'\n'
    if result=="":
        print("遭遇访问验证或其他问题，无法获取数据，重新访问该页面")
        return None,None
    return result, next_page


def get_page_inf(url, proxy_url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=get_headers(), proxies=get_proxy(proxy_url), timeout=5)
            response.raise_for_status()
            html_data = response.text
            return analysis_data(html_data)
        except requests.exceptions.RequestException as e:
            print(f"请求失败，原因: {e}。正在重试... (第 {attempt + 1}/{retries} 次)")
            if attempt < retries - 1:
                time.sleep(delay)  # 等待指定的延迟时间后重试
            else:
                print("已达到最大重试次数，无法获取页面信息。")
                return None  # 达到最大重试次数后返回None


def increment_page(url):
    return re.sub(r'(p)(\d+)', lambda match: f"{match.group(1)}{int(match.group(2)) + 1}", url)


def worker_function(task_queue,lock,shared_counter):
    while True:
        task = task_queue.get()
        if task is None:
            break  # 接收到退出信号，退出循环
        print("--------------------------------------------------")
        print("当前处理：", task[0])
        result,next_page = get_page_inf(task[0],task[1])
        if result is not None:
            with lock:
                with open("proceess2.txt",'a') as f:
                    f.write(result)
        else:
            task_queue.put(task)
            print('重新执行该任务')
        if next_page is not None:
            task_queue.put([increment_page(task[0]),task[1]])
            shared_counter.value =0
        time.sleep(random.uniform(1, 2))
        task_queue.task_done()


def create_processes(num_of_processes):
    task_queue = mp.JoinableQueue(maxsize=4)
    lock = mp.Lock()
    workers = []
    shared_counter = mp.Value('i', 0)  # 'i' 表示共享的整型变量，初始值为 0
    for _ in range(num_of_processes):
        worker = mp.Process(target=worker_function, args=(task_queue,lock,shared_counter,))
        workers.append(worker)
        worker.start()
    return task_queue, lock,workers,shared_counter


def end_process(num_of_processes, task_queue, workers, shared_counter,check_interval=3):
    while shared_counter.value < 20:
        if task_queue.empty():
            shared_counter.value += 1  # 如果队列为空，计数器加1
        else:
            shared_counter.value = 0  # 如果队列不为空，重置计数器
        time.sleep(check_interval)  # 等待指定的时间间隔
    print("任务结束")
    # 发送停止信号给所有工作进程
    for _ in range(num_of_processes):
        task_queue.put(None)
    # 等待所有进程结束
    for worker in workers:
        worker.join()


def change_page(page_number):
    return f'https://wuhan.anjuke.com/sale/o5-p{page_number}/'


if __name__ == '__main__':
    proxy_url = "http://api.tianqiip.com/getip?secret=i9i1lf0evodetilj&num=10&type=json&port=1&time=10&mr=1&sign=a70aa1f09bf8d11a7042d5edb69e05ad"
    num_of_processes=4
    page_num=10
    task_queue, lock, workers,shared_counter = create_processes(num_of_processes)
    page_url = change_page(1)
    task_queue.put([page_url, proxy_url])
    time.sleep(random.uniform(6.5, 9))
    end_process(num_of_processes,task_queue,workers,shared_counter)

