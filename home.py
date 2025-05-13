import os.path
import sys
# import redis
import json
from scrapy.cmdline import execute

# def push_to_redis():
#     # 连接到 Redis，使用 db=2（与 redis-cli -n 2 对应）
#     r = redis.Redis(host='localhost', port=6379, db=2, decode_responses=True)
#
#     # 构造要推送的数据（将其转换为 JSON 格式字符串）
#     data = {
#         # "url": "https://www.realestate.com.au/buy/list-1?activeSort=list-date",
#         "url": "https://www.realestate.com.au/property-house-sa-wynn+vale-147937236",
#         "meta": {
#             "job-id": "123xsd",
#             "start-date": "dd/mm/yy",
#             "schedule": "priority_url"
#         }
#     }
#     data_json = json.dumps(data)
#
#     # 指定要 push 到的队列 key
#     key = 'realestatespider:start_urls'
#
#     # 使用 LPUSH 命令将数据推入队列
#     result = r.lpush(key, data_json)
#     print(f"Pushed data to Redis list '{key}'. List length is now: {result}")

if __name__ == "__main__":
    # push_to_redis()
    # push_to_redis()
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    #  -a data='{"role": "admin", "ads": ["a1", "a2"]}
    # execute(['scrapy', 'crawl', 'realestate'])
    # 构造参数字典
    data_dict = {
        "role": "admin",
        # "ads": ["kxvj5ew", "kxvj5v1"]
        "ads": ["kxvj5ew"]
    }

    # 将参数字典转换为 JSON 字符串
    data_str = json.dumps(data_dict)

    # 调用 Scrapy 爬虫，并传递参数
    execute([
        'scrapy',
        'crawl',
        'realestate',
        '-a',
        f'data={data_str}'
    ])
