import time
import redis
from scrapy.utils.project import get_project_settings


# 获取项目设置
settings = get_project_settings()

# 创建全局连接池
pool = redis.BlockingConnectionPool(
    host=settings.get('REDIS_HOST', 'localhost'),
    port=settings.get('REDIS_PORT', 6379),
    db=settings.get('REDIS_DB', 3),
    password=settings.get('REDIS_PASSWORD', ''),
    max_connections=settings.get('REDIS_CONNECTIONS', 5),
    # 关于 decode_responses 参数：如果您希望 Redis 返回的结果是字符串类型而不是字节串，可以将其设置为 True。这在处理字符串数据时非常方便，但如果您需要处理二进制数据（如使用 pickle 序列化的对象），建议保持为 False，以避免解码错误。
    decode_responses=settings.get('REDIS_DECODE_RESPONSES', True),
    health_check_interval=settings.get('REDIS_HEALTH_CHECK_INTERVAL', 60),
    timeout=30,  # 等待连接可用的最大时间（秒）
)

def get_redis_client():
    """
    获取 Redis 客户端实例，使用全局连接池。
    """
    while True:
        try:
            client = redis.StrictRedis(connection_pool=pool)
            client.ping()  # 测试连接
            return client
        except redis.ConnectionError:
            print("Redis 连接失败，正在重试...")
            time.sleep(5)  # 等待 5 秒后重试