# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import json

class CombinedRealEstateItem(scrapy.Item):
    # 房源基本信息
    # scrapy过来的数据尤其是一些非常长的内容，比如images, features, agent, agency等相关的不好判断内容的正确性。我们先直接用json保存入库即可。
    # 后续处理的内容都由php插入网页的时候来判断他要的数据是否合法合理等。
    unique_id = scrapy.Field()
    url = scrapy.Field()
    url_md5 = scrapy.Field()
    property_type = scrapy.Field()  # 房产类型，如公寓、别墅等
    listing_type = scrapy.Field()
    name = scrapy.Field()           # 页面标识/名称（可选）
    title = scrapy.Field()          # 房源标题/名称

    address = scrapy.Field()        # 完整地址
    street = scrapy.Field()
    suburb = scrapy.Field()         # 所属郊区
    state = scrapy.Field()          # 州/地区
    postcode = scrapy.Field()       # 邮政编码

    # 房屋属性
    price_text = scrapy.Field()          # 价格信息（字符串格式）
    lower_price = scrapy.Field()          # 最低价格信息
    upper_price = scrapy.Field()          # 最高价格信息
    statement_pdf = scrapy.Field() # price pdf url

    bedrooms = scrapy.Field()       # 卧室数量
    bathrooms = scrapy.Field()      # 浴室数量
    car_spaces = scrapy.Field()     # 车位数量
    land_size = scrapy.Field()      # 土地/建筑面积

    # 房屋描述与特性
    description_title = scrapy.Field()    # 房产详细描述
    description = scrapy.Field()    # 房产详细描述
    council_rates = scrapy.Field() # 市政费
    features = scrapy.Field()       # 其他特性或配置

    origin_pdfs = scrapy.Field()

    # 媒体相关字段
    images = scrapy.Field()  # 图片 URL 列表
    origin_images = scrapy.Field()  # 原始图片 URL 列表

    image_meta = scrapy.Field() # 记录origin_images里面哪些是property, floor plan, agent 图片的。

    floor_plan = scrapy.Field()     # 楼层平面图图片 URL 列表（预期为 Python 列表）
    origin_floor_plan = scrapy.Field()   # 原楼层平面图图片 URL 列表（预期为 Python 列表）

    # 地理位置信息
    latitude = scrapy.Field()       # 纬度
    longitude = scrapy.Field()      # 经度

    # 发布时间字段
    publish_date = scrapy.Field()   # 房源发布时间

    # 代理人信息
    agents = scrapy.Field()
    agency = scrapy.Field()

    # 辅助内容
    image_type_groups = scrapy.Field()
    image_index_in_type = scrapy.Field()

    @classmethod
    def convert_images_to_json(cls, images):
        """
        将 images 字段（列表）转换为 JSON 格式字符串，
        方便在存储到数据库时直接作为 JSON 类型的数据使用。
        """
        try:
            return json.dumps(images)
        except Exception:
            return '[]'