import hashlib
import json
import logging

import scrapy
import time
import re
from urllib.parse import urljoin

from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains

from res_ads.adspool.driverpool import AdsWebDriverPool
from res_ads.db.listing_utils import ListingHelper

from res_ads.utils.getredis import get_redis_client

logger = logging.getLogger('realestate')

class RealestateSpider(scrapy.Spider):
    name = "realestate"
    allowed_domains = ["realestate.com.au","reastatic.net"]
    start_urls = ["https://www.realestate.com.au/buy/list-1?activeSort=list-date"]
    redis_key = 'realestate_spider:start_urls'
    current_page_key = "realestate_spider:current_page"
    seen_key = f"realestate_spider:seen_urls"
    BASE_DOMAIN = "https://realestate.com.au"
    js_scroll = "window.scrollTo(0, document.body.scrollHeight)"
    js = "window.scrollTo(0, document.body.scrollHeight)"

    # scrapy crawl realestate -a data='{"role": "admin", "ads": ["a1", "a2"]}'
    def __init__(self, data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.r = get_redis_client() # 需要考虑重连机制
        self.data = json.loads(data) if data else {}

        # self.role = self.data.get('role', '')
        self.ads = self.data.get('ads', [])
        logger.info(f"ads:%s", self.ads)

        if self.ads:
            self.driver_pool = AdsWebDriverPool(self.ads)
        else:
            raise ValueError("ads user_ids is None")

    def __del__(self):
        self.driver_pool.close_all()

    def scroll_down_slowly(self, driver: webdriver, pause_time=0.5, scroll_increment=100):
        """
        缓慢地向下滚动页面，直到页面底部。

        :param driver: Selenium WebDriver 实例
        :param pause_time: 每次滚动后的暂停时间（秒）
        :param scroll_increment: 每次滚动的像素数
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0

        while current_position < last_height:
            current_position += scroll_increment
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            time.sleep(pause_time)
            last_height = driver.execute_script("return document.body.scrollHeight")

    def safe_get(self, driver, url: str, retries: int = 6, delay: int = 5) -> bool:
        """
        尝试加载页面，若发生 TimeoutException，则重试指定次数。
        :param driver: Selenium WebDriver 实例
        :param url: 要加载的 URL
        :param retries: 最大重试次数（至少为 1）
        :param delay: 每次重试前的等待时间（秒）
        :return: True 表示加载成功，False 表示加载失败
        """
        if retries < 1:
            raise ValueError("retries 必须至少为 1")

        driver.set_page_load_timeout(30)  # 设置页面加载超时时间为30秒

        for attempt in range(1, retries + 1):
            try:
                driver.get(url)
                return True
            except (TimeoutException, WebDriverException) as e:
                logging.warning(f"第 {attempt} 次尝试加载 {url} 时发生异常: {e}")
                if attempt < retries:
                    sleep_time = delay * (2 ** (attempt - 1))  # 指数退避
                    logging.info(f"{sleep_time} 秒后重试...")
                    time.sleep(sleep_time)
                    # 停止页面加载并清理当前页面状态
                    try:
                        driver.execute_script("window.stop()")
                    except Exception as stop_exception:
                        logging.warning(f"执行 window.stop() 时发生异常: {stop_exception}")
                    driver.get("about:blank")
        return False  # 所有重试失败后返回 False


    def start_requests(self):
        self.ensure_connection()
        current_page = int(self.r.get(self.current_page_key) or 1)
        url = f"https://www.realestate.com.au/buy/list-{current_page}?activeSort=list-date"
        logger.debug(f"start_requests url:{url}")
        yield scrapy.Request(url=url, callback=self.process_page)

    def close_other_tabs(self, driver):
        current = driver.current_window_handle
        for handle in driver.window_handles:
            if handle != current:
                driver.switch_to.window(handle)
                driver.close()
        driver.switch_to.window(current)

    def process_page(self, response):
        self.ensure_connection()
        current_page = int(self.r.get(self.current_page_key) or 1)
        if current_page >= 80:
            logger.warning("current_page: %s >= 80.", current_page)
            return None
        logger.info("process_page url: %s", response.url)
        user_id, driver = self.driver_pool.get_driver()
        self.close_other_tabs(driver)
        base_url = response.url
        logger.info(f"user_id:{user_id}")
        try:
            self.safe_get(driver, base_url)
            self.scroll_down_slowly(driver)
            # 解析页面逻辑

            while True:
                logger.info(f"正在处理第 {current_page} 页")
                page_source = driver.page_source
                sel = Selector(text=page_source)

                # with open("p1", "w", encoding="utf-8") as f:
                #     f.write(page_source)

                data = {
                    "url": None,
                    "url_md5": None,
                    "price_text": None,
                    "address": None,
                    "bedrooms": None,
                    "bathrooms": None,
                    "car_spaces": None,
                    "land_size": None,
                    "property_type": None,
                    "publish_date": None
                }

                properties = sel.xpath('//div[@class="results-page"]//div[@class="divided-content"]//li//div[@role="presentation"]//a[contains(@href, "property")]/@href').getall()

                for p in properties:
                    full_url = urljoin(base_url, p)
                    logger.info(full_url)

                    url_md5 = hashlib.md5(full_url.encode('utf-8')).hexdigest()
                    # 判断是否已爬取
                    if ListingHelper.exists_by_url_md5(url_md5):
                        logger.warning("url:%s exists in db.", url_md5)
                        continue

                    store_data = {
                        "url": full_url,
                        "meta": {},
                    }

                    # redis-cli DEL realestate_spider:seen_urls
                    # self.r.delete("realestate_spider:seen_urls")
                    # sadd 返回 1 表示是新加入的，0 表示已存在
                    self.ensure_connection()
                    if self.r.sadd(self.seen_key, full_url):
                        self.r.rpush(self.redis_key, json.dumps(store_data))  # 只 push 新链接

                # 更新 Redis 中的当前页码
                current_page += 1
                if current_page > 80:
                    logger.warning("current_page: %s > 80.", current_page)
                    return None

                self.r.set(self.current_page_key, current_page)
                # 点击“下一页”按钮
                try:
                    next_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//nav[@aria-label="Pagination Navigation"]//a[@aria-label="Go to next page"]'))
                    )
                    # 滚动按钮居中并点击
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});",
                        next_btn
                    )
                    ActionChains(driver).move_to_element(next_btn).click().perform()
                    time.sleep(1)
                    self.scroll_down_slowly(driver)
                    time.sleep(1)

                except (TimeoutException, NoSuchElementException):
                    logger.info("No next page found, ending pagination loop")
                    time.sleep(2)
                    continue

        finally:
            self.driver_pool.release_driver(user_id, driver)

    def ensure_connection(self):
        try:
            self.r.ping()
            # logger.info("Redis 连接成功。")
        except ConnectionError:
            logger.warning("Redis 连接断开，正在尝试重新连接...")
            self.r = get_redis_client()
            # 可选：再次验证连接
            try:
                self.r.ping()
                logger.info("Redis 重新连接成功。")
            except ConnectionError:
                logger.error("Redis 重新连接失败。")
                raise

# properties = sel.xpath('//div[@class="results-page"]//div[@class="divided-content"]//li//div[@role="presentation"]')
#
# for p in properties:
#     # URL
#     relative_url = p.xpath('.//a[contains(@href, "/property")]/@href').get()
#     if relative_url:
#         full_url = urljoin(base_url, relative_url)
#         data["url"] = full_url
#         url_md5 = hashlib.md5(full_url.encode('utf-8')).hexdigest()
#         data["url_md5"] = url_md5
#
#         # 判断是否已爬取
#
#         if ListingHelper.exists_by_url_md5(url_md5):
#             logger.warning("url:%s exists in db.", url_md5)
#             continue
#
#     else:
#         continue
#
#     try:
#         # Price
#         price = p.xpath('.//span[contains(@class, "property-price")]/text()').get()
#         if price:
#             data["price_text"] = price.strip()
#
#         # Address
#         address = p.xpath('.//h2[contains(@class, "residential-card__address-heading")]//span/text()').get()
#         if address:
#             data["address"] = address.strip()
#
#         # Time posted
#         listed_time = p.xpath('.//div[contains(@class, "residential-card__banner-strip")]//span/text()').get()
#         if listed_time:
#             listed_time = listed_time.strip()
#             # Remove 'Added' prefix if present
#             listed_time_clean = re.sub(r'^\s*Added\s+', '', listed_time, flags=re.IGNORECASE)
#             dt = dateparser.parse(listed_time_clean)
#             if dt:
#                 data["publish_date"] = dt.strftime("%Y-%m-%d %H:%M:%S")
#             else:
#                 data["publish_date"] = None
#
#         # Features
#         features = p.xpath('.//ul[contains(@class, "residential-card__primary")]//li')
#         for feature in features:
#             aria = feature.xpath('./@aria-label').get(default="").lower()
#             value = feature.xpath('.//p/text()').get()
#             if not aria or not value:
#                 continue
#             if 'bedroom' in aria:
#                 data["bedrooms"] = int(value)
#             elif 'bathroom' in aria:
#                 data["bathrooms"] = int(value)
#             elif 'car space' in aria:
#                 data["car_spaces"] = int(value)
#             elif 'land size' in aria:
#                 m = re.search(r'([\d,.]+)', value)
#                 if m:
#                     size_str = m.group(1).replace(',', '')
#                     try:
#                         data["land_size"] = int(float(size_str))
#                     except ValueError:
#                         logger.warning("land_size parse error:%s", value)
#
#         # Property type
#         property_type = p.xpath('.//ul[contains(@class, "residential-card__primary")]/p/text()').get()
#         if property_type:
#             data["property_type"] = property_type.strip()
#     except Exception as e:
#         logger.warning("parse detail error:%s", e)
#
#     logger.info("full_url:%s, data:%s", full_url, data)

# self.ensure_connection()
# store_data = {
#     "url": full_url,
#     "meta": data,
# }
# self.r.rpush(self.redis_key, json.dumps(store_data))
# yield scrapy.Request(
#     url=full_url,
#     callback=self.parse_listing,
#     cb_kwargs={'data': data}
# )

