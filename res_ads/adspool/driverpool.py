import logging
import threading
from queue import Queue
from threading import RLock


import requests
from scrapy.utils.project import get_project_settings
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

logger = logging.getLogger('driverpool')
settings = get_project_settings()


class AdsWebDriverPool:
    def __init__(self, user_ids):
        self._lock = RLock()  # 改为可重入锁
        # self.lock = threading.Lock()
        self.pool = Queue()
        self.user_ids = user_ids
        self._initialize_pool()

    def _initialize_pool(self):
        for user_id in self.user_ids:
            driver = self._create_driver(user_id)
            if driver:
                self.pool.put((user_id, driver))

    def _create_driver(self, user_id):
        # 启动 AdsPower 浏览器实例
        open_url = f"http://local.adspower.net:50325/api/v1/browser/start?user_id={user_id}&api_key={settings.get('ADS_API_KEY')}"
        resp = requests.get(open_url).json()
        if resp.get("code") != 0:
            logger.error(f"启动浏览器失败，user_id: {user_id}, 错误信息: {resp.get('msg')}")
            return None

        chrome_driver = resp["data"]["webdriver"]
        debug_addr = resp["data"]["ws"]["selenium"]

        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", debug_addr)
        # chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("--disable-gpu")

        service = Service(executable_path=chrome_driver)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver

    def get_driver(self):
        with self._lock:  # 加锁保证原子操作
            return self.pool.get()

    def release_driver(self, user_id, driver):
        try:
            driver.delete_all_cookies()
            driver.get("about:blank")  # 重置到空白页
        except Exception as e:
            logger.warning(f"清理浏览器状态失败: {str(e)}")
        finally:
            with self._lock:
                self.pool.put((user_id, driver))
    # def release_driver(self, user_id, driver):
    #     with self._lock:
    #         self.pool.put((user_id, driver))

    def close_all(self):
        while not self.pool.empty():
            user_id, driver = self.pool.get()
            try:
                driver.quit()
            except Exception:
                pass
            # 关闭 AdsPower 浏览器实例
            close_url = f"http://local.adspower.net:50325/api/v1/browser/stop?user_id={user_id}&api_key={self.api_key}"
            requests.get(close_url)
