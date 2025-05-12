import requests, time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import sys

ads_id = "kxrqp1l"
#open_url = "http://local.adspower.net:50325/api/v1/browser/start?user_id=" + ads_id
open_url = "http://127.0.0.1:50325/api/v1/browser/start?user_id=" + ads_id
close_url = "http://local.adspower.net:50325/api/v1/browser/stop?user_id=" + ads_id

resp = requests.get(open_url).json()
print(resp)
if resp["code"] != 0:
    print(resp["msg"])
    print("please check ads_id")
    sys.exit()

chrome_driver = resp["data"]["webdriver"]
service = Service(executable_path=chrome_driver)
chrome_options = Options()

chrome_options.add_experimental_option("debuggerAddress", resp["data"]["ws"]["selenium"])
driver = webdriver.Chrome(service=service, options=chrome_options)

print(driver.title)
driver.get("https://www.browserscan.net/")
time.sleep(5)
driver.quit()
requests.get(close_url)
