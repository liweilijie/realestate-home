import os
import logging
from urllib.parse import urljoin

import requests
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import retry
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from res_ads.settings import NEWS_ACCOUNTS

logger = logging.getLogger(__name__)

class GCSMediaPipeline(ImagesPipeline):

    def __init__(self, store_uri, gcs_credentials_path, gcs_bucket_name, *args, **kwargs):
        super().__init__(store_uri, *args, **kwargs)
        # 其他初始化代码
        self.credentials = service_account.Credentials.from_service_account_file(gcs_credentials_path)
        self.client = storage.Client(credentials=self.credentials)
        self.bucket = self.client.bucket(gcs_bucket_name)
        self.session = self._create_retry_session()

    @classmethod
    def from_crawler(cls, crawler):
        store_uri = crawler.settings.get('IMAGES_STORE')
        gcs_credentials_path = crawler.settings.get('GOOGLE_APPLICATION_CREDENTIALS')
        gcs_bucket_name = crawler.settings.get('GCS_BUCKET_NAME')
        return cls(store_uri, gcs_credentials_path, gcs_bucket_name)

    def process_item(self, item, spider):
        origin_images = item.get('origin_images', [])
        image_meta = item.get('image_meta', {})
        image_index_map = item.get('image_index_in_type', {})
        unique_id = item.get('unique_id', 'unknown')

        if not origin_images:
            raise DropItem("Item does not contain 'origin_images'")

        images_info = []
        floor_plans_info = []

        cdn_prefix = NEWS_ACCOUNTS.get(item.get('name')).get('image_cdn_domain')

        if cdn_prefix is None:
            logger.error('cdn_prefix was not found in item')
            cdn_prefix = 'https://cdn.news.com.au'

        for idx, image_url in enumerate(origin_images):
            try:
                response = self.session.get(image_url, timeout=30)
                response.raise_for_status()
                image_data = response.content
            except Exception as e:
                logger.error(f"Download failed: {image_url}: {e}")
                continue

            image_type = image_meta.get(image_url, 'property')
            image_index = image_index_map.get(image_url, idx)
            filename = f"{unique_id}-{image_type}-{image_index}.jpg"
            blob_path = os.path.join('jiwu', 'realestate', unique_id, filename)
            file_full_path = urljoin(cdn_prefix, blob_path)

            blob = self.bucket.blob(blob_path)
            image_file = BytesIO(image_data)
            image_file.seek(0)

            try:
                blob.upload_from_file(image_file, content_type='image/jpeg', retry=self.retry_strategy())
                logger.info(f"GCS uploaded: {blob_path}")
            except Exception as e:
                logger.error(f"GCS upload failed: {blob_path}: {e}")
                continue

            uploaded = {
                "url": file_full_path,
                "origin_url": image_url,
                "index": image_index,
                "type": image_type,
            }

            # 暂时为了php调试方便，只给一个数组
            if image_type == 'floorplan':
                floor_plans_info.append(file_full_path)
            elif image_type == 'property':
                images_info.append(file_full_path)
            else:
                # 找到agents里面的photo_url, 替换为file_full_path
                # 假设 agents 是一个列表，每个元素是一个字典，包含 'photo_url' 键
                for agent in item["agents"]:
                    if agent.get('photo_url') == image_url:
                        agent['photo_url'] = file_full_path
                        break

        item["images"] = images_info
        item["floor_plan"] = floor_plans_info

        item["images"].append(item["floor_plan"])

        # 处理 PDF
        origin_pdfs = item.get("origin_pdfs", [])
        pdfs_uploaded = []

        for idx, pdf_url in enumerate(origin_pdfs):
            try:
                response = self.session.get(pdf_url, timeout=30)
                response.raise_for_status()
                pdf_data = response.content
            except Exception as e:
                logger.error(f"PDF download failed: {pdf_url}: {e}")
                continue

            filename = f"{unique_id}-{idx}.pdf"
            blob_path = os.path.join('jiwu', 'realestate', unique_id, filename)

            blob = self.bucket.blob(blob_path)
            pdf_file = BytesIO(pdf_data)
            pdf_file.seek(0)

            blob.content_disposition = f'inline; filename="{filename}"'
            blob.content_type = "application/pdf"

            try:
                blob.upload_from_file(pdf_file, retry=self.retry_strategy())
                logger.info(f"PDF GCS uploaded: {blob_path}")
                pdfs_uploaded.append(blob.public_url)
            except Exception as e:
                logger.error(f"PDF upload failed: {blob_path}: {e}")
                continue

        item["statement_pdf"] = pdfs_uploaded

        return item

    def retry_strategy(self):
        return retry.Retry(
            predicate=retry.if_transient_error,
            initial=1.0,
            maximum=60.0,
            multiplier=2.0,
            deadline=300.0
        )

    def _create_retry_session(self, retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)):
        session = requests.Session()
        retry_strage = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strage)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session