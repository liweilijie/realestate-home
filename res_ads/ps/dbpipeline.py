import datetime
import hashlib

from sqlalchemy.orm import sessionmaker

from res_ads.db import engine
from res_ads.db.models import Listing

import logging
import datetime
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger('listing')
Session = sessionmaker(bind=engine)

class ListingStorePipeline:
    def open_spider(self, spider):
        self.session = Session()

    def close_spider(self, spider):
        self.session.close()

    def process_item(self, item, spider):
        logger.info('processing item:%s', item)
        unique_id = item.get('unique_id')
        if not unique_id:
            logger.error("Item missing unique_id, skipping.")
            return item

        # 查找是否已存在此 unique_id
        existing = self.session.query(Listing).filter_by(unique_id=unique_id).first()

        now = datetime.datetime.utcnow()

        # 准备要写入的字段（字段名需与 Listing 模型一致）
        data = {
            'url': item.get('url'),
            'url_md5': item.get('url_md5'),
            'title': item.get('title'),
            'property_type': item.get('property_type'),
            'listing_type': item.get('listing_type'),
            'address': item.get('address'),
            'street': item.get('street'),
            'suburb': item.get('suburb'),
            'state': item.get('state'),
            'postcode': item.get('postcode'),
            'price_text': item.get('price_text'),
            'lower_price': item.get('lower_price'),
            'upper_price': item.get('upper_price'),
            'bedrooms': item.get('bedrooms'),
            'bathrooms': item.get('bathrooms'),
            'car_spaces': item.get('car_spaces'),
            'land_size': item.get('land_size'),
            'description_title': item.get('description_title'),
            'description': item.get('description'),
            'council_rates': item.get('council_rates'),
            'features': item.get('features'),
            'images': item.get('images'),
            'floor_plan': item.get('floor_plan'),
            'statement_pdf': item.get('statement_pdf'),
            'latitude': item.get('latitude'),
            'longitude': item.get('longitude'),
            'agents': item.get('agents'),
            'agency': item.get('agency'),
            'publish_date': item.get('publish_date'),
            'updated_at': now
        }

        try:
            if existing:
                # 更新已有字段
                for field, value in data.items():
                    setattr(existing, field, value)
                logger.info(f"Updated existing listing: {unique_id}, data:{data}")
            else:
                # 新建对象
                new_listing = Listing(
                    unique_id=unique_id,
                    created_at=now,
                    **data
                )
                self.session.add(new_listing)
                logger.info(f"Inserted new listing: {unique_id}, data:{data}")

            self.session.commit()
        except IntegrityError as e:
            logger.error(f"Database integrity error: {e}, data:{data}")
            self.session.rollback()
        except Exception as e:
            logger.exception(f"Unexpected error: {e}, data:{data}")
            self.session.rollback()

        return item
