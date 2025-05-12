from res_ads.db.models import Listing
from res_ads.db import session


class ListingHelper:
    @staticmethod
    def exists_by_url_md5(url_md5: str) -> bool:
        """根据 url_md5 判断 Listing 是否已存在"""
        return session.query(Listing.id).filter_by(url_md5=url_md5).first() is not None

    @staticmethod
    def exists_by_unique_id(unique_id: str) -> bool:
        """根据 unique_id 判断 Listing 是否已存在"""
        return session.query(Listing.id).filter_by(unique_id=unique_id).first() is not None
