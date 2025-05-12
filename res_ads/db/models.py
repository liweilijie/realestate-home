from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Numeric,
    UniqueConstraint, Index, CHAR
)
from sqlalchemy.dialects.mysql import TINYINT, JSON
from sqlalchemy.orm import declarative_base
import datetime
from res_ads.db import engine

Base = declarative_base()

class Listing(Base):
    __tablename__ = 'wp_listings'

    __table_args__ = (
        UniqueConstraint('unique_id', name='uq_listing_unique_id'),
        UniqueConstraint('url', name='uq_listing_url'),
        UniqueConstraint('url_md5', name='uq_url_md5'),
        Index('idx_listing_status', 'status'),
        Index('idx_listing_state_postcode', 'state', 'postcode'),
        Index('idx_listing_type', 'listing_type'),
        Index('idx_listing_price', 'lower_price', 'upper_price'),
        Index('idx_listing_geo', 'latitude', 'longitude'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键")
    url = Column(String(500), nullable=False, comment="页面 URL")
    url_md5 = Column(CHAR(32), nullable=False, comment="URL 的 MD5 哈希值")
    unique_id = Column(String(50), nullable=False, comment="网站房屋ID")
    title = Column(String(255), nullable=True, comment="房源标题/名称")
    address = Column(String(500), nullable=True, comment="完整地址")
    street = Column(String(500), nullable=True, comment="街道")
    suburb = Column(String(500), nullable=True, comment="所属郊区")
    state = Column(String(50), nullable=True, comment="州/地区")
    postcode = Column(String(10), nullable=True, comment="邮政编码")

    price_text = Column(String(255), nullable=True, comment="价格文本信息")
    lower_price = Column(Integer, nullable=True, comment="最低价格")
    upper_price = Column(Integer, nullable=True, comment="最高价格")

    property_type = Column(String(50), nullable=True, comment="房产类型")
    listing_type = Column(String(20), nullable=True, comment="房源类型（出售/出租/已售）")

    bedrooms = Column(Integer, nullable=True, comment="卧室数量")
    bathrooms = Column(Integer, nullable=True, comment="浴室数量")
    car_spaces = Column(Integer, nullable=True, comment="车位数量")
    # land_size = Column(Numeric(10, 2), nullable=True, comment="土地/建筑面积（平方米）")
    land_size = Column(Integer, nullable=True, comment="土地/建筑面积（平方米）")

    description_title = Column(String(500), nullable=True, comment="房产描述标题")
    description = Column(Text, nullable=True, comment="房产描述")
    council_rates = Column(String(50), nullable=True, comment="市政费")

    features = Column(JSON, nullable=True, comment="其他特性或配置")
    images = Column(JSON, nullable=True, comment="房屋图片URL列表")
    floor_plan = Column(JSON, nullable=True, comment="楼层平面图图片URL列表")
    statement_pdf = Column(JSON, nullable=True, comment="房屋PDF文档URL")

    latitude = Column(Numeric(11, 7), nullable=True, comment="纬度")
    longitude = Column(Numeric(11, 7), nullable=True, comment="经度")

    agents = Column(JSON, nullable=True, comment="代理人信息列表")
    agency = Column(JSON, nullable=True, comment="机构信息")

    status = Column(TINYINT, default=0, nullable=False, comment="数据处理状态：0=初始，1=已处理，2=无效")
    post_id = Column(Integer, nullable=True, comment="关联的 WordPress property ID")

    publish_date = Column(DateTime, default=datetime.datetime.utcnow, nullable=False, comment="房源发布时间")
    created_at = Column(DateTime, default=datetime.datetime.utcnow, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, comment="更新时间")

    def __repr__(self):
        return f"<Listing(id={self.id}, unique_id='{self.unique_id}', address='{self.address}')>"

# 创建所有表
Base.metadata.create_all(engine)