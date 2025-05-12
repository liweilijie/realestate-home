"""Create SQLAlchemy engine and session objects."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from res_ads.settings import SQLALCHEMY_DATABASE_URI

# Create database engine
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False, pool_recycle=1800, pool_pre_ping=True)

# Create database session
Session = sessionmaker(bind=engine)
session = Session()