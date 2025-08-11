"""Database connection for Azure SQL Database (or MySQL as implied by errors)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# noinspection PyUnresolvedReferences
from tvbingefriend_tvmaze_models.models.base import Base

import tvbingefriend_show_sync.config as config

SQLALCHEMY_DATABASE_URL = config.SQLALCHEMY_CONNECTION_STRING

if not SQLALCHEMY_DATABASE_URL:
    raise ValueError("SQLALCHEMY_CONNECTION_STRING is not set in the configuration.")

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=5,          # Number of connections to keep open in the pool
    max_overflow=10,      # Number of connections that can be opened beyond pool_size
    pool_recycle=1800,    # Recycle connections after 30 minutes (important for MySQL)
    pool_timeout=30,      # How long to wait for a connection from the pool
    pool_pre_ping=True    # Enable "pre-ping" to test connections before checkout
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
