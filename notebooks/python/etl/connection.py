"""
Database Connection
===================
Context manager for database connections with error handling.
"""

import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .config import ETLConfig


class DatabaseConnection:
    """Context manager for database connections with error handling"""
    
    def __init__(self, config: ETLConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.engine = None
        self.connection = None
    
    def __enter__(self):
        try:
            connection_string = (
                f"mysql+pymysql://{self.config.db_user}:{self.config.db_password}"
                f"@{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
            )
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            self.connection = self.engine.connect()
            self.logger.info(f"✓ Connected to database: {self.config.db_name}")
            return self
        except SQLAlchemyError as e:
            self.logger.error(f"✗ Database connection failed: {str(e)}")
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            self.logger.info("✓ Database connection closed")
        if exc_type:
            self.logger.error(f"✗ Error during database operation: {exc_val}")
        return False
