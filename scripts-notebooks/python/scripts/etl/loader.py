"""
Data Loader
===========
Load transformed data into database.
"""

import logging
from typing import Tuple
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from .config import ETLConfig
from .connection import DatabaseConnection


class DataLoader:
    """Load transformed data into database"""
    
    def __init__(
        self, 
        connection: DatabaseConnection, 
        config: ETLConfig, 
        logger: logging.Logger
    ):
        self.connection = connection
        self.config = config
        self.logger = logger
    
    def load_data(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'append',
        create_backup: bool = True
    ) -> Tuple[int, int]:
        """
        Load data into database with batch processing.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            create_backup: Whether to create backup before replace
            
        Returns:
            Tuple of (rows_loaded, rows_failed)
        """
        self.logger.info(f"Loading {len(df)} rows into table: {table_name}")
        
        rows_loaded = 0
        rows_failed = 0
        
        try:
            if create_backup and if_exists == 'replace':
                self._create_backup(table_name)
            
            # Batch loading
            for i in range(0, len(df), self.config.batch_size):
                batch = df.iloc[i:i + self.config.batch_size]
                
                try:
                    batch.to_sql(
                        name=table_name,
                        con=self.connection.engine,
                        if_exists=if_exists if i == 0 else 'append',
                        index=False,
                        method='multi'
                    )
                    rows_loaded += len(batch)
                except IntegrityError as e:
                    self.logger.error(f"Integrity error in batch: {str(e)}")
                    rows_failed += len(batch)
                except SQLAlchemyError as e:
                    self.logger.error(f"Database error in batch: {str(e)}")
                    rows_failed += len(batch)
            
            self.logger.info(f"✓ Load completed: {rows_loaded} rows loaded, {rows_failed} rows failed")
            return rows_loaded, rows_failed
        except Exception as e:
            self.logger.error(f"✗ Load failed: {str(e)}")
            raise
    
    def _create_backup(self, table_name: str):
        """
        Create backup table before replacement.
        
        Args:
            table_name: Table to backup
        """
        backup_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            with self.connection.connection.begin():
                self.connection.connection.execute(
                    text(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
                )
            self.logger.info(f"✓ Created backup table: {backup_name}")
        except SQLAlchemyError as e:
            self.logger.warning(f"Could not create backup: {str(e)}")
