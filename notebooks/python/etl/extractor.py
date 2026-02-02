"""
Data Extractor
==============
Extract data from various sources.
"""

import logging
from typing import Optional, Dict
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from .connection import DatabaseConnection


class DataExtractor:
    """Extract data from various sources"""
    
    def __init__(self, connection: DatabaseConnection, logger: logging.Logger):
        self.connection = connection
        self.logger = logger
    
    def extract_table(
        self, 
        table_name: str, 
        date_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract data from a table with optional date filtering.
        
        Args:
            table_name: Name of the table to extract
            date_column: Optional column name for date filtering
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            DataFrame with extracted data
        """
        try:
            self.logger.info(f"Extracting data from table: {table_name}")
            
            query = f"SELECT * FROM {table_name}"
            
            if date_column and start_date and end_date:
                query += f" WHERE {date_column} BETWEEN :start_date AND :end_date"
                params = {'start_date': start_date, 'end_date': end_date}
                df = pd.read_sql_query(text(query), self.connection.connection, params=params)
            else:
                df = pd.read_sql_query(query, self.connection.connection)
            
            self.logger.info(f"✓ Extracted {len(df):,} rows from {table_name}")
            return df
        except SQLAlchemyError as e:
            self.logger.error(f"✗ Failed to extract from {table_name}: {str(e)}")
            raise
    
    def extract_with_joins(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute custom query with optional parameters.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            DataFrame with query results
        """
        try:
            self.logger.info("Executing custom extraction query")
            if params:
                df = pd.read_sql_query(text(query), self.connection.connection, params=params)
            else:
                df = pd.read_sql_query(query, self.connection.connection)
            self.logger.info(f"✓ Extracted {len(df):,} rows from custom query")
            return df
        except SQLAlchemyError as e:
            self.logger.error(f"✗ Custom query failed: {str(e)}")
            raise
