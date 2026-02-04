"""
Data Transformer
================
Transform and clean extracted data.
"""

import logging
from typing import List, Optional
import pandas as pd


class DataTransformer:
    """Transform and clean extracted data"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def clean_nulls(self, df: pd.DataFrame, strategy: str = 'drop') -> pd.DataFrame:
        """
        Handle null values using specified strategy.
        
        Args:
            df: DataFrame to clean
            strategy: Strategy to use ('drop', 'fill_mean', 'fill_median', 'fill_forward')
            
        Returns:
            Cleaned DataFrame
        """
        null_count = df.isnull().sum().sum()
        self.logger.info(f"Handling {null_count} null values using strategy: {strategy}")
        
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'fill_mean':
            return df.fillna(df.mean(numeric_only=True))
        elif strategy == 'fill_median':
            return df.fillna(df.median(numeric_only=True))
        elif strategy == 'fill_forward':
            return df.fillna(method='ffill')
        else:
            self.logger.warning(f"Unknown strategy '{strategy}', returning original DataFrame")
            return df
    
    def remove_duplicates(
        self, 
        df: pd.DataFrame, 
        subset: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Remove duplicate rows.
        
        Args:
            df: DataFrame to deduplicate
            subset: Optional subset of columns to consider
            
        Returns:
            Deduplicated DataFrame
        """
        initial_rows = len(df)
        df_clean = df.drop_duplicates(subset=subset, keep='first')
        removed = initial_rows - len(df_clean)
        
        if removed > 0:
            self.logger.warning(f"Removed {removed} duplicate rows")
        else:
            self.logger.info("✓ No duplicates found")
        
        return df_clean
    
    def standardize_dates(
        self, 
        df: pd.DataFrame, 
        date_columns: List[str],
        date_format: str = '%Y-%m-%d'
    ) -> pd.DataFrame:
        """
        Standardize date columns to datetime format.
        
        Args:
            df: DataFrame with date columns
            date_columns: List of column names to standardize
            date_format: Expected date format
            
        Returns:
            DataFrame with standardized dates
        """
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    self.logger.info(f"✓ Standardized date column: {col}")
                except Exception as e:
                    self.logger.error(f"✗ Failed to standardize {col}: {str(e)}")
        return df
    
    def add_derived_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Add derived/calculated columns based on table type.
        
        Args:
            df: DataFrame to enhance
            table_name: Name of the table (determines which columns to add)
            
        Returns:
            DataFrame with derived columns
        """
        self.logger.info(f"Adding derived columns for {table_name}")
        
        if table_name == 'inventory':
            if 'quantity_on_hand' in df.columns and 'quantity_reserved' in df.columns:
                df['quantity_available'] = df['quantity_on_hand'] - df['quantity_reserved']
                self.logger.info("✓ Added 'quantity_available' column")
        
        elif table_name == 'orders':
            if 'expected_delivery_date' in df.columns and 'actual_delivery_date' in df.columns:
                df['delivery_delay_days'] = (
                    pd.to_datetime(df['actual_delivery_date']) - 
                    pd.to_datetime(df['expected_delivery_date'])
                ).dt.days
                df['is_late'] = df['delivery_delay_days'] > 0
                self.logger.info("✓ Added delivery metrics columns")
        
        elif table_name == 'sales':
            if 'revenue' in df.columns and 'quantity_sold' in df.columns:
                df['unit_price'] = df['revenue'] / df['quantity_sold']
                self.logger.info("✓ Added 'unit_price' column")
        
        return df
    
    def apply_business_rules(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply domain-specific business rules to filter invalid data.
        
        Args:
            df: DataFrame to filter
            table_name: Name of the table (determines which rules to apply)
            
        Returns:
            Filtered DataFrame
        """
        self.logger.info(f"Applying business rules for {table_name}")
        initial_rows = len(df)
        
        if table_name == 'products':
            df = df[df['unit_cost'] > 0]
            df = df[df['reorder_level'] >= 0]
        elif table_name == 'inventory':
            df = df[df['quantity_on_hand'] >= 0]
            df = df[df['quantity_reserved'] >= 0]
        elif table_name == 'orders':
            df = df[df['order_quantity'] > 0]
            df = df[df['order_cost'] >= 0]
        elif table_name == 'sales':
            df = df[df['quantity_sold'] > 0]
            df = df[df['revenue'] >= 0]
        
        removed = initial_rows - len(df)
        if removed > 0:
            self.logger.warning(f"Removed {removed} rows violating business rules")
        
        return df
