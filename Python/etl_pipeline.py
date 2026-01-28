# !pip install pandas numpy sqlalchemy scipy
# !pip install pymysql
# !pip install cryptography
# !pip install python-dotenv

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlalchemy
import logging
from scipy import stats
import os
from dotenv import load_dotenv

load_dotenv(r"C:\Users\caiof\OneDrive\Desktop PC\Desktop\Documentos\GitHub\Supply-Chain-Intelligence-Hub\.env")

DB_CONNECTION = (
    f"mysql+pymysql://"
    f"{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}/"
    f"{os.getenv('DB_NAME')}"
)

# print(f"Connecting to: {os.getenv('DB_NAME')} as {os.getenv('DB_USER')}")

# ============ CONFIGURATION ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# DB_CONNECTION = "mysql+pymysql://user:password@localhost/supply_chain_analytics"

class SupplyChainETL:
    """
    ETL Pipeline for Supply Chain Intelligence Hub
    """
    
    def __init__(self, connection_string):
        self.engine = sqlalchemy.create_engine(connection_string)
        self.logger = logging.getLogger(__name__)
    
    # ============ EXTRACT PHASE ============
    
    def extract_raw_data(self):
        """Extract data from source systems (simulated)"""
        self.logger.info("Starting EXTRACT phase...")
        
        # In production: API calls, CSV imports, etc.
        # For now: Load from database
        query = """
        SELECT 
            o.order_id,
            o.order_date,
            s.supplier_id,
            s.supplier_name,
            o.order_quantity,
            o.order_cost,
            o.expected_delivery_date,
            o.actual_delivery_date,
            DATEDIFF(o.actual_delivery_date, o.expected_delivery_date) as days_late
        FROM orders o
        JOIN suppliers s ON o.supplier_id = s.supplier_id
        WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        """
        
        df = pd.read_sql(query, self.engine)
        self.logger.info(f"Extracted {len(df)} records")
        return df
    
    # ============ TRANSFORM PHASE ============
    
    def transform_order_data(self, df):
        """Data cleaning and transformation"""
        self.logger.info("Starting TRANSFORM phase...")
        
        # Data Type Conversion
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['actual_delivery_date'] = pd.to_datetime(df['actual_delivery_date'])
        df['expected_delivery_date'] = pd.to_datetime(df['expected_delivery_date'])
        
        # Handle Nulls
        df['days_late'] = df['days_late'].fillna(0)
        
        # Feature Engineering
        df['delivery_on_time'] = df['days_late'] <= 0
        df['delivery_late_critical'] = df['days_late'] > 7
        df['cost_per_unit'] = df['order_cost'] / df['order_quantity']
        
        # Add Time Dimensions
        df['week_of_year'] = df['order_date'].dt.isocalendar().week
        df['month'] = df['order_date'].dt.month
        df['quarter'] = df['order_date'].dt.quarter
        df['year'] = df['order_date'].dt.year
        
        # Outlier Detection (using Z-score)
        df['cost_per_unit_zscore'] = np.abs(
            stats.zscore(df['cost_per_unit'].fillna(0))
        )
        df['is_outlier'] = df['cost_per_unit_zscore'] > 3
        
        self.logger.info("Transformation completed")
        return df
    
    # ============ LOAD PHASE ============
    
    def load_to_warehouse(self, df, table_name):
        """Load cleaned data to data warehouse"""
        self.logger.info(f"Loading {len(df)} records to {table_name}...")
        
        # Create staging table
        staging_table = f"{table_name}_staging"
        df.to_sql(staging_table, self.engine, if_exists='replace', index=False)
        
        # Merge into main table (UPSERT)
        with self.engine.connect() as conn:
            conn.execute(f"""
                REPLACE INTO {table_name}
                SELECT * FROM {staging_table}
            """)
            conn.commit()
        
        self.logger.info(f"Successfully loaded to {table_name}")
    
    def run_full_pipeline(self):
        """Execute complete ETL pipeline"""
        try:
            raw_data = self.extract_raw_data()
            transformed_data = self.transform_order_data(raw_data)
            self.load_to_warehouse(transformed_data, 'orders_analytics')
            self.logger.info("ETL Pipeline completed successfully!")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise


class DataQualityValidator:
    """Data Quality Checks"""
    
    def __init__(self, connection_string):
        self.engine = sqlalchemy.create_engine(connection_string)
    
    def validate_inventory_consistency(self):
        """Ensure inventory >= 0"""
        query = """
        SELECT COUNT(*) as negative_inventory
        FROM inventory
        WHERE quantity_on_hand < 0 OR quantity_reserved < 0
        """
        result = pd.read_sql(query, self.engine)
        return result['negative_inventory'][0] == 0
    
    def validate_delivery_dates(self):
        """Ensure actual_delivery >= expected_delivery OR flag"""
        query = """
        SELECT COUNT(*) as invalid_dates
        FROM orders
        WHERE actual_delivery_date < order_date
        """
        result = pd.read_sql(query, self.engine)
        return result['invalid_dates'][0] == 0
    
    def validate_no_future_sales(self):
        """Sales should not be in future"""
        query = """
        SELECT COUNT(*) as future_sales
        FROM sales
        WHERE sale_date > CURDATE()
        """
        result = pd.read_sql(query, self.engine)
        return result['future_sales'][0] == 0


# ============ EXECUTION ============

if __name__ == "__main__":
    # Initialize ETL
    etl = SupplyChainETL(DB_CONNECTION)
    etl.run_full_pipeline()
    
    # Validate data quality
    validator = DataQualityValidator(DB_CONNECTION)
    assert validator.validate_inventory_consistency(), "Inventory validation failed"
    assert validator.validate_delivery_dates(), "Delivery date validation failed"
    assert validator.validate_no_future_sales(), "Sales date validation failed"
    
    print("✅ All validations passed!")








