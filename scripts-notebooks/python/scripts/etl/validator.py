"""
Data Quality Validator
======================
Comprehensive data quality checks during ETL execution.
"""

import logging
from typing import Optional, List, Dict
import pandas as pd
from .config import ETLConfig, DataQualityReport
from .connection import DatabaseConnection


class DataQualityValidator:
    """Comprehensive data quality checks"""
    
    def __init__(
        self, 
        connection: DatabaseConnection, 
        config: ETLConfig, 
        logger: logging.Logger
    ):
        self.connection = connection
        self.config = config
        self.logger = logger
    
    def validate_table(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        required_columns: Optional[List[str]] = None
    ) -> DataQualityReport:
        """
        Validate data quality for a table.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            required_columns: Optional list of required columns
            
        Returns:
            DataQualityReport with validation results
        """
        self.logger.info(f"=== Validating data quality for: {table_name} ===")
        report = DataQualityReport(table_name=table_name, total_rows=len(df))
        
        # Check required columns
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                report.add_issue(f"Missing required columns: {missing_cols}")
                self.logger.error(f"✗ Missing columns: {missing_cols}")
        
        # Check null counts
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                null_pct = count / len(df)
                report.null_count[col] = count
                
                if null_pct > self.config.null_threshold:
                    report.add_issue(
                        f"Column '{col}' has {null_pct:.1%} nulls (threshold: {self.config.null_threshold:.1%})"
                    )
                    self.logger.warning(f"⚠ High null count in '{col}': {count} ({null_pct:.1%})")
        
        # Check duplicates
        if len(df) > 0:
            duplicate_count = df.duplicated().sum()
            report.duplicate_count = duplicate_count
            
            if duplicate_count > 0:
                dup_pct = duplicate_count / len(df)
                if dup_pct > self.config.duplicate_threshold:
                    report.add_issue(f"Found {duplicate_count} duplicates ({dup_pct:.1%})")
                    self.logger.warning(f"⚠ Duplicates found: {duplicate_count}")
        
        # Check foreign keys
        fk_issues = self._validate_foreign_keys(df, table_name)
        if fk_issues:
            report.missing_foreign_keys = fk_issues
            for fk, count in fk_issues.items():
                report.add_issue(f"Missing foreign key references in '{fk}': {count} rows")
                self.logger.error(f"✗ Foreign key issue in '{fk}': {count} orphaned rows")
        
        # Log final result
        if report.validation_passed:
            self.logger.info(f"✓ Data quality validation PASSED for {table_name}")
        else:
            self.logger.error(f"✗ Data quality validation FAILED for {table_name}")
        
        return report
    
    def _validate_foreign_keys(self, df: pd.DataFrame, table_name: str) -> Dict[str, int]:
        """
        Validate foreign key relationships.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            
        Returns:
            Dictionary of foreign key issues {column: orphaned_count}
        """
        issues = {}
        fk_mapping = {
            'products': {'supplier_id': 'suppliers'},
            'inventory': {'product_id': 'products', 'warehouse_id': 'warehouses'},
            'orders': {'supplier_id': 'suppliers'},
            'sales': {'product_id': 'products', 'warehouse_id': 'warehouses'},
            'price_history': {'product_id': 'products', 'supplier_id': 'suppliers'}
        }
        
        if table_name not in fk_mapping:
            return issues
        
        for fk_col, parent_table in fk_mapping[table_name].items():
            if fk_col not in df.columns:
                continue
            
            try:
                parent_ids = pd.read_sql_query(
                    f"SELECT {fk_col.replace('_id', '')}_id FROM {parent_table}",
                    self.connection.connection
                )
                valid_ids = set(parent_ids.iloc[:, 0])
                orphaned = ~df[fk_col].isin(valid_ids)
                orphaned_count = orphaned.sum()
                
                if orphaned_count > 0:
                    issues[fk_col] = orphaned_count
            except Exception as e:
                self.logger.warning(f"Could not validate FK {fk_col}: {str(e)}")
        
        return issues
    
    def generate_quality_summary(self, reports: List[DataQualityReport]) -> pd.DataFrame:
        """
        Generate summary DataFrame from validation reports.
        
        Args:
            reports: List of DataQualityReport objects
            
        Returns:
            Summary DataFrame
        """
        summary_data = []
        for report in reports:
            summary_data.append({
                'table_name': report.table_name,
                'total_rows': report.total_rows,
                'null_columns': len(report.null_count),
                'total_nulls': sum(report.null_count.values()),
                'duplicates': report.duplicate_count,
                'fk_issues': len(report.missing_foreign_keys),
                'validation_passed': report.validation_passed,
                'issue_count': len(report.issues),
                'timestamp': report.timestamp
            })
        return pd.DataFrame(summary_data)
