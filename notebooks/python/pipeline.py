"""
ETL Pipeline Orchestrator
=========================
Main orchestrator for the complete ETL process.
"""

import logging
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd
from etl import (
    ETLConfig,
    DatabaseConnection,
    DataExtractor,
    DataTransformer,
    DataQualityValidator,
    DataLoader
)


class ETLPipeline:
    """Complete ETL pipeline orchestrator"""
    
    def __init__(self, config: ETLConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.execution_stats = {
            'start_time': None,
            'end_time': None,
            'tables_processed': 0,
            'total_rows_extracted': 0,
            'total_rows_loaded': 0,
            'failed_tables': []
        }
    
    def run_full_pipeline(
        self, 
        tables: List[str],
        enable_validation: bool = True,
        enable_transformation: bool = True
    ) -> Dict:
        """
        Run complete ETL pipeline for specified tables.
        
        Args:
            tables: List of table names to process
            enable_validation: Whether to run data quality validation
            enable_transformation: Whether to run transformations
            
        Returns:
            Dictionary with execution statistics
        """
        self.execution_stats['start_time'] = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("ETL PIPELINE EXECUTION STARTED")
        self.logger.info("=" * 80)
        
        try:
            with DatabaseConnection(self.config, self.logger) as db:
                extractor = DataExtractor(db, self.logger)
                transformer = DataTransformer(self.logger)
                validator = DataQualityValidator(db, self.config, self.logger)
                loader = DataLoader(db, self.config, self.logger)
                
                for table in tables:
                    try:
                        self._process_table(
                            table, 
                            extractor, 
                            transformer, 
                            validator, 
                            loader,
                            enable_validation,
                            enable_transformation
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to process {table}: {str(e)}")
                        self.execution_stats['failed_tables'].append(table)
        
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise
        finally:
            self.execution_stats['end_time'] = datetime.now()
            self._log_summary()
        
        return self.execution_stats
    
    def _process_table(
        self,
        table: str,
        extractor: DataExtractor,
        transformer: DataTransformer,
        validator: DataQualityValidator,
        loader: DataLoader,
        enable_validation: bool,
        enable_transformation: bool
    ):
        """Process a single table through ETL pipeline"""
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"Processing table: {table}")
        self.logger.info(f"{'='*70}")
        
        # EXTRACT
        df = extractor.extract_table(table)
        self.execution_stats['total_rows_extracted'] += len(df)
        
        # TRANSFORM
        if enable_transformation:
            df = transformer.remove_duplicates(df)
            
            # Standardize date columns
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            if date_cols:
                df = transformer.standardize_dates(df, date_cols)
            
            # Add derived columns
            df = transformer.add_derived_columns(df, table)
            
            # Apply business rules
            df = transformer.apply_business_rules(df, table)
        
        # VALIDATE
        if enable_validation:
            report = validator.validate_table(df, table)
            
            if not report.validation_passed:
                self.logger.warning(f"⚠ Validation issues found in {table}")
                for issue in report.issues:
                    self.logger.warning(f"  - {issue}")
            else:
                self.logger.info(f"✓ Validation passed for {table}")
        
        # LOAD (optional - uncomment to enable loading)
        # rows_loaded, rows_failed = loader.load_data(df, f"{table}_processed")
        # self.execution_stats['total_rows_loaded'] += rows_loaded
        
        self.execution_stats['tables_processed'] += 1
    
    def _log_summary(self):
        """Log pipeline execution summary"""
        duration = (self.execution_stats['end_time'] - 
                   self.execution_stats['start_time']).total_seconds()
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("ETL PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Start time: {self.execution_stats['start_time']}")
        self.logger.info(f"End time: {self.execution_stats['end_time']}")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(f"Tables processed: {self.execution_stats['tables_processed']}")
        self.logger.info(f"Total rows extracted: {self.execution_stats['total_rows_extracted']:,}")
        self.logger.info(f"Total rows loaded: {self.execution_stats['total_rows_loaded']:,}")
        
        if self.execution_stats['failed_tables']:
            self.logger.error(f"Failed tables: {', '.join(self.execution_stats['failed_tables'])}")
        else:
            self.logger.info("✓ All tables processed successfully")
        
        self.logger.info("=" * 80)


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Configure logging for ETL pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger('ETL_Pipeline')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(console_formatter)
        logger.addHandler(file_handler)
    
    return logger