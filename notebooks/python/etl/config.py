"""
ETL Configuration
=================
Configuration dataclass for ETL pipeline parameters.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class ETLConfig:
    """Configuration for ETL pipeline"""
    db_host: str = "mysql"
    db_port: int = 3306
    db_user: str = "analytics_user"
    db_password: str = "analyticspass123"
    db_name: str = "supply_chain_db"
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    null_threshold: float = 0.05
    duplicate_threshold: float = 0.01
    log_level: str = "INFO"
    log_file: str = "etl_pipeline.log"


@dataclass
class DataQualityReport:
    """Data quality validation results"""
    table_name: str
    total_rows: int
    null_count: Dict[str, int] = field(default_factory=dict)
    duplicate_count: int = 0
    missing_foreign_keys: Dict[str, int] = field(default_factory=dict)
    validation_passed: bool = True
    issues: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def add_issue(self, issue: str):
        """Add validation issue"""
        self.issues.append(issue)
        self.validation_passed = False
