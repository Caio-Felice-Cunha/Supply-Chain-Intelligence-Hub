"""
Data Quality Rules Engine
==========================
Define and execute data quality validation rules.
"""

import pandas as pd
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ValidationRule:
    """Individual validation rule definition"""
    rule_name: str
    rule_type: str
    column: Optional[str] = None
    threshold: Optional[float] = None
    condition: Optional[Callable] = None
    severity: str = 'WARNING'
    description: str = ""


@dataclass
class ValidationResult:
    """Result of a validation rule execution"""
    rule_name: str
    passed: bool
    severity: str
    message: str
    affected_rows: int = 0
    affected_percentage: float = 0.0
    details: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


class DataQualityRulesEngine:
    """Engine to define and execute data quality rules"""
    
    def __init__(self):
        self.rules: Dict[str, List[ValidationRule]] = {}
        self.results: List[ValidationResult] = []
    
    def add_rule(self, table_name: str, rule: ValidationRule):
        """Add validation rule for a specific table"""
        if table_name not in self.rules:
            self.rules[table_name] = []
        self.rules[table_name].append(rule)
    
    def define_standard_rules(self):
        """Define standard data quality rules for all tables"""
        
        # SUPPLIERS
        self.add_rule('suppliers', ValidationRule(
            rule_name='supplier_id_unique',
            rule_type='uniqueness',
            column='supplier_id',
            severity='CRITICAL',
            description='Supplier ID must be unique'
        ))
        
        self.add_rule('suppliers', ValidationRule(
            rule_name='reliability_score_range',
            rule_type='validity',
            column='reliability_score',
            condition=lambda df: (df['reliability_score'] >= 0) & (df['reliability_score'] <= 100),
            severity='CRITICAL',
            description='Reliability score must be between 0 and 100'
        ))
        
        # PRODUCTS
        self.add_rule('products', ValidationRule(
            rule_name='product_id_unique',
            rule_type='uniqueness',
            column='product_id',
            severity='CRITICAL',
            description='Product ID must be unique'
        ))
        
        self.add_rule('products', ValidationRule(
            rule_name='unit_cost_positive',
            rule_type='validity',
            column='unit_cost',
            condition=lambda df: df['unit_cost'] > 0,
            severity='CRITICAL',
            description='Unit cost must be positive'
        ))
        
        # INVENTORY
        self.add_rule('inventory', ValidationRule(
            rule_name='quantity_on_hand_valid',
            rule_type='validity',
            column='quantity_on_hand',
            condition=lambda df: df['quantity_on_hand'] >= 0,
            severity='CRITICAL',
            description='Quantity on hand cannot be negative'
        ))
        
        self.add_rule('inventory', ValidationRule(
            rule_name='reserved_not_exceed_onhand',
            rule_type='consistency',
            column='quantity_reserved',
            condition=lambda df: df['quantity_reserved'] <= df['quantity_on_hand'],
            severity='CRITICAL',
            description='Reserved quantity cannot exceed quantity on hand'
        ))
        
        # ORDERS
        self.add_rule('orders', ValidationRule(
            rule_name='order_quantity_positive',
            rule_type='validity',
            column='order_quantity',
            condition=lambda df: df['order_quantity'] > 0,
            severity='CRITICAL',
            description='Order quantity must be positive'
        ))
        
        # SALES
        self.add_rule('sales', ValidationRule(
            rule_name='quantity_sold_positive',
            rule_type='validity',
            column='quantity_sold',
            condition=lambda df: df['quantity_sold'] > 0,
            severity='CRITICAL',
            description='Quantity sold must be positive'
        ))
    
    def execute_rules(self, df: pd.DataFrame, table_name: str) -> List[ValidationResult]:
        """Execute all rules for a given table"""
        if table_name not in self.rules:
            return []
        
        results = []
        total_rows = len(df)
        
        for rule in self.rules[table_name]:
            try:
                if rule.rule_type == 'uniqueness':
                    result = self._check_uniqueness(df, rule, total_rows)
                elif rule.rule_type == 'completeness':
                    result = self._check_completeness(df, rule, total_rows)
                elif rule.rule_type in ['validity', 'consistency']:
                    result = self._check_condition(df, rule, total_rows)
                else:
                    continue
                
                results.append(result)
            except Exception as e:
                results.append(ValidationResult(
                    rule_name=rule.rule_name,
                    passed=False,
                    severity='CRITICAL',
                    message=f"Rule execution failed: {str(e)}"
                ))
        
        self.results.extend(results)
        return results
    
    def _check_uniqueness(self, df: pd.DataFrame, rule: ValidationRule, 
                         total_rows: int) -> ValidationResult:
        """Check if column values are unique"""
        duplicates = df[rule.column].duplicated().sum()
        passed = duplicates == 0
        
        return ValidationResult(
            rule_name=rule.rule_name,
            passed=passed,
            severity=rule.severity,
            message=f"{'✓ PASS' if passed else '✗ FAIL'}: {rule.description}",
            affected_rows=duplicates,
            affected_percentage=(duplicates / total_rows * 100) if total_rows > 0 else 0,
            details={'duplicate_count': int(duplicates)}
        )
    
    def _check_completeness(self, df: pd.DataFrame, rule: ValidationRule,
                           total_rows: int) -> ValidationResult:
        """Check for null/missing values"""
        null_count = df[rule.column].isnull().sum()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        passed = null_pct <= (rule.threshold or 0.0)
        
        return ValidationResult(
            rule_name=rule.rule_name,
            passed=passed,
            severity=rule.severity,
            message=f"{'✓ PASS' if passed else '✗ FAIL'}: {rule.description}",
            affected_rows=null_count,
            affected_percentage=null_pct,
            details={'null_count': int(null_count), 'null_percentage': null_pct}
        )
    
    def _check_condition(self, df: pd.DataFrame, rule: ValidationRule,
                        total_rows: int) -> ValidationResult:
        """Check custom condition"""
        if rule.condition is None:
            return ValidationResult(
                rule_name=rule.rule_name,
                passed=False,
                severity='CRITICAL',
                message="No condition defined for rule"
            )
        
        try:
            valid = rule.condition(df)
            invalid_count = (~valid).sum()
            passed = invalid_count == 0
            
            return ValidationResult(
                rule_name=rule.rule_name,
                passed=passed,
                severity=rule.severity,
                message=f"{'✓ PASS' if passed else '✗ FAIL'}: {rule.description}",
                affected_rows=invalid_count,
                affected_percentage=(invalid_count / total_rows * 100) if total_rows > 0 else 0,
                details={'invalid_count': int(invalid_count)}
            )
        except Exception as e:
            return ValidationResult(
                rule_name=rule.rule_name,
                passed=False,
                severity='CRITICAL',
                message=f"Condition evaluation failed: {str(e)}"
            )
    
    def get_summary(self) -> pd.DataFrame:
        """Get summary of all validation results"""
        summary_data = []
        
        for result in self.results:
            summary_data.append({
                'rule_name': result.rule_name,
                'status': '✓ PASS' if result.passed else '✗ FAIL',
                'severity': result.severity,
                'affected_rows': result.affected_rows,
                'affected_percentage': f"{result.affected_percentage:.2f}%",
                'timestamp': result.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return pd.DataFrame(summary_data)
