"""
Anomaly Detector
================
Detect anomalies in data using statistical methods.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
from scipy import stats
from sklearn.ensemble import IsolationForest


class AnomalyDetector:
    """Detect anomalies in data using statistical methods"""
    
    @staticmethod
    def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
        """Detect outliers using IQR method"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        return (series < lower_bound) | (series > upper_bound)
    
    @staticmethod
    def detect_outliers_zscore(series: pd.Series, threshold: float = 3.0) -> pd.Series:
        """Detect outliers using Z-score method"""
        z_scores = np.abs(stats.zscore(series.dropna()))
        outliers = pd.Series(False, index=series.index)
        outliers.loc[series.dropna().index] = z_scores > threshold
        return outliers
    
    @staticmethod
    def detect_outliers_isolation_forest(
        df: pd.DataFrame, 
        columns: List[str],
        contamination: float = 0.1
    ) -> np.ndarray:
        """Detect multivariate outliers using Isolation Forest"""
        X = df[columns].select_dtypes(include=[np.number]).dropna()
        
        if len(X) == 0:
            return np.array([])
        
        iso_forest = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        
        predictions = iso_forest.fit_predict(X)
        return predictions
    
    @staticmethod
    def analyze_table_anomalies(df: pd.DataFrame, table_name: str) -> Dict:
        """Comprehensive anomaly analysis for a table"""
        results = {
            'table_name': table_name,
            'total_rows': len(df),
            'columns_analyzed': [],
            'outliers_detected': {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if df[col].nunique() > 10:  # Only analyze columns with sufficient variation
                outliers_iqr = AnomalyDetector.detect_outliers_iqr(df[col])
                outlier_count = outliers_iqr.sum()
                
                results['columns_analyzed'].append(col)
                results['outliers_detected'][col] = {
                    'method': 'IQR',
                    'count': int(outlier_count),
                    'percentage': float(outlier_count / len(df) * 100),
                    'outlier_values': df[col][outliers_iqr].tolist()[:10]  # First 10 outliers
                }
        
        return results
    
    @staticmethod
    def get_outlier_summary(anomaly_results: Dict) -> pd.DataFrame:
        """Convert anomaly results to summary DataFrame"""
        summary_data = []
        
        for col, data in anomaly_results['outliers_detected'].items():
            summary_data.append({
                'column': col,
                'method': data['method'],
                'outlier_count': data['count'],
                'outlier_percentage': f"{data['percentage']:.2f}%"
            })
        
        return pd.DataFrame(summary_data)
