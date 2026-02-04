"""
Data Profiler
=============
Generate comprehensive data profiles for quality analysis.
"""

import pandas as pd
from typing import Dict


class DataProfiler:
    """Generate comprehensive data profiles"""
    
    @staticmethod
    def profile_numeric_column(series: pd.Series) -> Dict:
        """Profile a numeric column"""
        return {
            'count': int(series.count()),
            'missing': int(series.isnull().sum()),
            'mean': float(series.mean()),
            'median': float(series.median()),
            'std': float(series.std()),
            'min': float(series.min()),
            'max': float(series.max()),
            'q25': float(series.quantile(0.25)),
            'q75': float(series.quantile(0.75)),
            'skewness': float(series.skew()),
            'kurtosis': float(series.kurtosis())
        }
    
    @staticmethod
    def profile_categorical_column(series: pd.Series) -> Dict:
        """Profile a categorical column"""
        value_counts = series.value_counts()
        
        return {
            'count': int(series.count()),
            'missing': int(series.isnull().sum()),
            'unique': int(series.nunique()),
            'top_value': str(value_counts.index[0]) if len(value_counts) > 0 else None,
            'top_frequency': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
            'value_distribution': value_counts.head(10).to_dict()
        }
    
    @staticmethod
    def profile_date_column(series: pd.Series) -> Dict:
        """Profile a date column"""
        series_dt = pd.to_datetime(series, errors='coerce')
        
        return {
            'count': int(series_dt.count()),
            'missing': int(series_dt.isnull().sum()),
            'min_date': str(series_dt.min()),
            'max_date': str(series_dt.max()),
            'date_range_days': int((series_dt.max() - series_dt.min()).days) if series_dt.count() > 0 else 0
        }
    
    @staticmethod
    def profile_dataframe(df: pd.DataFrame, table_name: str) -> Dict:
        """Generate comprehensive profile for entire DataFrame"""
        profile = {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'duplicate_rows': int(df.duplicated().sum()),
            'columns': {}
        }
        
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': float(df[col].isnull().sum() / len(df) * 100) if len(df) > 0 else 0
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile['stats'] = DataProfiler.profile_numeric_column(df[col])
            elif pd.api.types.is_datetime64_any_dtype(df[col]) or 'date' in col.lower():
                col_profile['stats'] = DataProfiler.profile_date_column(df[col])
            else:
                col_profile['stats'] = DataProfiler.profile_categorical_column(df[col])
            
            profile['columns'][col] = col_profile
        
        return profile
    
    @staticmethod
    def profile_summary_to_dataframe(profile: Dict) -> pd.DataFrame:
        """Convert profile to summary DataFrame"""
        summary_data = []
        
        for col_name, col_data in profile['columns'].items():
            summary_data.append({
                'column': col_name,
                'dtype': col_data['dtype'],
                'null_count': col_data['null_count'],
                'null_percentage': f"{col_data['null_percentage']:.2f}%"
            })
        
        return pd.DataFrame(summary_data)
