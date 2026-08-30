"""Static analytics for the public dashboard."""

from .kpis import build_dashboard_payload, calculate_filtered_kpis, transform_dataset

__all__ = ["build_dashboard_payload", "calculate_filtered_kpis", "transform_dataset"]
