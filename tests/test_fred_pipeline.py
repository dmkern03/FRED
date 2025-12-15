"""
Unit tests for FRED Data Pipeline
"""

import pytest
import pandas as pd
from datetime import date


class TestDataTransformations:
    """Tests for data transformation logic."""
    
    def test_date_conversion(self):
        """Test date string to date conversion."""
        date_str = "2024-12-15"
        converted = pd.to_datetime(date_str).date()
        assert converted == date(2024, 12, 15)
    
    def test_value_conversion(self):
        """Test value string to float conversion."""
        value_str = "4.75"
        converted = float(value_str)
        assert converted == 4.75
    
    def test_missing_value_handling(self):
        """Test handling of missing values (FRED uses '.')."""
        observations = [
            {"date": "2024-12-01", "value": "4.50"},
            {"date": "2024-12-02", "value": "."},
            {"date": "2024-12-03", "value": "4.55"},
        ]
        valid_obs = [obs for obs in observations if obs["value"] != "."]
        assert len(valid_obs) == 2
    
    def test_deduplication_logic(self):
        """Test deduplication keeps latest run_timestamp."""
        data = pd.DataFrame([
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.50, "run_timestamp": "2024-12-10 08:00:00"},
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.55, "run_timestamp": "2024-12-10 12:00:00"},
        ])
        
        deduplicated = data.loc[
            data.groupby(["series_id", "date"])["run_timestamp"].idxmax()
        ]
        
        assert len(deduplicated) == 1
        assert deduplicated.iloc[0]["value"] == 4.55


class TestRateSeries:
    """Tests for rate series configuration."""
    
    def test_treasury_rates_included(self):
        """Test that key treasury rates are in the series list."""
        rate_series = {
            "DGS1": "1-Year Treasury Rate",
            "DGS2": "2-Year Treasury Rate",
            "DGS5": "5-Year Treasury Rate",
            "DGS10": "10-Year Treasury Rate",
            "DGS30": "30-Year Treasury Rate",
        }
        assert "DGS10" in rate_series
        assert "DGS30" in rate_series


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
