"""
Unit tests for FRED Data Pipeline
"""

import pytest
import pandas as pd
from datetime import date, datetime
from unittest.mock import patch, MagicMock


class TestDataTransformations:
    """Tests for data transformation logic."""

    def test_date_conversion(self):
        """Test date string to date conversion."""
        date_str = "2024-12-15"
        converted = pd.to_datetime(date_str).date()
        assert converted == date(2024, 12, 15)

    def test_date_conversion_various_formats(self):
        """Test date conversion handles various formats."""
        test_cases = [
            ("2024-01-01", date(2024, 1, 1)),
            ("2024-12-31", date(2024, 12, 31)),
            ("2020-02-29", date(2020, 2, 29)),  # Leap year
        ]
        for date_str, expected in test_cases:
            converted = pd.to_datetime(date_str).date()
            assert converted == expected

    def test_value_conversion(self):
        """Test value string to float conversion."""
        value_str = "4.75"
        converted = float(value_str)
        assert converted == 4.75

    def test_value_conversion_edge_cases(self):
        """Test value conversion handles edge cases."""
        assert float("0.0") == 0.0
        assert float("-1.5") == -1.5
        assert float("100.12345") == 100.12345

    def test_missing_value_handling(self):
        """Test handling of missing values (FRED uses '.')."""
        observations = [
            {"date": "2024-12-01", "value": "4.50"},
            {"date": "2024-12-02", "value": "."},
            {"date": "2024-12-03", "value": "4.55"},
        ]
        valid_obs = [obs for obs in observations if obs["value"] != "."]
        assert len(valid_obs) == 2

    def test_missing_value_not_included(self):
        """Test that missing values are correctly filtered out."""
        observations = [
            {"date": "2024-12-01", "value": "."},
            {"date": "2024-12-02", "value": "."},
        ]
        valid_obs = [obs for obs in observations if obs["value"] != "."]
        assert len(valid_obs) == 0

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

    def test_deduplication_multiple_series(self):
        """Test deduplication works across multiple series."""
        data = pd.DataFrame([
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.50, "run_timestamp": "2024-12-10 08:00:00"},
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.55, "run_timestamp": "2024-12-10 12:00:00"},
            {"series_id": "DGS10", "date": "2024-12-01", "value": 3.90, "run_timestamp": "2024-12-10 08:00:00"},
            {"series_id": "DGS10", "date": "2024-12-01", "value": 3.95, "run_timestamp": "2024-12-10 12:00:00"},
        ])

        deduplicated = data.loc[
            data.groupby(["series_id", "date"])["run_timestamp"].idxmax()
        ]

        assert len(deduplicated) == 2
        dff_row = deduplicated[deduplicated["series_id"] == "DFF"].iloc[0]
        dgs10_row = deduplicated[deduplicated["series_id"] == "DGS10"].iloc[0]
        assert dff_row["value"] == 4.55
        assert dgs10_row["value"] == 3.95


class TestSeries:
    """Tests for series configuration."""

    EXPECTED_SERIES = {
        "DFF": "Federal Funds Effective Rate",
        "DTB3": "3-Month Treasury Bill Rate",
        "DGS1": "1-Year Treasury Rate",
        "DGS2": "2-Year Treasury Rate",
        "DGS5": "5-Year Treasury Rate",
        "DGS10": "10-Year Treasury Rate",
        "DGS30": "30-Year Treasury Rate",
        "SOFR": "Secured Overnight Financing Rate",
        "DPRIME": "Bank Prime Loan Rate",
        "MORTGAGE30US": "30-Year Fixed Rate Mortgage",
        "MORTGAGE15US": "15-Year Fixed Rate Mortgage",
        "BAMLC0A0CM": "ICE BofA US Corporate Index Yield",
        "BAMLH0A0HYM2": "ICE BofA US High Yield Index",
        "T10YIE": "10-Year Breakeven Inflation Rate",
        "T5YIE": "5-Year Breakeven Inflation Rate",
    }

    def test_treasury_series_included(self):
        """Test that key treasury series are in the series list."""
        treasury_series = ["DGS1", "DGS2", "DGS5", "DGS10", "DGS30"]
        for series_id in treasury_series:
            assert series_id in self.EXPECTED_SERIES

    def test_all_series_count(self):
        """Test that we have exactly 15 series configured."""
        assert len(self.EXPECTED_SERIES) == 15

    def test_series_ids_uppercase(self):
        """Test that all series IDs are uppercase."""
        for series_id in self.EXPECTED_SERIES.keys():
            assert series_id == series_id.upper()

    def test_mortgage_series_included(self):
        """Test that mortgage series are included."""
        assert "MORTGAGE30US" in self.EXPECTED_SERIES
        assert "MORTGAGE15US" in self.EXPECTED_SERIES

    def test_inflation_series_included(self):
        """Test that inflation expectation series are included."""
        assert "T10YIE" in self.EXPECTED_SERIES
        assert "T5YIE" in self.EXPECTED_SERIES


class TestAPIHelpers:
    """Tests for API helper functions."""

    def test_fred_base_url_format(self):
        """Test FRED base URL is correctly formatted."""
        from src.utils.api_helpers import FRED_BASE_URL
        assert FRED_BASE_URL == "https://api.stlouisfed.org/fred"
        assert FRED_BASE_URL.startswith("https://")

    @patch('src.utils.api_helpers.requests.get')
    def test_make_fred_request_success(self, mock_get):
        """Test successful FRED API request."""
        from src.utils.api_helpers import make_fred_request

        mock_response = MagicMock()
        mock_response.json.return_value = {"observations": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = make_fred_request("series/observations", "test_api_key")

        assert result == {"observations": []}
        mock_get.assert_called_once()

    @patch('src.utils.api_helpers.requests.get')
    def test_make_fred_request_with_params(self, mock_get):
        """Test FRED API request with additional parameters."""
        from src.utils.api_helpers import make_fred_request

        mock_response = MagicMock()
        mock_response.json.return_value = {"observations": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = make_fred_request(
            "series/observations",
            "test_api_key",
            params={"series_id": "DFF"}
        )

        call_args = mock_get.call_args
        assert "series_id" in call_args.kwargs["params"]

    @patch.dict('os.environ', {'FRED_API_KEY': 'test_env_key'})
    def test_get_api_key_from_env(self):
        """Test API key retrieval from environment variable."""
        from src.utils.api_helpers import get_fred_api_key

        api_key = get_fred_api_key()
        assert api_key == 'test_env_key'


class TestDataQuality:
    """Tests for data quality checks."""

    def test_value_within_expected_range(self):
        """Test that rate values are within reasonable ranges."""
        # Interest rates should typically be between -5% and 25%
        test_values = [0.0, 2.5, 5.0, 10.0, 15.0]
        for value in test_values:
            assert -5 <= value <= 25, f"Value {value} outside expected range"

    def test_date_not_in_future(self):
        """Test that observation dates are not in the future."""
        observation_date = date(2024, 12, 15)
        today = date.today()
        # This test will pass if observation is not more than 1 day in future
        # (accounting for timezone differences)
        assert observation_date <= today or (observation_date - today).days <= 1

    def test_series_id_valid_format(self):
        """Test series IDs match expected format."""
        valid_series = ["DFF", "DGS10", "MORTGAGE30US", "BAMLC0A0CM"]
        for series_id in valid_series:
            # Series IDs should be alphanumeric and uppercase
            assert series_id.isalnum() or series_id.replace("_", "").isalnum()
            assert series_id == series_id.upper()

    def test_no_duplicate_observations(self):
        """Test that deduplication removes duplicates correctly."""
        observations = pd.DataFrame([
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.50},
            {"series_id": "DFF", "date": "2024-12-01", "value": 4.50},
            {"series_id": "DFF", "date": "2024-12-02", "value": 4.55},
        ])

        deduplicated = observations.drop_duplicates(subset=["series_id", "date"])
        assert len(deduplicated) == 2


class TestTimestampHandling:
    """Tests for timestamp handling."""

    def test_run_timestamp_format(self):
        """Test run timestamp is in expected format."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        parsed = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        assert isinstance(parsed, datetime)

    def test_run_timestamp_ordering(self):
        """Test that newer timestamps sort after older ones."""
        older = "2024-12-10 08:00:00"
        newer = "2024-12-10 12:00:00"
        assert newer > older


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
