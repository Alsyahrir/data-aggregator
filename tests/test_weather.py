"""Tests for solstice.weather module.

Network calls are mocked at the urlopen level — no real requests to
Open-Meteo happen here, keeping these tests fast and CI-safe.
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from solstice.schema import WeatherEnrichmentError
from solstice.weather import (
    compute_clearness_index,
    enrich_with_weather,
    fetch_weather,
    get_weather_columns,
)


def _mock_response(payload: dict) -> MagicMock:
    """Build a mock compatible with `with urlopen(...) as resp: resp.read()`."""
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode()
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


class TestFetchWeather:
    def test_parses_daily_data_and_derives_features(self):
        payload = {
            "daily": {
                "time": ["2024-01-01", "2024-01-02"],
                "temperature_2m_max": [30.0, 31.0],
                "temperature_2m_min": [24.0, 25.0],
                "sunshine_duration": [36000, 18000],  # seconds
            }
        }
        with patch("solstice.weather.urlopen", return_value=_mock_response(payload)):
            df = fetch_weather(1.35, 103.8, "2024-01-01", "2024-01-02")

        assert len(df) == 2
        assert "sunshine_duration" not in df.columns
        assert df["sunshine_hours"].iloc[0] == pytest.approx(10.0)  # 36000s -> 10h
        assert df["temp_range"].iloc[0] == pytest.approx(6.0)       # 30 - 24

    def test_raises_on_api_error(self):
        payload = {"error": True, "reason": "Invalid coordinates"}
        with patch("solstice.weather.urlopen", return_value=_mock_response(payload)):
            with pytest.raises(WeatherEnrichmentError, match="Invalid coordinates"):
                fetch_weather(999, 999, "2024-01-01", "2024-01-02")

    def test_raises_on_missing_daily_key(self):
        with patch("solstice.weather.urlopen", return_value=_mock_response({})):
            with pytest.raises(WeatherEnrichmentError, match="No daily data"):
                fetch_weather(1.35, 103.8, "2024-01-01", "2024-01-02")

    def test_raises_on_network_failure(self):
        with patch("solstice.weather.urlopen", side_effect=OSError("connection refused")):
            with pytest.raises(WeatherEnrichmentError, match="Failed to fetch"):
                fetch_weather(1.35, 103.8, "2024-01-01", "2024-01-02")


class TestComputeClearnessIndex:
    def test_index_stays_within_expected_bounds(self):
        df = pd.DataFrame({
            "date": pd.date_range("2024-06-01", periods=5),
            "shortwave_radiation_sum": [20.0, 15.0, 5.0, 25.0, 0.0],
        })
        result = compute_clearness_index(df, latitude=1.35)

        assert "clearness_index" in result.columns
        valid = result["clearness_index"].dropna()
        assert (valid >= 0).all()
        assert (valid <= 1.2).all()

    def test_missing_radiation_column_is_a_no_op(self):
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3)})
        result = compute_clearness_index(df, radiation_col="does_not_exist")
        assert "clearness_index" not in result.columns

    def test_missing_date_column_is_a_no_op(self):
        df = pd.DataFrame({"shortwave_radiation_sum": [10.0, 20.0]})
        result = compute_clearness_index(df)
        assert "clearness_index" not in result.columns

    def test_uses_timestamp_column_when_date_absent(self):
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-06-01", periods=3),
            "shortwave_radiation_sum": [20.0, 15.0, 10.0],
        })
        result = compute_clearness_index(df, latitude=1.35)
        assert "clearness_index" in result.columns


class TestEnrichWithWeather:
    def test_raises_without_timestamp_column(self):
        df = pd.DataFrame({"energy": [1, 2, 3]})
        with pytest.raises(WeatherEnrichmentError, match="timestamp"):
            enrich_with_weather(df, latitude=1.35, longitude=103.8)

    def test_merges_weather_columns_by_date(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "energy": [100.0, 110.0],
        })
        payload = {
            "daily": {
                "time": ["2024-01-01", "2024-01-02"],
                "temperature_2m_mean": [27.0, 28.0],
                "shortwave_radiation_sum": [20.0, 18.0],
            }
        }
        with patch("solstice.weather.urlopen", return_value=_mock_response(payload)):
            result = enrich_with_weather(df, latitude=1.35, longitude=103.8)

        assert result["temperature_2m_mean"].iloc[0] == pytest.approx(27.0)
        assert "energy" in result.columns  # original columns preserved
        assert "_merge_date" not in result.columns  # internal merge key dropped
        assert "date" not in result.columns  # weather's own date col dropped

    def test_does_not_overwrite_existing_columns(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01"]),
            "temperature_2m_mean": [999.0],  # already present in caller's df
        })
        payload = {"daily": {"time": ["2024-01-01"], "temperature_2m_mean": [27.0]}}
        with patch("solstice.weather.urlopen", return_value=_mock_response(payload)):
            result = enrich_with_weather(df, latitude=1.35, longitude=103.8)

        assert result["temperature_2m_mean"].iloc[0] == 999.0

    def test_unmatched_dates_are_null_not_dropped(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01", "2024-03-01"]),
            "energy": [100.0, 200.0],
        })
        payload = {"daily": {"time": ["2024-01-01"], "temperature_2m_mean": [27.0]}}
        with patch("solstice.weather.urlopen", return_value=_mock_response(payload)):
            result = enrich_with_weather(df, latitude=1.35, longitude=103.8)

        assert len(result) == 2  # left join keeps both rows
        assert pd.isna(result["temperature_2m_mean"].iloc[1])


class TestGetWeatherColumns:
    def test_returns_expected_columns(self):
        cols = get_weather_columns()
        assert isinstance(cols, list)
        assert "clearness_index" in cols
        assert "sunshine_hours" in cols
        assert "sunshine_duration" not in cols  # renamed, not kept
