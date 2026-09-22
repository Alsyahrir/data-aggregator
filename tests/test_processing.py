"""Tests for solstice.processing module."""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os

from solstice.processing import (
    load_file,
    standardise_dataframe,
    validate_dataframe,
    detect_anomalies,
    auto_clean,
)


@pytest.fixture
def sample_csv(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_path = tmp_path / "test_data.csv"
    df = pd.DataFrame({
        "Date": pd.date_range("2024-01-01", periods=30, freq="D"),
        "Energy_kWh": np.random.uniform(50, 200, 30),
        "Temp": np.random.uniform(25, 35, 30),
    })
    df.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=100, freq="D")
    return pd.DataFrame({
        "timestamp": dates,
        "source_id": ["PANEL01"] * 100,
        "energy": np.random.uniform(50, 200, 100),
        "ambient_temp": np.random.uniform(25, 35, 100),
        "irradiance": np.random.uniform(200, 900, 100),
    })


class TestLoadFile:
    """Tests for load_file."""

    def test_load_csv(self, sample_csv):
        df = load_file(sample_csv)
        assert len(df) == 30
        assert "Date" in df.columns

    def test_load_nonexistent_raises(self):
        with pytest.raises(FileNotFoundError):
            load_file("/nonexistent/file.csv")

    def test_load_unsupported_raises(self, tmp_path):
        bad_file = tmp_path / "data.txt"
        bad_file.write_text("hello")
        with pytest.raises(ValueError, match="Unsupported file type"):
            load_file(str(bad_file))


class TestStandardiseDataframe:
    """Tests for standardise_dataframe."""

    def test_renames_columns(self):
        df = pd.DataFrame({
            "Date": ["2024-01-01", "2024-01-02"],
            "Energy_kWh": [100.0, 150.0],
        })
        mapping = {"Date": "timestamp", "Energy_kWh": "energy"}
        result = standardise_dataframe(df, mapping, "TEST01")
        assert "timestamp" in result.columns
        assert "energy" in result.columns
        assert "source_id" in result.columns
        assert result["source_id"].iloc[0] == "TEST01"

    def test_converts_timestamp(self):
        df = pd.DataFrame({"Date": ["01/01/2024", "02/01/2024"]})
        mapping = {"Date": "timestamp"}
        result = standardise_dataframe(df, mapping, "TEST")
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_converts_numeric_fields(self):
        df = pd.DataFrame({
            "Date": ["2024-01-01"],
            "val": ["123.45"],
        })
        mapping = {"Date": "timestamp", "val": "energy"}
        result = standardise_dataframe(df, mapping, "TEST")
        assert result["energy"].dtype in [np.float64, np.float32]


class TestValidateDataframe:
    """Tests for validate_dataframe."""

    def test_valid_dataframe_passes(self, sample_df):
        is_valid, issues = validate_dataframe(sample_df)
        errors = [i for i in issues if i['severity'] == 'error']
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_required_column(self):
        df = pd.DataFrame({"foo": [1, 2, 3]})
        is_valid, issues = validate_dataframe(df)
        assert is_valid is False
        error_msgs = [i['message'] for i in issues if i['severity'] == 'error']
        assert any("timestamp" in msg for msg in error_msgs)

    def test_negative_energy_flagged(self, sample_df):
        sample_df.loc[0, "energy"] = -50
        is_valid, issues = validate_dataframe(sample_df)
        error_msgs = [i['message'] for i in issues if i['severity'] == 'error']
        assert any("negative" in msg for msg in error_msgs)

    def test_out_of_range_warns(self, sample_df):
        sample_df.loc[0, "irradiance"] = 9999  # Way above physical limit
        _, issues = validate_dataframe(sample_df)
        warnings = [i['message'] for i in issues if i['severity'] == 'warning']
        assert any("irradiance" in msg for msg in warnings)

    def test_duplicate_timestamps_warned(self, sample_df):
        # Create duplicate timestamp
        sample_df.loc[1, "timestamp"] = sample_df.loc[0, "timestamp"]
        _, issues = validate_dataframe(sample_df)
        warnings = [i['message'] for i in issues if i['severity'] == 'warning']
        assert any("duplicate" in msg for msg in warnings)


class TestAnomalyDetection:
    """Tests for detect_anomalies and auto_clean."""

    def test_detect_anomalies_iqr(self, sample_df):
        # Insert an obvious outlier
        sample_df.loc[0, "energy"] = 99999
        result = detect_anomalies(sample_df, columns=["energy"], method="iqr")
        assert "is_anomaly" in result.columns
        assert result.loc[0, "is_anomaly"] is True or result.loc[0, "is_anomaly"] == True

    def test_detect_anomalies_zscore(self, sample_df):
        sample_df.loc[0, "energy"] = 99999
        result = detect_anomalies(sample_df, columns=["energy"], method="zscore")
        assert result.loc[0, "is_anomaly"] is True or result.loc[0, "is_anomaly"] == True

    def test_auto_clean_flag(self, sample_df):
        sample_df.loc[0, "energy"] = 99999
        result = auto_clean(sample_df, strategy="flag")
        assert "is_anomaly" in result.columns
        assert len(result) == len(sample_df)  # No rows removed

    def test_auto_clean_drop(self, sample_df):
        sample_df.loc[0, "energy"] = 99999
        result = auto_clean(sample_df, strategy="drop")
        assert len(result) < len(sample_df)
        assert "is_anomaly" not in result.columns  # Cleaned up

    def test_auto_clean_clip(self, sample_df):
        original_max = sample_df["energy"].max()
        sample_df.loc[0, "energy"] = 99999
        result = auto_clean(sample_df, strategy="clip")
        assert result["energy"].max() < 99999

    def test_no_anomalies_in_clean_data(self, sample_df):
        result = detect_anomalies(sample_df, columns=["energy"])
        assert result["is_anomaly"].sum() == 0 or result["is_anomaly"].sum() < len(sample_df) * 0.1
