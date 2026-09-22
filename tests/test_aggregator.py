"""End-to-end tests for the Solstice aggregation pipeline."""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os

from solstice import Solstice


@pytest.fixture
def inverter_csv(tmp_path):
    """Create a sample inverter CSV."""
    csv_path = tmp_path / "inverter_data.csv"
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    df = pd.DataFrame({
        "Date": dates,
        "Energy_kWh": np.random.uniform(80, 200, 60),
    })
    df.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def environment_csv(tmp_path):
    """Create a sample environment CSV."""
    csv_path = tmp_path / "environment_data.csv"
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    df = pd.DataFrame({
        "timestamp": dates,
        "ambient_temperature": np.random.uniform(25, 35, 60),
        "humidity": np.random.uniform(60, 90, 60),
        "wind_speed": np.random.uniform(1, 10, 60),
    })
    df.to_csv(csv_path, index=False)
    return str(csv_path)


class TestSolstice:
    """End-to-end aggregation tests."""

    def test_single_file_aggregation(self, inverter_csv):
        agg = Solstice()
        agg.add_file(inverter_csv)
        result = agg.aggregate(freq="1D")

        assert isinstance(result, pd.DataFrame)
        assert "timestamp" in result.columns
        assert "energy" in result.columns
        assert "source_id" in result.columns
        assert len(result) > 0

    def test_multiple_files(self, inverter_csv, environment_csv):
        agg = Solstice()
        agg.add_file(inverter_csv)
        agg.add_file(environment_csv)
        result = agg.aggregate(freq="1D")
        
        assert len(result) > 0
        assert "energy" in result.columns

    def test_custom_mapping(self, inverter_csv):
        agg = Solstice()
        mapping = {"Date": "timestamp", "Energy_kWh": "energy"}
        agg.add_file(inverter_csv, source_id="MY_PANEL", mapping=mapping)
        result = agg.aggregate(freq="1D")
        
        assert "MY_PANEL" in result["source_id"].values

    def test_weekly_aggregation(self, inverter_csv):
        agg = Solstice()
        agg.add_file(inverter_csv)
        result = agg.aggregate(freq="1W")
        
        # Should have fewer rows than daily
        daily = agg.aggregate(freq="1D")
        assert len(result) <= len(daily)

    def test_save_csv(self, inverter_csv, tmp_path):
        agg = Solstice()
        agg.add_file(inverter_csv)
        agg.aggregate(freq="1D")
        
        output_path = str(tmp_path / "output.csv")
        agg.save(output_path)
        assert os.path.exists(output_path)
        
        loaded = pd.read_csv(output_path)
        assert len(loaded) > 0

    def test_get_summary(self, inverter_csv):
        agg = Solstice()
        agg.add_file(inverter_csv)
        agg.aggregate(freq="1D")
        
        summary = agg.get_summary()
        assert "SUMMARY" in summary
        assert "kWh" in summary

    def test_get_dataframe_types(self, inverter_csv):
        agg = Solstice()
        agg.add_file(inverter_csv)
        agg.aggregate(freq="1D")
        
        agg_df = agg.get_dataframe("aggregated")
        assert agg_df is not None
        
        merged_df = agg.get_dataframe("merged")
        assert merged_df is not None

    def test_no_data_raises(self):
        agg = Solstice()
        with pytest.raises(ValueError, match="No inverter data"):
            agg.aggregate()

    def test_chaining(self, inverter_csv):
        """Test fluent API chaining."""
        agg = Solstice()
        result = agg.add_file(inverter_csv)
        assert result is agg  # add_file returns self
