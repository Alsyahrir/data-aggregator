"""Tests for solstice.detection module."""

import pytest
import pandas as pd
from solstice.detection import auto_detect_columns


class TestAutoDetectColumns:
    """Tests for keyword-based column detection."""

    def test_detects_inverter_columns(self):
        df = pd.DataFrame({
            "Date": ["2024-01-01"],
            "Energy_kWh": [100.0],
            "device_id": ["INV01"],
        })
        mapping, file_type = auto_detect_columns(df)
        assert file_type == "inverter"
        assert mapping.get("Date") == "timestamp"
        assert mapping.get("Energy_kWh") == "energy"

    def test_detects_environment_columns(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "ambient_temperature": [28.5],
            "humidity": [75.0],
        })
        mapping, file_type = auto_detect_columns(df)
        assert mapping.get("timestamp") == "timestamp"
        assert mapping.get("ambient_temperature") == "ambient_temp"
        assert mapping.get("humidity") == "humidity"

    def test_detects_irradiance_columns(self):
        df = pd.DataFrame({
            "datetime": ["2024-01-01"],
            "GHI": [850.0],
        })
        mapping, file_type = auto_detect_columns(df)
        assert mapping.get("datetime") == "timestamp"
        assert mapping.get("GHI") == "irradiance"
        assert file_type == "irradiance"

    def test_returns_unknown_for_ambiguous_columns(self):
        df = pd.DataFrame({
            "col_A": [1],
            "col_B": [2],
            "col_C": [3],
        })
        mapping, file_type = auto_detect_columns(df)
        assert file_type == "unknown"
        assert len(mapping) == 0  # no recognisable columns

    def test_ignores_voltage_for_energy(self):
        """Voltage columns should not be mapped to energy."""
        df = pd.DataFrame({
            "Date": ["2024-01-01"],
            "dc_voltage": [400.0],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("dc_voltage") != "energy"

    def test_ignores_dni_for_irradiance(self):
        """DNI should not be mapped to GHI irradiance."""
        df = pd.DataFrame({
            "Date": ["2024-01-01"],
            "DNI": [600.0],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("DNI") != "irradiance"

    def test_detects_wind_speed(self):
        df = pd.DataFrame({
            "time": ["2024-01-01"],
            "wind_speed": [5.2],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("wind_speed") == "wind_speed"

    def test_detects_module_temp(self):
        df = pd.DataFrame({
            "time": ["2024-01-01"],
            "panel_temp": [45.0],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("panel_temp") == "module_temp"
