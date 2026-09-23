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


class TestScoringBeatsColumnOrder:
    """Detection used to be first-match-wins, so a weak match could claim a
    field and leave the real column unmapped depending on column order."""

    def test_weak_match_does_not_steal_timestamp(self):
        # "downtime_minutes" contains "time" but is not a timestamp.
        df = pd.DataFrame({
            "downtime_minutes": [5, 6],
            "Measured_on": ["2024-01-01", "2024-01-02"],
            "energy_kwh": [2.0, 3.0],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("Measured_on") == "timestamp"
        assert "downtime_minutes" not in mapping

    def test_result_is_independent_of_column_order(self):
        cols = {
            "Measured_on": ["2024-01-01", "2024-01-02"],
            "downtime_minutes": [5, 6],
            "energy_kwh": [2.0, 3.0],
        }
        forward = auto_detect_columns(pd.DataFrame(cols))[0]
        reversed_ = auto_detect_columns(pd.DataFrame({k: cols[k] for k in reversed(list(cols))}))[0]
        assert forward == reversed_

    def test_prefers_column_that_holds_real_dates(self):
        """A 'Timestamp' column holding only a constant time-of-day should
        lose to a 'Date' column that actually spans dates."""
        df = pd.DataFrame({
            "Date": ["01-05-2025", "02-05-2025"],
            "Timestamp": ["12:00:00 am", "12:00:00 am"],
            "energy": [1.0, 2.0],
        })
        mapping, _ = auto_detect_columns(df)
        assert mapping.get("Date") == "timestamp"
