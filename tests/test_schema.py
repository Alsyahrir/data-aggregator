"""Tests for solstice.schema module."""

import pytest
from solstice.schema import (
    SCHEMA, SchemaField, AggregationMethod,
    get_aggregation_rules, get_required_fields, get_optional_fields,
    register_field,
)


class TestSchema:
    """Tests for schema definitions."""

    def test_schema_has_required_fields(self):
        required = get_required_fields()
        assert "timestamp" in required
        assert "source_id" in required
        assert "energy" in required

    def test_schema_has_optional_fields(self):
        optional = get_optional_fields()
        assert "ambient_temp" in optional
        assert "irradiance" in optional
        assert "wind_speed" in optional
        assert "humidity" in optional
        assert "module_temp" in optional

    def test_energy_uses_sum_aggregation(self):
        assert SCHEMA["energy"].aggregation == AggregationMethod.SUM

    def test_temperature_uses_mean_aggregation(self):
        assert SCHEMA["ambient_temp"].aggregation == AggregationMethod.MEAN
        assert SCHEMA["module_temp"].aggregation == AggregationMethod.MEAN

    def test_all_fields_have_keywords(self):
        for name, field in SCHEMA.items():
            assert len(field.keywords) > 0, f"Field '{name}' has no keywords"

    def test_aggregation_rules_exclude_timestamp_and_source(self):
        rules = get_aggregation_rules()
        assert "timestamp" not in rules
        assert "source_id" not in rules
        assert "energy" in rules
        assert rules["energy"] == "sum"

    def test_new_fields_power_voltage_current(self):
        """Verify extended schema fields exist."""
        assert "power" in SCHEMA
        assert "voltage" in SCHEMA
        assert "current" in SCHEMA
        assert SCHEMA["power"].unit == "W"
        assert SCHEMA["voltage"].unit == "V"
        assert SCHEMA["current"].unit == "A"


class TestRegisterField:
    """Tests for the register_field API."""

    def test_register_custom_field(self):
        field = register_field(
            "efficiency",
            keywords=["eff", "eta"],
            unit="%",
            description="Solar panel efficiency",
        )
        assert "efficiency" in SCHEMA
        assert SCHEMA["efficiency"].unit == "%"
        assert "eff" in SCHEMA["efficiency"].keywords
        # Clean up
        del SCHEMA["efficiency"]

    def test_register_field_defaults(self):
        field = register_field("test_field")
        assert field.required is False
        assert field.aggregation == AggregationMethod.MEAN
        assert "test_field" in field.keywords
        # Clean up
        del SCHEMA["test_field"]
