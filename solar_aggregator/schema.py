from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AggregationMethod(Enum):
    SUM = "sum"
    MEAN = "mean"
    COUNT = "count"


@dataclass
class SchemaField:
    name: str
    required: bool
    aggregation: AggregationMethod
    keywords: List[str]
    description: str
    unit: str = ""


SCHEMA: Dict[str, SchemaField] = {
    "timestamp": SchemaField(
        name="timestamp",
        required=True,
        aggregation=AggregationMethod.MEAN,
        keywords=["time", "date", "datetime", "timestamp", "measured", "measured_on"],
        description="Date and time of measurement",
        unit="datetime"
    ),
    "source_id": SchemaField(
        name="source_id",
        required=True,
        aggregation=AggregationMethod.MEAN,
        keywords=["id", "source", "plant", "site", "inverter", "device"],
        description="Unique identifier for the data source",
        unit=""
    ),
    "energy": SchemaField(
        name="energy",
        required=True,
        aggregation=AggregationMethod.SUM,
        keywords=["energy", "kwh", "kw_h", "ac_output", "output_kwh", "generation", "value"],
        description="Energy generated in the time period",
        unit="kWh"
    ),
    "ambient_temp": SchemaField(
        name="ambient_temp",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["ambient", "ambient_temp", "ambient_temperature", "air_temp"],
        description="Ambient air temperature",
        unit="C"
    ),
    "irradiance": SchemaField(
        name="irradiance",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["irradiance", "ghi", "global_horizontal", "solar_radiation"],
        description="Solar irradiance (GHI)",
        unit="W/m2"
    ),
    "wind_speed": SchemaField(
        name="wind_speed",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["wind", "wind_speed", "windspeed"],
        description="Wind speed",
        unit="m/s"
    ),
    "module_temp": SchemaField(
        name="module_temp",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["module_temp", "panel_temp", "cell_temp", "pv_temp"],
        description="PV module temperature",
        unit="C"
    ),
    "humidity": SchemaField(
        name="humidity",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["humidity", "relative_humidity", "rh"],
        description="Relative humidity",
        unit="%"
    ),
    "power": SchemaField(
        name="power",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["power", "ac_power", "dc_power", "active_power", "watt"],
        description="Power output",
        unit="W"
    ),
    "voltage": SchemaField(
        name="voltage",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["voltage", "dc_voltage", "ac_voltage", "vdc", "vac"],
        description="Voltage",
        unit="V"
    ),
    "current": SchemaField(
        name="current",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["current", "dc_current", "ac_current", "ampere"],
        description="Current",
        unit="A"
    ),
}


# ── Custom Exceptions ────────────────────────────────────────────────────────

class SolarAggregatorError(Exception):
    """Base exception for the solar aggregator library."""

class SchemaValidationError(SolarAggregatorError):
    """Raised when data fails schema validation."""

class DetectionError(SolarAggregatorError):
    """Raised when column detection fails."""

class AggregationError(SolarAggregatorError):
    """Raised when aggregation fails."""

class WeatherEnrichmentError(SolarAggregatorError):
    """Raised when weather data enrichment fails."""


# ── Schema Extension API ─────────────────────────────────────────────────────

def register_field(
    name: str,
    required: bool = False,
    aggregation: AggregationMethod = AggregationMethod.MEAN,
    keywords: Optional[List[str]] = None,
    description: str = "",
    unit: str = "",
) -> SchemaField:
    """Register a custom field in the schema.

    Example:
        register_field("efficiency", keywords=["eff", "eta"], unit="%")
    """
    field = SchemaField(
        name=name,
        required=required,
        aggregation=aggregation,
        keywords=keywords or [name],
        description=description or f"Custom field: {name}",
        unit=unit,
    )
    SCHEMA[name] = field
    logger.info("Registered custom schema field: %s", name)
    return field


def get_aggregation_rules() -> Dict[str, str]:
    return {
        name: field.aggregation.value
        for name, field in SCHEMA.items()
        if name not in ["timestamp", "source_id"]
    }


def get_required_fields() -> List[str]:
    return [name for name, field in SCHEMA.items() if field.required]


def get_optional_fields() -> List[str]:
    return [name for name, field in SCHEMA.items() if not field.required]


def print_schema():
    print("=" * 70)
    print("SOLAR DATA AGGREGATOR - STANDARD SCHEMA")
    print("=" * 70)
    print("\nREQUIRED FIELDS:")
    print("-" * 40)
    for name, field in SCHEMA.items():
        if field.required:
            unit_str = f" ({field.unit})" if field.unit else ""
            print(f"  {name}{unit_str}")
            print(f"    Aggregation: {field.aggregation.value.upper()}")
            print(f"    Description: {field.description}\n")
    print("OPTIONAL FIELDS:")
    print("-" * 40)
    for name, field in SCHEMA.items():
        if not field.required:
            unit_str = f" ({field.unit})" if field.unit else ""
            print(f"  {name}{unit_str}")
            print(f"    Aggregation: {field.aggregation.value.upper()}")
            print(f"    Description: {field.description}\n")
    print("=" * 70)
    print("KEY: Energy = SUM, Temperature/Irradiance = MEAN")
    print("=" * 70)
